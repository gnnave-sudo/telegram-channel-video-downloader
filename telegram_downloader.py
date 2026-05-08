#!/usr/bin/env python3
"""
Telegram Channel Video Downloader

An async Python script that downloads videos from a Telegram channel using Telethon.
Supports resumable downloads, concurrency control, disk monitoring, retry logic,
exponential backoff, integrity verification, duplicate skipping, and Telegram bot
notifications.

Usage:
    python telegram_downloader.py [OPTIONS]

    All options can also be set via environment variables in a .env file.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import shutil
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Set

import aiofiles
import requests
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import (
    ChannelInvalidError,
    ChannelNotFoundError,
    FloodWaitError,
    MessageIdInvalidError,
    FileReferenceExpiredError,
    TimedOutError,
)
from telethon.tl.types import InputMessagesFilterVideo
from tqdm import tqdm


# ──────────────────────────────────────────────────────────────────────────────
# Configuration Dataclass
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class Config:
    """Runtime configuration for the downloader."""
    api_id: int
    api_hash: str
    channel: str
    bot_token: Optional[str] = None
    chat_id: Optional[str] = None
    output_dir: str = "videos_output"
    min_size_mb: int = 100
    concurrent: int = 15
    retries: int = 5
    retry_delay: int = 5
    max_retry_delay: int = 40
    session_name: str = "anon"
    disk_threshold_gb: int = 10


# ──────────────────────────────────────────────────────────────────────────────
# Downloader Class
# ──────────────────────────────────────────────────────────────────────────────

class TelegramVideoDownloader:
    """
    Async Telegram channel video downloader with resumable downloads,
    concurrency control, retry logic, disk monitoring, and notifications.
    """

    def __init__(self, config: Config) -> None:
        self.config = config
        self.shutdown_event = asyncio.Event()
        self.client: Optional[TelegramClient] = None

        # Statistics counters
        self.stats = {
            "downloaded": 0,
            "failed": 0,
            "skipped": 0,
            "since_last_notify": 0,
        }

        # Tracking sets
        self.downloaded_ids: Set[int] = set()
        self.id_file_path: Path = Path(config.output_dir) / "downloaded_videos.txt"

        # Notification state
        self._notify_lock = asyncio.Lock()
        self._start_time: Optional[float] = None

        # Setup
        self._ensure_directories()
        self._setup_logging()
        self._load_downloaded_ids()

    # ── Directory & Logging Setup ────────────────────────────────────────────

    def _ensure_directories(self) -> None:
        """Create output and logs directories if they don't exist."""
        Path(self.config.output_dir).mkdir(parents=True, exist_ok=True)
        Path("logs").mkdir(parents=True, exist_ok=True)

    def _setup_logging(self) -> None:
        """Configure logging to both console and timestamped file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = Path("logs") / f"telegram_downloader_{timestamp}.log"

        self.logger = logging.getLogger("telegram_downloader")
        self.logger.setLevel(logging.DEBUG)
        self.logger.handlers.clear()

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # File handler
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        self.logger.info("Logging initialized → %s", log_file)

    # ── Downloaded ID Tracking ───────────────────────────────────────────────

    def _load_downloaded_ids(self) -> None:
        """Load already-downloaded message IDs from disk into memory."""
        if not self.id_file_path.exists():
            self.downloaded_ids = set()
            self.logger.info("No existing download log found; starting fresh.")
            return

        try:
            with open(self.id_file_path, "r", encoding="utf-8") as f:
                lines = [line.strip() for line in f if line.strip()]
            # Each line may be just an ID or "ID filename"
            self.downloaded_ids = {
                int(line.split()[0]) for line in lines if line.split()[0].isdigit()
            }
            self.logger.info(
                "Loaded %d previously downloaded video IDs.", len(self.downloaded_ids)
            )
        except Exception as exc:
            self.logger.error("Failed to load downloaded IDs: %s", exc)
            self.downloaded_ids = set()

    async def _save_downloaded_id(self, message_id: int) -> None:
        """Persist a newly-downloaded message ID to the tracking file."""
        try:
            async with aiofiles.open(self.id_file_path, "a", encoding="utf-8") as f:
                await f.write(f"{message_id}\n")
            self.downloaded_ids.add(message_id)
        except Exception as exc:
            self.logger.error("Failed to save downloaded ID %d: %s", message_id, exc)

    # ── Disk Space Guard ─────────────────────────────────────────────────────

    def _check_disk_space(self, required_bytes: int = 0) -> bool:
        """
        Check whether free disk space is above the configured threshold.

        Args:
            required_bytes: Additional bytes expected to be written.

        Returns:
            True if safe to proceed, False if below threshold.
        """
        try:
            usage = shutil.disk_usage(self.config.output_dir)
            free_gb = usage.free / (1024 ** 3)
            threshold_bytes = self.config.disk_threshold_gb * (1024 ** 3)

            if usage.free - required_bytes < threshold_bytes:
                self.logger.warning(
                    "Low disk space: %.2f GB free (threshold: %d GB). "
                    "Aborting new downloads.",
                    free_gb, self.config.disk_threshold_gb,
                )
                return False
            return True
        except Exception as exc:
            self.logger.error("Disk space check failed: %s", exc)
            return False

    # ── Telegram Bot Notifications ───────────────────────────────────────────

    def _send_notification(self, message: str) -> None:
        """
        Send a message via the Telegram Bot API using a synchronous POST.

        Args:
            message: The text to send. Truncated to 4096 chars if needed.
        """
        if not self.config.bot_token or not self.config.chat_id:
            return

        try:
            text = message[:4096]  # Telegram message limit
            url = (
                f"https://api.telegram.org/bot{self.config.bot_token}/sendMessage"
            )
            payload = {
                "chat_id": self.config.chat_id,
                "text": text,
                "parse_mode": "HTML",
            }
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code != 200:
                self.logger.warning(
                    "Notification failed (HTTP %d): %s", resp.status_code, resp.text
                )
        except Exception as exc:
            self.logger.warning("Failed to send notification: %s", exc)

    async def _maybe_send_batch_notification(self) -> None:
        """Send a progress update every 10 completed downloads."""
        async with self._notify_lock:
            self.stats["since_last_notify"] += 1
            if self.stats["since_last_notify"] >= 10:
                elapsed = time.time() - (self._start_time or time.time())
                msg = (
                    f"📊 <b>Download Progress</b>\n"
                    f"✅ Downloaded: {self.stats['downloaded']}\n"
                    f"❌ Failed: {self.stats['failed']}\n"
                    f"⏭ Skipped: {self.stats['skipped']}\n"
                    f"⏱ Elapsed: {self._fmt_duration(elapsed)}"
                )
                self._send_notification(msg)
                self.stats["since_last_notify"] = 0

    async def _send_completion_notification(self) -> None:
        """Send the final summary notification."""
        elapsed = time.time() - (self._start_time or time.time())
        msg = (
            f"🏁 <b>Download Session Complete</b>\n"
            f"✅ Downloaded: {self.stats['downloaded']}\n"
            f"❌ Failed: {self.stats['failed']}\n"
            f"⏭ Skipped: {self.stats['skipped']}\n"
            f"⏱ Total Time: {self._fmt_duration(elapsed)}"
        )
        self._send_notification(msg)

    async def _send_error_alert(self, message_id: int, error: str) -> None:
        """Send an alert for a failed download."""
        msg = (
            f"⚠️ <b>Download Error</b>\n"
            f"Message ID: <code>{message_id}</code>\n"
            f"Error: <pre>{error[:500]}</pre>"
        )
        self._send_notification(msg)

    async def _send_stop_notification(self) -> None:
        """Send a notification when the downloader is stopping."""
        msg = (
            f"🛑 <b>Downloader Stopped</b>\n"
            f"Completed so far: {self.stats['downloaded']}\n"
            f"Failed: {self.stats['failed']}\n"
            f"Skipped: {self.stats['skipped']}"
        )
        self._send_notification(msg)

    @staticmethod
    def _fmt_duration(seconds: float) -> str:
        """Format a duration in seconds as H:MM:SS."""
        m, s = divmod(int(seconds), 60)
        h, m = divmod(m, 60)
        return f"{h}:{m:02d}:{s:02d}"

    # ── Filename Builder ─────────────────────────────────────────────────────

    @staticmethod
    def _build_filename(message) -> str:
        """
        Build the output filename from message metadata.

        Format: {YYYY-MM-DD_HH-MM-SS}_{message_id}.mp4
        """
        date_str = message.date.strftime("%Y-%m-%d_%H-%M-%S")
        return f"{date_str}_{message.id}.mp4"

    # ── Message Refresh (File Reference) ─────────────────────────────────────

    async def _refresh_message(self, original_message) -> Optional:
        """
        Re-fetch a message to obtain a fresh file reference.

        Returns:
            The refreshed message object, or None on failure.
        """
        try:
            self.logger.debug(
                "Refreshing file reference for message %d", original_message.id
            )
            refreshed = await self.client.get_messages(
                self.config.channel, ids=original_message.id
            )
            return refreshed
        except Exception as exc:
            self.logger.error(
                "Failed to refresh message %d: %s", original_message.id, exc
            )
            return None

    # ── Core Download with Retry ─────────────────────────────────────────────

    async def _download_single_video(self, message, semaphore: asyncio.Semaphore) -> bool:
        """
        Download one video message with retry logic and integrity checks.

        Args:
            message: A Telethon message object containing a video.
            semaphore: Semaphore to limit concurrent downloads.

        Returns:
            True on success, False on failure.
        """
        async with semaphore:
            msg_id = message.id
            video = message.video

            if not video:
                self.logger.warning("Message %d has no video; skipping.", msg_id)
                return False

            expected_size = video.size or 0
            filename = self._build_filename(message)
            final_path = Path(self.config.output_dir) / filename
            temp_path = final_path.with_suffix(final_path.suffix + ".temp")

            # Skip if already downloaded
            if msg_id in self.downloaded_ids and final_path.exists():
                self.logger.info("Skipping message %d (already downloaded).", msg_id)
                self.stats["skipped"] += 1
                return True

            # Check minimum size filter
            if expected_size > 0 and expected_size < self.config.min_size_mb * (1024 ** 2):
                self.logger.info(
                    "Message %d: video size %.2f MB < min %d MB; skipping.",
                    msg_id,
                    expected_size / (1024 ** 2),
                    self.config.min_size_mb,
                )
                self.stats["skipped"] += 1
                return True

            # Disk space guard
            if not self._check_disk_space(required_bytes=expected_size):
                self.logger.error(
                    "Disk space check failed for message %d; aborting.", msg_id
                )
                return False

            # ── Retry loop ──────────────────────────────────────────────────────
            current_message = message
            delay = self.config.retry_delay

            for attempt in range(1, self.config.retries + 1):
                if self.shutdown_event.is_set():
                    self.logger.info(
                        "Shutdown requested; aborting download of message %d.", msg_id
                    )
                    return False

                try:
                    # Determine resume offset
                    offset = 0
                    if temp_path.exists():
                        offset = temp_path.stat().st_size
                        self.logger.info(
                            "Resuming message %d from byte %d (attempt %d/%d)",
                            msg_id, offset, attempt, self.config.retries,
                        )
                    else:
                        self.logger.info(
                            "Downloading message %d (attempt %d/%d)",
                            msg_id, attempt, self.config.retries,
                        )

                    pbar = None
                    try:
                        # Progress bar
                        pbar = tqdm(
                            total=expected_size,
                            initial=offset,
                            desc=f"msg_{msg_id}",
                            unit="B",
                            unit_scale=True,
                            unit_divisor=1024,
                            leave=False,
                        )

                        def progress_callback(current: int, total: int) -> None:
                            pbar.update(current - pbar.n)

                        # Perform download
                        with open(temp_path, "ab") as temp_file:
                            await self.client.download_media(
                                current_message,
                                file=temp_file,
                                offset=offset,
                                progress_callback=progress_callback,
                            )
                    finally:
                        if pbar is not None:
                            pbar.close()

                    # ── Integrity verification ────────────────────────────────
                    downloaded_size = temp_path.stat().st_size
                    if expected_size > 0 and downloaded_size != expected_size:
                        self.logger.warning(
                            "Integrity check failed for message %d: "
                            "expected %d bytes, got %d bytes.",
                            msg_id, expected_size, downloaded_size,
                        )
                        raise RuntimeError(
                            f"Size mismatch: {downloaded_size} != {expected_size}"
                        )

                    # ── Finalize ──────────────────────────────────────────────
                    temp_path.rename(final_path)
                    self.logger.info(
                        "✓ Message %d downloaded → %s (%.2f MB)",
                        msg_id,
                        final_path.name,
                        downloaded_size / (1024 ** 2),
                    )

                    # Track success
                    await self._save_downloaded_id(msg_id)
                    self.stats["downloaded"] += 1
                    await self._maybe_send_batch_notification()
                    return True

                except FileReferenceExpiredError:
                    self.logger.warning(
                        "File reference expired for message %d; refreshing...", msg_id
                    )
                    refreshed = await self._refresh_message(current_message)
                    if refreshed and refreshed.video:
                        current_message = refreshed
                        self.logger.info(
                            "File reference refreshed for message %d; retrying.", msg_id
                        )
                    else:
                        self.logger.error(
                            "Could not refresh file reference for message %d.", msg_id
                        )

                except FloodWaitError as exc:
                    wait_sec = exc.seconds
                    self.logger.warning(
                        "FloodWaitError for message %d: must wait %d seconds.",
                        msg_id, wait_sec,
                    )
                    if attempt < self.config.retries:
                        sleep_time = min(wait_sec + delay, self.config.max_retry_delay)
                        self.logger.info("Retrying in %d seconds...", sleep_time)
                        await asyncio.sleep(sleep_time)
                        delay = min(delay * 2, self.config.max_retry_delay)

                except (TimedOutError, TimeoutError, asyncio.TimeoutError):
                    self.logger.warning(
                        "TimeoutError for message %d (attempt %d/%d).",
                        msg_id, attempt, self.config.retries,
                    )
                    if attempt < self.config.retries:
                        sleep_time = min(delay, self.config.max_retry_delay)
                        self.logger.info("Retrying in %d seconds...", sleep_time)
                        await asyncio.sleep(sleep_time)
                        delay = min(delay * 2, self.config.max_retry_delay)

                except RuntimeError as exc:
                    # Integrity check failure — allow retry with resume
                    self.logger.error(
                        "Integrity error for message %d (attempt %d/%d): %s",
                        msg_id, attempt, self.config.retries, exc,
                    )
                    if attempt < self.config.retries:
                        sleep_time = min(delay, self.config.max_retry_delay)
                        self.logger.info("Retrying in %d seconds...", sleep_time)
                        await asyncio.sleep(sleep_time)
                        delay = min(delay * 2, self.config.max_retry_delay)

                except Exception as exc:
                    self.logger.error(
                        "Download error for message %d (attempt %d/%d): %s",
                        msg_id, attempt, self.config.retries, exc,
                    )
                    if attempt < self.config.retries:
                        sleep_time = min(delay, self.config.max_retry_delay)
                        self.logger.info("Retrying in %d seconds...", sleep_time)
                        await asyncio.sleep(sleep_time)
                        delay = min(delay * 2, self.config.max_retry_delay)

            # All retries exhausted
            self.logger.error(
                "Failed to download message %d after %d attempts.",
                msg_id, self.config.retries,
            )
            self.stats["failed"] += 1
            await self._send_error_alert(msg_id, f"All retries exhausted")
            return False

    # ── Main Runner ──────────────────────────────────────────────────────────

    async def run(self) -> None:
        """
        Main entry point: connect to Telegram, iterate video messages,
        and download with concurrency control.
        """
        self._start_time = time.time()
        self.logger.info("=== Telegram Channel Video Downloader Starting ===")
        self.logger.info(
            "Channel: %s | Output: %s | Concurrent: %d | Retries: %d | "
            "Min Size: %d MB | Disk Threshold: %d GB",
            self.config.channel,
            self.config.output_dir,
            self.config.concurrent,
            self.config.retries,
            self.config.min_size_mb,
            self.config.disk_threshold_gb,
        )

        # Initialize Telegram client
        self.client = TelegramClient(
            self.config.session_name,
            self.config.api_id,
            self.config.api_hash,
        )

        try:
            await self.client.start()
            self.logger.info("Telegram client connected.")
        except Exception as exc:
            self.logger.error("Failed to connect Telegram client: %s", exc)
            self._send_notification(
                f"❌ <b>Fatal Error</b>\nFailed to connect Telegram client:\n<pre>{exc}</pre>"
            )
            return

        # Validate channel
        try:
            entity = await self.client.get_entity(self.config.channel)
            self.logger.info("Resolved channel: %s (ID: %s)", entity.title, entity.id)
        except (ChannelNotFoundError, ChannelInvalidError, ValueError) as exc:
            self.logger.error("Channel not found: %s", exc)
            self._send_notification(
                f"❌ <b>Fatal Error</b>\nChannel not found:\n<pre>{exc}</pre>"
            )
            await self.client.disconnect()
            return
        except Exception as exc:
            self.logger.error("Failed to resolve channel: %s", exc)
            self._send_notification(
                f"❌ <b>Fatal Error</b>\nFailed to resolve channel:\n<pre>{exc}</pre>"
            )
            await self.client.disconnect()
            return

        # Semaphore for concurrency control
        semaphore = asyncio.Semaphore(self.config.concurrent)
        active_tasks: Set[asyncio.Task] = set()

        self.logger.info("Scanning channel for video messages...")

        try:
            async for message in self.client.iter_messages(
                entity, filter=InputMessagesFilterVideo
            ):
                if self.shutdown_event.is_set():
                    self.logger.info(
                        "Shutdown signal received; finishing active downloads..."
                    )
                    break

                if not message.video:
                    continue

                # Launch download task with semaphore passed for concurrency control
                task = asyncio.create_task(
                    self._download_single_video(message, semaphore)
                )
                active_tasks.add(task)
                task.add_done_callback(active_tasks.discard)

                # Brief yield to allow other tasks to run
                await asyncio.sleep(0.01)

        except KeyboardInterrupt:
            self.logger.info("KeyboardInterrupt caught; initiating graceful shutdown...")
            self.shutdown_event.set()
        except Exception as exc:
            self.logger.error("Error during message iteration: %s", exc)

        # Wait for all active downloads to complete
        if active_tasks:
            self.logger.info(
                "Waiting for %d active download(s) to finish...", len(active_tasks)
            )
            await asyncio.gather(*active_tasks, return_exceptions=True)

        # Disconnect cleanly
        await self.client.disconnect()
        self.logger.info("Telegram client disconnected.")

        # Final summary
        elapsed = time.time() - self._start_time
        self.logger.info(
            "=== Session Complete ===\n"
            "  Downloaded: %d\n"
            "  Failed:     %d\n"
            "  Skipped:    %d\n"
            "  Elapsed:    %s",
            self.stats["downloaded"],
            self.stats["failed"],
            self.stats["skipped"],
            self._fmt_duration(elapsed),
        )
        await self._send_completion_notification()

    async def shutdown(self) -> None:
        """Signal the downloader to stop gracefully."""
        self.logger.info("Shutdown signal received.")
        self.shutdown_event.set()
        await self._send_stop_notification()


# ──────────────────────────────────────────────────────────────────────────────
# CLI Argument Parsing
# ──────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Async Telegram Channel Video Downloader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--channel",
        type=str,
        default=os.getenv("TELEGRAM_CHANNEL"),
        help="Channel username or ID (default: TELEGRAM_CHANNEL env var)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=os.getenv("OUTPUT_DIR", "videos_output"),
        help="Output folder for downloaded videos (default: videos_output)",
    )
    parser.add_argument(
        "--min-size",
        type=int,
        default=int(os.getenv("MIN_SIZE_MB", "100")),
        help="Minimum video size in MB to download (default: 100)",
    )
    parser.add_argument(
        "--concurrent",
        type=int,
        default=int(os.getenv("CONCURRENT", "15")),
        help="Max simultaneous downloads (default: 15)",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=int(os.getenv("RETRIES", "5")),
        help="Max retry attempts per video (default: 5)",
    )
    parser.add_argument(
        "--retry-delay",
        type=int,
        default=int(os.getenv("RETRY_DELAY", "5")),
        help="Initial retry delay in seconds (default: 5)",
    )
    parser.add_argument(
        "--max-retry-delay",
        type=int,
        default=int(os.getenv("MAX_RETRY_DELAY", "40")),
        help="Max retry delay in seconds (default: 40)",
    )
    parser.add_argument(
        "--session",
        type=str,
        default=os.getenv("SESSION_NAME", "anon"),
        help="Telethon session name (default: anon)",
    )
    parser.add_argument(
        "--disk-threshold",
        type=int,
        default=int(os.getenv("DISK_THRESHOLD_GB", "10")),
        help="Minimum free disk space in GB (default: 10)",
    )
    return parser.parse_args()


def build_config_from_args(args: argparse.Namespace) -> Config:
    """Build a Config object from parsed CLI arguments and environment."""
    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")

    if not api_id or not api_hash:
        raise SystemExit(
            "Error: TELEGRAM_API_ID and TELEGRAM_API_HASH must be set in .env "
            "or environment variables.\n"
            "Please copy .env.example to .env and fill in your credentials."
        )

    try:
        api_id_int = int(api_id)
    except ValueError:
        raise SystemExit(f"Error: TELEGRAM_API_ID must be an integer, got: {api_id}")

    if not args.channel:
        raise SystemExit(
            "Error: --channel or TELEGRAM_CHANNEL is required.\n"
            "Please provide a channel username or ID."
        )

    return Config(
        api_id=api_id_int,
        api_hash=api_hash,
        channel=args.channel,
        bot_token=os.getenv("TELEGRAM_BOT_API_KEY") or None,
        chat_id=os.getenv("TELEGRAM_CHAT_ID") or None,
        output_dir=args.output,
        min_size_mb=args.min_size,
        concurrent=args.concurrent,
        retries=args.retries,
        retry_delay=args.retry_delay,
        max_retry_delay=args.max_retry_delay,
        session_name=args.session,
        disk_threshold_gb=args.disk_threshold,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Signal Handling & Main Entry Point
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    """Main entry point."""
    load_dotenv()
    args = parse_args()
    config = build_config_from_args(args)

    downloader = TelegramVideoDownloader(config)

    # Cross-platform signal handling
    loop = asyncio.get_event_loop()

    def signal_handler(sig, frame):
        if not downloader.shutdown_event.is_set():
            asyncio.create_task(downloader.shutdown())

    try:
        # Windows supports SIGINT; Unix supports SIGINT + SIGTERM
        signal.signal(signal.SIGINT, signal_handler)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, signal_handler)
    except Exception:
        pass  # Ignore signal registration errors (e.g., Windows limitations)

    try:
        loop.run_until_complete(downloader.run())
    except KeyboardInterrupt:
        if not downloader.shutdown_event.is_set():
            loop.run_until_complete(downloader.shutdown())
    finally:
        # Ensure client disconnects even on unexpected errors
        if downloader.client and downloader.client.is_connected():
            loop.run_until_complete(downloader.client.disconnect())
        # Close remaining tasks
        pending = asyncio.all_tasks(loop)
        if pending:
            for task in pending:
                task.cancel()
            loop.run_until_complete(
                asyncio.gather(*pending, return_exceptions=True)
            )
        loop.close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Telegram Media Downloader

A comprehensive async Telegram media downloader using Telethon.
Scans all chats and downloads multiple media types with resume support,
progress tracking, and robust error handling.

Usage:
    python telegram_media_downloader.py --api-id <API_ID> --api-hash <API_HASH>

Dependencies:
    pip install telethon
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from telethon import TelegramClient
from telethon.errors import (
    ChannelInvalidError,
    FloodWaitError,
    RpcCallFailError,
)
from telethon.tl.types import (
    Document,
    Message,
    Photo,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

APP_NAME = "telegram_media_downloader"
APP_VERSION = "1.0.0"
STATE_VERSION = 1

# Media attribute to message attribute mapping
MEDIA_ATTR_MAP = {
    "photo": "photo",
    "video": "video",
    "audio": "audio",
    "document": "document",
    "voice": "voice",
    "sticker": "sticker",
}

# Default file extensions per media type
DEFAULT_EXTENSIONS = {
    "photo": "jpg",
    "video": "mp4",
    "audio": "mp3",
    "document": "bin",
    "voice": "ogg",
    "sticker": "webp",
}

# ---------------------------------------------------------------------------
# CLI Arguments
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download media from all Telegram chats with resume support.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  %(prog)s --api-id 12345 --api-hash abcdef...\n"
            "  %(prog)s --api-id 12345 --api-hash abcdef... --chat @mychannel\n"
            "  %(prog)s --api-id 12345 --api-hash abcdef... --chat -1001234567890 --media-types video\n"
            "  %(prog)s --api-id 12345 --api-hash abcdef... --chat @group1 @group2 --media-types photo\n"
            "  %(prog)s --api-id 12345 --api-hash abcdef... --media-types photo --date-from 2024-01-01\n"
            "  %(prog)s --api-id 12345 --api-hash abcdef... --reset --concurrent 4\n"
        ),
    )

    parser.add_argument("--api-id", type=int, required=True, help="Telegram API ID")
    parser.add_argument("--api-hash", type=str, required=True, help="Telegram API hash")
    parser.add_argument(
        "--session",
        type=str,
        default="telegram_media_downloader",
        help="Session file name (default: telegram_media_downloader)",
    )
    parser.add_argument(
        "--phone",
        type=str,
        default=None,
        help="Phone number with country code (e.g. +1234567890)",
    )
    parser.add_argument(
        "--download-dir",
        type=str,
        default="./downloads",
        help="Output folder for downloaded media (default: ./downloads)",
    )
    parser.add_argument(
        "--media-types",
        type=str,
        nargs="+",
        default=["photo", "video"],
        help="Space-separated media types: photo, video, audio, document, voice, sticker (default: photo video)",
    )
    parser.add_argument(
        "--max-size",
        type=int,
        default=2048,
        help="Maximum file size in MB (default: 2048)",
    )
    parser.add_argument(
        "--min-size",
        type=int,
        default=0,
        help="Minimum file size in KB (default: 0)",
    )
    parser.add_argument(
        "--date-from",
        type=str,
        default=None,
        help="Start date filter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--date-to",
        type=str,
        default=None,
        help="End date filter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--no-private",
        action="store_true",
        help="Skip 1-on-1 private chats",
    )
    parser.add_argument(
        "--no-groups",
        action="store_true",
        help="Skip groups",
    )
    parser.add_argument(
        "--no-channels",
        action="store_true",
        help="Skip channels",
    )
    parser.add_argument(
        "--chat",
        type=str,
        nargs="*",
        default=None,
        help=(
            "Download ONLY from specific chat(s) by ID or username. "
            "Examples: --chat @mychannel  --chat -1001234567890  --chat @group1 @group2"
        ),
    )
    parser.add_argument(
        "--exclude-chats",
        type=str,
        nargs="*",
        default=[],
        help="Space-separated chat IDs or usernames to skip",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=1.5,
        help="Seconds to sleep between API calls (default: 1.5)",
    )
    parser.add_argument(
        "--concurrent",
        type=int,
        default=2,
        help="Maximum simultaneous downloads (default: 2)",
    )
    parser.add_argument(
        "--skip-forwards",
        action="store_true",
        help="Skip forwarded messages",
    )
    parser.add_argument(
        "--organize-by-date",
        action="store_true",
        help="Create YYYY-MM subfolders for downloads",
    )
    parser.add_argument(
        "--no-dedup",
        action="store_true",
        help="Disable duplicate detection by media file ID",
    )
    parser.add_argument(
        "--state-file",
        type=str,
        default="download_state.json",
        help="Resume state file path (default: download_state.json)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Clear state and restart from scratch",
    )

    return parser


# ---------------------------------------------------------------------------
# State Management
# ---------------------------------------------------------------------------


@dataclass
class DownloadState:
    version: int = STATE_VERSION
    downloaded_ids: set[str] = field(default_factory=set)
    chat_offsets: dict[str, int] = field(default_factory=dict)
    total_downloaded: int = 0
    total_failed: int = 0
    total_skipped: int = 0

    @classmethod
    def load(cls, path: str, reset: bool = False) -> "DownloadState":
        if reset:
            print("[State] Reset requested — starting fresh.")
            return cls()
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                state = cls(
                    version=data.get("version", STATE_VERSION),
                    downloaded_ids=set(data.get("downloaded_ids", [])),
                    chat_offsets=data.get("chat_offsets", {}),
                    total_downloaded=data.get("total_downloaded", 0),
                    total_failed=data.get("total_failed", 0),
                    total_skipped=data.get("total_skipped", 0),
                )
                print(f"[State] Loaded from '{path}' — "
                      f"{len(state.downloaded_ids)} media IDs, "
                      f"{len(state.chat_offsets)} chat offsets.")
                return state
            except (json.JSONDecodeError, KeyError, TypeError) as exc:
                print(f"[State] Corrupt state file ({exc}) — starting fresh.")
                return cls()
        return cls()

    def save(self, path: str) -> None:
        data = {
            "version": self.version,
            "downloaded_ids": sorted(self.downloaded_ids),
            "chat_offsets": self.chat_offsets,
            "total_downloaded": self.total_downloaded,
            "total_failed": self.total_failed,
            "total_skipped": self.total_skipped,
        }
        tmp_path = path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, path)


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------


def sanitize_folder_name(name: str) -> str:
    """Replace invalid filesystem characters."""
    sanitized = re.sub(r'[\\/*?:"<>|]', "_", name)
    sanitized = sanitized.strip(". ")
    return sanitized or "unnamed"


def chat_folder_name(chat_title: str, chat_id: int) -> str:
    """Generate a unique folder name for a chat."""
    safe_title = sanitize_folder_name(chat_title)
    return f"{safe_title}_{chat_id}"


def media_file_name(media_type: str, message_id: int, ext: str) -> str:
    """Generate a consistent file name for downloaded media."""
    return f"{media_type}_{message_id}.{ext}"


def parse_date(date_str: str | None) -> datetime | None:
    """Parse YYYY-MM-DD to timezone-aware datetime (UTC)."""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError(f"Invalid date format '{date_str}': expected YYYY-MM-DD") from exc


def get_media_type_from_message(message: Message) -> str | None:
    """Return the media type string if the message contains media, else None."""
    for media_type, attr in MEDIA_ATTR_MAP.items():
        if getattr(message, attr, None) is not None:
            return media_type
    return None


def get_media_size(message: Message, media_type: str) -> int | None:
    """Return media size in bytes, or None if unavailable."""
    media = getattr(message, media_type, None)
    if media is None:
        return None
    if isinstance(media, Photo):
        return media.sizes[-1].size if media.sizes else None
    if isinstance(media, Document):
        return media.size
    # For video, audio, voice, sticker — they wrap a Document
    if hasattr(media, "size"):
        return media.size
    if hasattr(media, "document") and media.document:
        return media.document.size
    return None


def get_media_extension(message: Message, media_type: str) -> str:
    """Extract or guess file extension from media."""
    media = getattr(message, media_type, None)
    if media is None:
        return DEFAULT_EXTENSIONS.get(media_type, "bin")

    # Try to get mime_type-based extension
    mime_type = None
    if isinstance(media, Document):
        mime_type = media.mime_type
    elif hasattr(media, "mime_type"):
        mime_type = media.mime_type
    elif hasattr(media, "document") and media.document:
        mime_type = media.document.mime_type

    if mime_type:
        ext = _mime_to_ext(mime_type)
        if ext:
            return ext

    # Fallback to default
    return DEFAULT_EXTENSIONS.get(media_type, "bin")


def _mime_to_ext(mime_type: str) -> str | None:
    """Simple MIME type to extension mapping."""
    mapping = {
        "image/jpeg": "jpg",
        "image/png": "png",
        "image/gif": "gif",
        "image/webp": "webp",
        "video/mp4": "mp4",
        "video/x-matroska": "mkv",
        "video/avi": "avi",
        "video/quicktime": "mov",
        "audio/mpeg": "mp3",
        "audio/ogg": "ogg",
        "audio/mp4": "m4a",
        "audio/wav": "wav",
        "application/pdf": "pdf",
        "application/zip": "zip",
        "application/x-rar-compressed": "rar",
        "application/x-tar": "tar",
        "application/x-7z-compressed": "7z",
        "application/msword": "doc",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
        "application/vnd.ms-excel": "xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
        "text/plain": "txt",
        "text/html": "html",
    }
    return mapping.get(mime_type)


def get_media_id(message: Message, media_type: str) -> str | None:
    """Get a unique identifier for the media file (for deduplication)."""
    media = getattr(message, media_type, None)
    if media is None:
        return None
    if hasattr(media, "id") and media.id is not None:
        return f"{media_type}_{media.id}"
    if hasattr(media, "document") and media.document and media.document.id:
        return f"{media_type}_{media.document.id}"
    return f"{media_type}_{message.id}"


# ---------------------------------------------------------------------------
# Progress Display
# ---------------------------------------------------------------------------


class ProgressTracker:
    """Simple console progress display."""

    def __init__(self) -> None:
        self.chats_processed = 0
        self.total_chats = 0
        self.files_downloaded = 0
        self.files_failed = 0
        self.files_skipped = 0
        self.bytes_downloaded = 0
        self._start_time = time.monotonic()
        self._last_display_time = 0.0

    def set_total_chats(self, n: int) -> None:
        self.total_chats = n

    def display(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_display_time < 1.0:
            return
        self._last_display_time = now

        elapsed = now - self._start_time
        speed = self.bytes_downloaded / elapsed if elapsed > 0 else 0

        if speed >= 1024 * 1024:
            speed_str = f"{speed / (1024 * 1024):.1f} MB/s"
        elif speed >= 1024:
            speed_str = f"{speed / 1024:.1f} KB/s"
        else:
            speed_str = f"{speed:.0f} B/s"

        chat_info = f"{self.chats_processed}/{self.total_chats}" if self.total_chats else str(self.chats_processed)

        line = (
            f"\r[Progress] Chats: {chat_info} | "
            f"Downloaded: {self.files_downloaded} | "
            f"Failed: {self.files_failed} | "
            f"Skipped: {self.files_skipped} | "
            f"Speed: {speed_str}"
        )
        sys.stdout.write(line.ljust(90))
        sys.stdout.flush()

    def finalize(self) -> None:
        self.display(force=True)
        print()  # newline


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------


async def authenticate(client: TelegramClient, phone: str | None) -> None:
    """Handle first-run authentication: phone number, code, password."""
    if not client.is_connected():
        await client.connect()

    if await client.is_user_authorized():
        return

    if not phone:
        phone = input("Enter your phone number (with country code, e.g. +1234567890): ").strip()

    await client.send_code_request(phone)
    code = input("Enter the code you received: ").strip()
    try:
        await client.sign_in(phone, code)
    except Exception:
        # May need 2FA password
        password = input("Two-factor authentication enabled. Enter your password: ").strip()
        await client.sign_in(password=password)

    print("[Auth] Successfully authenticated!")


# ---------------------------------------------------------------------------
# Download Logic
# ---------------------------------------------------------------------------


async def download_media_file(
    client: TelegramClient,
    message: Message,
    media_type: str,
    output_path: str,
    state: DownloadState,
    progress: ProgressTracker,
    dedup: bool,
    args: argparse.Namespace,
) -> bool:
    """Download a single media file. Returns True on success."""
    media_id = get_media_id(message, media_type)

    # Duplicate check
    if dedup and media_id and media_id in state.downloaded_ids:
        progress.files_skipped += 1
        state.total_skipped += 1
        return False  # skipped, not failed

    # Size check
    media_size = get_media_size(message, media_type)
    max_bytes = args.max_size * 1024 * 1024  # MB to bytes
    min_bytes = args.min_size * 1024  # KB to bytes

    if media_size is not None:
        if media_size > max_bytes:
            progress.files_skipped += 1
            state.total_skipped += 1
            return False
        if media_size < min_bytes:
            progress.files_skipped += 1
            state.total_skipped += 1
            return False

    # Build file path
    ext = get_media_extension(message, media_type)
    file_name = media_file_name(media_type, message.id, ext)

    if args.organize_by_date and message.date:
        date_folder = message.date.strftime("%Y-%m")
        output_dir = os.path.join(os.path.dirname(output_path), date_folder)
    else:
        output_dir = os.path.dirname(output_path)

    os.makedirs(output_dir, exist_ok=True)
    final_path = os.path.join(output_dir, file_name)

    # Skip if file already exists on disk
    if os.path.exists(final_path):
        if dedup and media_id:
            state.downloaded_ids.add(media_id)
        progress.files_skipped += 1
        state.total_skipped += 1
        return True

    # Download
    try:
        downloaded_path = await client.download_media(
            message=message,
            file=final_path,
            progress_callback=None,
        )

        if downloaded_path:
            file_size = os.path.getsize(downloaded_path)
            progress.files_downloaded += 1
            progress.bytes_downloaded += file_size
            state.total_downloaded += 1
            if dedup and media_id:
                state.downloaded_ids.add(media_id)
            return True
        else:
            progress.files_failed += 1
            state.total_failed += 1
            return False

    except FloodWaitError as exc:
        wait_time = exc.seconds + 5
        print(f"\n[Rate Limit] FloodWaitError: sleeping {wait_time}s ...")
        await asyncio.sleep(wait_time)
        # Retry once
        try:
            downloaded_path = await client.download_media(
                message=message,
                file=final_path,
            )
            if downloaded_path:
                file_size = os.path.getsize(downloaded_path)
                progress.files_downloaded += 1
                progress.bytes_downloaded += file_size
                state.total_downloaded += 1
                if dedup and media_id:
                    state.downloaded_ids.add(media_id)
                return True
        except Exception as retry_exc:
            print(f"\n[Download] Retry failed for msg {message.id}: {retry_exc}")
        progress.files_failed += 1
        state.total_failed += 1
        return False

    except Exception as exc:
        print(f"\n[Download] Error on msg {message.id}: {exc}")
        progress.files_failed += 1
        state.total_failed += 1
        return False


async def process_chat(
    client: TelegramClient,
    dialog: Any,
    state: DownloadState,
    progress: ProgressTracker,
    args: argparse.Namespace,
    semaphore: asyncio.Semaphore,
) -> None:
    """Process a single chat: iterate messages, filter, download media."""
    chat = dialog.entity
    chat_id = str(chat.id)
    chat_title = getattr(chat, "title", None) or getattr(chat, "first_name", "") or "unknown"
    folder_name = chat_folder_name(chat_title, chat.id)
    output_dir = os.path.join(args.download_dir, folder_name)

    # Determine resume offset
    offset_id = state.chat_offsets.get(chat_id, 0)
    if offset_id == -1:
        print(f"\n[Chat] {chat_title} — already fully processed, skipping.")
        return

    print(f"\n[Chat] {chat_title} (id={chat.id}) — starting download...")

    # Date filters
    date_from = parse_date(args.date_from)
    date_to = parse_date(args.date_to)
    if date_to:
        date_to = date_to.replace(hour=23, minute=59, second=59)

    downloaded_in_chat = 0
    messages_in_chat = 0

    try:
        async for message in client.iter_messages(
            chat,
            reverse=False,
            offset_id=offset_id if offset_id > 0 else 0,
        ):
            if not isinstance(message, Message):
                continue

            messages_in_chat += 1

            # Update offset immediately for resume safety
            state.chat_offsets[chat_id] = message.id

            # Forward filter
            if args.skip_forwards and message.forward:
                continue

            # Date filter
            if date_from and message.date and message.date < date_from:
                continue
            if date_to and message.date and message.date > date_to:
                break  # Messages are newest-first, so older ones come later

            # Media type check
            media_type = get_media_type_from_message(message)
            if media_type is None or media_type not in args.media_types:
                continue

            # Rate limit before download
            await asyncio.sleep(args.rate_limit)

            async with semaphore:
                success = await download_media_file(
                    client=client,
                    message=message,
                    media_type=media_type,
                    output_path=output_dir,
                    state=state,
                    progress=progress,
                    dedup=not args.no_dedup,
                    args=args,
                )
                if success:
                    downloaded_in_chat += 1

                # Save state every 10 downloads
                if state.total_downloaded > 0 and state.total_downloaded % 10 == 0:
                    state.save(args.state_file)

                progress.display()

        # Mark chat as fully processed
        state.chat_offsets[chat_id] = -1
        progress.chats_processed += 1
        print(
            f"\n[Chat] {chat_title} — "
            f"scanned {messages_in_chat} messages, downloaded {downloaded_in_chat} files."
        )

    except ChannelInvalidError:
        print(f"\n[Chat] {chat_title} — ChannelInvalidError, skipping.")
        state.chat_offsets[chat_id] = -1
        progress.chats_processed += 1
    except FloodWaitError as exc:
        wait_time = exc.seconds + 5
        print(f"\n[Rate Limit] FloodWaitError for chat {chat_title}: sleeping {wait_time}s ...")
        await asyncio.sleep(wait_time)
        # Mark for retry next run
        del state.chat_offsets[chat_id]
    except Exception as exc:
        print(f"\n[Chat] Error processing {chat_title}: {exc}")
        # Keep offset so we can resume

    # Save state after each chat
    state.save(args.state_file)
    progress.display(force=True)


async def resolve_target_chats(
    client: TelegramClient,
    chat_identifiers: list[str],
) -> list[Any]:
    """Resolve chat IDs/usernames into dialog-like objects for direct targeting."""
    results = []
    for identifier in chat_identifiers:
        try:
            entity = await client.get_entity(identifier)
            # Wrap in a simple namespace-like object compatible with process_chat
            class _FakeDialog:
                def __init__(self, entity):
                    self.entity = entity
            results.append(_FakeDialog(entity))
            print(f"[Target] Resolved '{identifier}' -> {getattr(entity, 'title', identifier)}")
        except Exception as exc:
            print(f"[Warning] Could not resolve chat '{identifier}': {exc}")
    return results


async def scan_dialogs(
    client: TelegramClient,
    state: DownloadState,
    progress: ProgressTracker,
    args: argparse.Namespace,
    semaphore: asyncio.Semaphore,
) -> None:
    """Iterate over all dialogs and process each eligible chat."""
    # If --chat is specified, resolve those chats directly instead of scanning all dialogs
    if args.chat:
        dialogs = await resolve_target_chats(client, args.chat)
        if not dialogs:
            print("[Error] No target chats could be resolved. Check the chat ID/username.")
            return
        progress.set_total_chats(len(dialogs))
        print(f"[Info] Targeting {len(dialogs)} specific chat(s).")
        for dialog in dialogs:
            await process_chat(client, dialog, state, progress, args, semaphore)
        return

    dialogs: list[Any] = []
    async for dialog in client.iter_dialogs():
        dialogs.append(dialog)

    progress.set_total_chats(len(dialogs))
    print(f"[Info] Found {len(dialogs)} dialogs to process.")

    # Build exclude set
    exclude_chats: set[str] = set(args.exclude_chats or [])

    for dialog in dialogs:
        chat = dialog.entity
        chat_id = str(chat.id)

        # Skip excluded chats (by ID or username)
        chat_username = getattr(chat, "username", None) or ""
        if chat_id in exclude_chats or chat_username in exclude_chats:
            print(f"\n[Skip] Excluded chat: {getattr(chat, 'title', chat_username)}")
            continue

        # Skip by chat type
        chat_type = "unknown"
        if getattr(chat, "broadcast", False):
            chat_type = "channel"
        elif getattr(chat, "megagroup", False) or getattr(chat, "gigagroup", False):
            chat_type = "group"
        else:
            participants = getattr(chat, "participants_count", None)
            if participants is not None and participants > 2:
                chat_type = "group"
            else:
                chat_type = "private"

        if args.no_private and chat_type == "private":
            continue
        if args.no_groups and chat_type == "group":
            continue
        if args.no_channels and chat_type == "channel":
            continue

        await process_chat(client, dialog, state, progress, args, semaphore)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    # Validate media types
    valid_types = set(MEDIA_ATTR_MAP.keys())
    for mt in args.media_types:
        if mt not in valid_types:
            print(f"[Error] Invalid media type '{mt}'. Valid: {', '.join(sorted(valid_types))}")
            sys.exit(1)

    # Paths
    os.makedirs(args.download_dir, exist_ok=True)
    state_file_path = os.path.abspath(args.state_file)

    # Load (or reset) state
    state = DownloadState.load(state_file_path, reset=args.reset)

    # Progress
    progress = ProgressTracker()
    progress.files_downloaded = state.total_downloaded
    progress.files_failed = state.total_failed
    progress.files_skipped = state.total_skipped

    # Concurrent downloads semaphore
    semaphore = asyncio.Semaphore(max(1, args.concurrent))

    print(f"=== {APP_NAME} v{APP_VERSION} ===")
    print(f"[Config] Session: {args.session}")
    print(f"[Config] Download dir: {os.path.abspath(args.download_dir)}")
    print(f"[Config] Media types: {args.media_types}")
    print(f"[Config] Max size: {args.max_size} MB")
    print(f"[Config] Min size: {args.min_size} KB")
    print(f"[Config] Date from: {args.date_from or '—'}")
    print(f"[Config] Date to: {args.date_to or '—'}")
    print(f"[Config] Skip forwards: {args.skip_forwards}")
    print(f"[Config] Organize by date: {args.organize_by_date}")
    print(f"[Config] Deduplication: {not args.no_dedup}")
    print(f"[Config] Rate limit: {args.rate_limit}s")
    print(f"[Config] Concurrent: {args.concurrent}")
    print(f"[Config] No private: {args.no_private}")
    print(f"[Config] No groups: {args.no_groups}")
    print(f"[Config] No channels: {args.no_channels}")
    print(f"[Config] Target chat(s): {args.chat or 'All chats'}")
    print(f"[Config] Exclude chats: {args.exclude_chats or '—'}")
    print()

    # Connect and authenticate
    client = TelegramClient(args.session, args.api_id, args.api_hash)
    try:
        await client.start(phone=lambda: args.phone or input("Phone number: "))
        print(f"[Auth] Logged in as {(await client.get_me()).first_name}\n")

        await scan_dialogs(client, state, progress, args, semaphore)

        print("\n=== Download Complete ===")
        progress.finalize()
        print(f"Total downloaded: {state.total_downloaded}")
        print(f"Total failed:     {state.total_failed}")
        print(f"Total skipped:    {state.total_skipped}")
        state.save(state_file_path)
        print(f"State saved to: {state_file_path}")

    except KeyboardInterrupt:
        print("\n\n[Interrupt] Saving state before exit...")
        state.save(state_file_path)
        progress.finalize()
        print(f"State saved to: {state_file_path}")
        sys.exit(130)
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

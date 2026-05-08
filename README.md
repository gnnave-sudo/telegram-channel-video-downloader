# Telegram Channel Video Downloader

A production-ready, asynchronous Telegram channel video downloader built with Python and [Telethon](https://github.com/LonamiWebs/Telethon). Features resumable downloads, concurrency control, disk space monitoring, retry logic with exponential backoff, integrity verification, duplicate skipping, and Telegram bot notifications.

---

## Features

| Feature | Description |
|---------|-------------|
| **Async Downloads** | Non-blocking I/O with `asyncio` for maximum throughput |
| **Concurrency Control** | Semaphore-limited simultaneous downloads (default: 15) |
| **Resumable Downloads** | Interrupted downloads resume from the last byte offset |
| **Exponential Backoff** | Retry delay doubles after each failure: 5s -> 10s -> 20s -> 40s |
| **Disk Space Guard** | Aborts if free space drops below threshold (default: 10 GB) |
| **Integrity Verification** | Validates file size matches expected value before finalizing |
| **Duplicate Skipping** | Tracks downloaded IDs; skips already-downloaded files on restart |
| **Telegram Notifications** | Sends progress, completion, and error alerts via your bot |
| **File Reference Refresh** | Auto-refreshes expired Telegram file references |
| **Graceful Shutdown** | Ctrl+C triggers clean disconnect; waits for active downloads |

---

## Prerequisites

- **Python 3.7+**
- **Telegram API Credentials**: `api_id` and `api_hash` from [my.telegram.org/apps](https://my.telegram.org/apps)
- **Telegram Bot** (optional): For progress notifications via [@BotFather](https://t.me/BotFather)

---

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/telegram-channel-video-downloader.git
cd telegram-channel-video-downloader
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` and fill in your credentials:

```env
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=your_api_hash_here
TELEGRAM_CHANNEL=@your_channel_username

# Optional: for Telegram bot notifications
TELEGRAM_BOT_API_KEY=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
```

### Getting Your Credentials

**API ID & Hash:**
1. Visit [my.telegram.org/apps](https://my.telegram.org/apps)
2. Log in with your Telegram account
3. Fill out the application form (any name/description works)
4. Copy the **API ID** and **API Hash**

**Bot Token & Chat ID (for notifications):**
1. Message [@BotFather](https://t.me/BotFather) on Telegram
2. Create a new bot with `/newbot`
3. Copy the bot token
4. Message [@userinfobot](https://t.me/userinfobot) to get your Chat ID

---

## Usage

### Basic Run (uses `.env` defaults)

```bash
python telegram_downloader.py
```

### Command Line Options

| Flag | Description | Default |
|------|-------------|---------|
| `--channel` | Channel username or ID | From `.env` |
| `--output` | Output folder | `videos_output` |
| `--min-size` | Minimum video size in MB | `100` |
| `--concurrent` | Max simultaneous downloads | `15` |
| `--retries` | Max retry attempts | `5` |
| `--retry-delay` | Initial retry delay (seconds) | `5` |
| `--max-retry-delay` | Max retry delay (seconds) | `40` |
| `--session` | Telethon session name | `anon` |
| `--disk-threshold` | Min free space in GB | `10` |

### Examples

```bash
# Download only very large videos (500MB+)
python telegram_downloader.py --min-size 500

# Reduce concurrency for slower connections
python telegram_downloader.py --concurrent 5

# Specific channel with custom output folder
python telegram_downloader.py --channel @mychannel --output ./backups/videos

# Higher disk threshold for safety
python telegram_downloader.py --disk-threshold 50
```

---

## File Structure

```
telegram-channel-video-downloader/
|-- telegram_downloader.py    # Main script
|-- requirements.txt          # Python dependencies
|-- .env                      # Credentials (gitignored)
|-- .env.example              # Template for sharing
|-- .gitignore                # Git ignore rules
|-- README.md                 # This file
|-- videos_output/            # Downloaded videos (auto-created)
|   |-- 2024-01-15_10-30-00_12345.mp4
|   |-- 2024-01-15_11-45-00_12346.mp4
|   |-- downloaded_videos.txt # ID tracking log
|-- logs/                     # Log files (auto-created)
|   |-- telegram_downloader_20240115_103045.log
```

---

## How It Works

1. **Authentication** - Creates a `.session` file after first login (reused automatically)
2. **Scanning** - Iterates channel messages filtered by `InputMessagesFilterVideo`
3. **Filtering** - Skips already-downloaded IDs and files below size threshold
4. **Downloading** - Writes to `.temp` file, verifies size, then renames to final
5. **Resuming** - Checks existing `.temp` file size and uses `offset` parameter
6. **Retrying** - On timeout, waits with exponential backoff up to max retries
7. **Notifying** - Sends batch updates every 10 downloads and final summary

---

## Key Behaviors

- **Interrupting**: Press `Ctrl+C` - the script catches `KeyboardInterrupt`, sends a stop notification, and disconnects cleanly after finishing active downloads
- **Resuming**: Restart the script - it skips completed files and resumes partial `.temp` files
- **Errors**: Failed downloads after max retries are logged and notified; the script continues with the remaining queue
- **Disk Space**: If space drops below the threshold, downloads pause and a warning is sent

---

## Security Notes

- **Never commit `.env`** - it is already in `.gitignore`
- **Protect `.session` files** - they contain your Telegram authentication (already in `.gitignore`)
- **Use a dedicated API app** - don't share API credentials across projects

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `FileReferenceExpiredError` | Script auto-refreshes; if persistent, restart the script |
| `TimedOutError` repeatedly | Lower `--concurrent` or increase `--retry-delay` |
| Disk full warnings | Increase `--disk-threshold` or free up space |
| Channel not found | Use full ID format `-1001234567890` or exact `@username` |
| No notifications | Verify `TELEGRAM_BOT_API_KEY` and `TELEGRAM_CHAT_ID` |

---

## License

MIT License - feel free to use, modify, and distribute.

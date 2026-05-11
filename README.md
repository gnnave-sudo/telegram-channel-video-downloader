# Telegram Media Downloader

A comprehensive **video/photo downloader for all your Telegram chats** built with [Telethon](https://codeberg.org/Lonami/Telethon). Supports channels, groups, and private chats with duplicate detection, resume support, and configurable filters.

## Features

- **All Chats**: Scans and downloads from every chat you're in — channels, groups, and private conversations
- **Smart Filtering**: Filter by media type, date range, file size, chat type
- **Duplicate Detection**: Tracks downloaded media by unique ID so nothing is downloaded twice
- **Resume Support**: Persistent state file — stop and resume anytime without losing progress
- **Rate Limiting**: Built-in delays and flood-wait handling to keep your account safe
- **Organized Output**: Files saved in folders named by chat (`downloads/ChatName_ID/`)
- **Progress Tracking**: Real-time console display showing chats processed, files downloaded, and transfer speed
- **Concurrent Downloads**: Configurable parallel downloads for faster completion

## Quick Start

### 1. Get API Credentials

You need an `api_id` and `api_hash` from Telegram:

1. Go to [https://my.telegram.org](https://my.telegram.org)
2. Log in with your phone number
3. Click **API development tools**
4. Create a new app (any name works)
5. Copy the **api_id** (numbers) and **api_hash** (letters/numbers)

### 2. Install Dependencies

```bash
pip install telethon
```

Or manually:
```bash
pip install telethon
```

### 3. Run the Downloader

```bash
python telegram_downloader.py \
    --api-id 12345 \
    --api-hash abcdef0123456789abcdef0123456789 \
    --download-dir ./my_downloads
```

On first run, you'll receive a login code via Telegram. Enter it when prompted.

## Usage Examples

### Download only photos from 2024
```bash
python telegram_downloader.py \
    --api-id 12345 --api-hash abcdef... \
    --media-types photo \
    --date-from 2024-01-01
```

### Download only from groups, skip channels and private chats
```bash
python telegram_downloader.py \
    --api-id 12345 --api-hash abcdef... \
    --no-private --no-channels
```

### Download only videos under 500MB, skip specific chats
```bash
python telegram_downloader.py \
    --api-id 12345 --api-hash abcdef... \
    --media-types video \
    --max-size 500 \
    --exclude-chats spam_channel_1 -1001234567890
```

### Resume an interrupted download
```bash
# Just run the same command again — progress is saved automatically
python telegram_downloader.py --api-id 12345 --api-hash abcdef...
```

### Start fresh (ignore previous state)
```bash
python telegram_downloader.py --api-id 12345 --api-hash abcdef... --reset
```

### Organize by date subfolders
```bash
python telegram_downloader.py \
    --api-id 12345 --api-hash abcdef... \
    --organize-by-date
```

## Command Reference

| Option | Default | Description |
|--------|---------|-------------|
| `--api-id` | *(required)* | Telegram API ID |
| `--api-hash` | *(required)* | Telegram API hash |
| `--session` | `telegram_media_downloader` | Session file name |
| `--phone` | *(prompts)* | Phone number with country code |
| `--download-dir` | `./downloads` | Output folder |
| `--media-types` | `photo video` | Space-separated: `photo`, `video`, `audio`, `document`, `voice`, `sticker` |
| `--max-size` | `2048` | Max file size in MB |
| `--min-size` | `0` | Min file size in KB |
| `--date-from` | — | Start date (YYYY-MM-DD) |
| `--date-to` | — | End date (YYYY-MM-DD) |
| `--no-private` | — | Skip 1-on-1 chats |
| `--no-groups` | — | Skip groups |
| `--no-channels` | — | Skip channels |
| `--exclude-chats` | — | Space-separated chat IDs/usernames to skip |
| `--rate-limit` | `1.5` | Seconds between API calls |
| `--concurrent` | `2` | Simultaneous downloads |
| `--skip-forwards` | — | Skip forwarded messages |
| `--organize-by-date` | — | Create `YYYY-MM` subfolders |
| `--no-dedup` | — | Disable duplicate detection |
| `--state-file` | `download_state.json` | Resume state file |
| `--reset` | — | Clear state and restart |

## File Structure

After downloading, your files are organized like this:

```
downloads/
├── My_Channel_1_-1001234567890/
│   ├── photo_123.jpg
│   ├── video_456.mp4
│   └── document_789.pdf
├── Private_Chat_987654321/
│   └── photo_100.jpg
└── Group_Chat_111111111/
    ├── video_200.mp4
    └── video_201.mp4
```

With `--organize-by-date`:
```
downloads/
├── My_Channel_1_-1001234567890/
│   ├── 2024-01/
│   │   └── photo_123.jpg
│   └── 2024-03/
│       └── video_456.mp4
```

## How It Works

1. **Connects** to Telegram using your API credentials
2. **Scans** all dialogs (chats, groups, channels) you have access to
3. **Iterates** messages newest-first, filtering for media you configured
4. **Downloads** each media file with progress tracking
5. **Saves** state after every 10 files and after each chat — safe to interrupt anytime

## Safety Notes

- **Telegram ToS**: Don't abuse this. Aggressive scraping can get your account limited. The default `--rate-limit 1.5` is conservative.
- **Flood waits**: If Telegram asks you to wait, the script handles it automatically.
- **Storage**: Large channels can have terabytes of media. Check available disk space and use `--max-size` / `--date-from` filters.

## Troubleshooting

### "No media found in chat"
- Check that `--media-types` includes the type you're looking for
- Use `--date-from` to verify messages exist in that date range

### "FloodWaitError"
- This is normal. The script sleeps automatically and resumes.
- Increase `--rate-limit` if it happens frequently.

### Session expires / "Invalid password"
- Delete the `.session` file and run again to re-authenticate.

### Want to skip already-processed chats
- Edit `download_state.json` and set the chat offset to `-1` for chats you want to skip.

## License

This project is unlicensed — use it however you want. Respect Telegram's Terms of Service and the privacy of chat participants.

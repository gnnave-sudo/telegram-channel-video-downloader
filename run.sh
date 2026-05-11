#!/bin/bash
cd /opt/telegram-downloader
source venv/bin/activate
exec python telegram_media_downloader.py "$@"

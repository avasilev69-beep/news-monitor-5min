# News Monitor 5min

Real-time financial news monitoring with 5-minute intervals.

## Overview

Monitors 39 financial instruments via Finnhub API and sends new news alerts to Telegram every 5 minutes.

## Architecture

- **Trigger**: cron-job.org (5-min intervals)
- **Executor**: GitHub Actions
- **Data**: Finnhub API
- **Notifications**: Telegram Bot
- **State**: state_5min.json (Git-tracked)

## Setup

### 1. GitHub Secrets

Required secrets in repo settings:

- `FINNHUB_API_KEY`: Your Finnhub API key
- `NEWS_5MIN_TELEGRAM_BOT_TOKEN`: Telegram bot token
- `NEWS_5MIN_TELEGRAM_CHAT_ID`: Telegram channel ID (negative for private channels)

### 2. GitHub PAT (for cron-job.org)

Create Personal Access Token with:
- `contents: read/write`
- `actions: read/write`
- 90-day expiration

### 3. Telegram Setup

1. Create private channel: `News Monitor 5min`
2. Create bot via @BotFather
3. Add bot as admin to channel
4. Get Chat ID from: `https://api.telegram.org/bot{TOKEN}/getUpdates`

## Files

- `news_monitor_5min.py` — Main monitoring script
- `state_5min.json` — Cached news state (auto-generated)
- `tickers.txt` — List of 39 tickers (to be added)
- `.github/workflows/news-monitor-5min.yml` — GitHub Actions workflow

## Workflow

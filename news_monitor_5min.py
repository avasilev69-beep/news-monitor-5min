#!/usr/bin/env python3
"""
News Monitor 5min - Real-time financial news monitoring.

Monitors US tickers via Finnhub (per-company news) plus a general/macro
news stream (keyword-filtered, not tied to a ticker), and sends new
alerts to Telegram every 5 minutes.
"""

import asyncio
import html
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import aiohttp

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("NEWS_5MIN_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("NEWS_5MIN_TELEGRAM_CHAT_ID")
MACRO_CHAT_ID = os.getenv("NEWS_5MIN_TELEGRAM_MACRO_CHAT_ID")

FINNHUB_BASE_URL = "https://finnhub.io/api/v1"

STATE_FILE = Path("state_5min.json")
TICKERS_FILE = Path("tickers.txt")
KEYWORDS_FILE = Path("keywords.txt")

SEMAPHORE_LIMIT = 10
REQUEST_TIMEOUT = 10
RETRY_MAX_ATTEMPTS = 3
RETRY_BASE_DELAY = 1

TELEGRAM_MIN_INTERVAL = 0.35  # мин. пауза между съобщения (сек) - предпазва от 429
TELEGRAM_MAX_RETRIES = 3

LOG_FORMAT = "%(asctime)s | %(levelname)-5s | %(name)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, stream=sys.stdout)
logger = logging.getLogger(__name__)


class NewsArticle:
    def __init__(self, article_id, headline, source, datetime_, url):
        self.id = article_id
        self.headline = headline
        self.source = source
        self.datetime = datetime_
        self.url = url

    def __repr__(self):
        return f"NewsArticle(id={self.id}, headline={self.headline[:50]}...)"


class MonitorState:
    def __init__(self):
        self.last_check = ""
        self.tickers_processed = 0
        self.tickers_succeeded = 0
        self.tickers_failed = []
        self.seen_news_ids = {}
        self.general_seen_ids = []

    def to_dict(self):
        return {
            "last_check": self.last_check,
            "tickers_processed": self.tickers_processed,
            "tickers_succeeded": self.tickers_succeeded,
            "tickers_failed": self.tickers_failed,
            "seen_news_ids": self.seen_news_ids,
            "general_seen_ids": self.general_seen_ids,
        }

    @staticmethod
    def from_dict(data):
        state = MonitorState()
        state.last_check = data.get("last_check", "")
        state.tickers_processed = data.get("tickers_processed", 0)
        state.tickers_succeeded = data.get("tickers_succeeded", 0)
        state.tickers_failed = data.get("tickers_failed", [])
        state.seen_news_ids = data.get("seen_news_ids", {})
        state.general_seen_ids = data.get("general_seen_ids", [])
        return state


class StateManager:
    def __init__(self, state_file=STATE_FILE):
        self.state_file = state_file

    def load(self):
        if not self.state_file.exists():
            logger.info(f"State file not found: {self.state_file}, creating fresh state")
            return MonitorState()
        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.info(f"Loaded state from {self.state_file}")
            return MonitorState.from_dict(data)
        except json.JSONDecodeError:
            logger.warning("State file corrupted, creating fresh state")
            return MonitorState()
        except Exception as e:
            logger.error(f"Error loading state: {e}, creating fresh state")
            return MonitorState()

    def save(self, state):
        try:
            temp_file = self.state_file.with_suffix(".tmp")
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(state.to_dict(), f, indent=2)
            temp_file.replace(self.state_file)
        except Exception as e:
            logger.error(f"Error saving state: {e}")


class AsyncFinnhubClient:
    def __init__(self, api_key, semaphore):
        self.api_key = api_key
        self.semaphore = semaphore
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _get_with_retry(self, url, params, context_label):
        for attempt in range(RETRY_MAX_ATTEMPTS):
            try:
                async with self.semaphore:
                    async with self.session.get(
                        url, params=params, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                    ) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 429:
                            delay = RETRY_BASE_DELAY * (2 ** attempt)
                            logger.warning(f"Rate limited for {context_label}, retrying in {delay}s")
                            await asyncio.sleep(delay)
                            continue
                        elif response.status == 401:
                            raise ValueError("Invalid Finnhub API key")
                        else:
                            raise Exception(f"HTTP {response.status}")
            except asyncio.TimeoutError:
                if attempt == RETRY_MAX_ATTEMPTS - 1:
                    raise
                await asyncio.sleep(RETRY_BASE_DELAY * (2 ** attempt))
            except aiohttp.ClientError:
                if attempt == RETRY_MAX_ATTEMPTS - 1:
                    raise
                await asyncio.sleep(RETRY_BASE_DELAY * (2 ** attempt))
        raise Exception(f"Failed to fetch {context_label} after {RETRY_MAX_ATTEMPTS} attempts")

    async def fetch_company_news(self, symbol, from_date, to_date):
        url = f"{FINNHUB_BASE_URL}/company-news"
        params = {"symbol": symbol, "from": from_date, "to": to_date, "token": self.api_key}
        data = await self._get_with_retry(url, params, symbol)
        return [
            NewsArticle(
                article_id=str(a.get("id", "")),
                headline=a.get("headline", ""),
                source=a.get("source", ""),
                datetime_=a.get("datetime", 0),
                url=a.get("url", ""),
            )
            for a in data
        ]

    async def fetch_general_news(self, category="general"):
        url = f"{FINNHUB_BASE_URL}/news"
        params = {"category": category, "token": self.api_key}
        data = await self._get_with_retry(url, params, f"general/{category}")
        return [
            NewsArticle(
                article_id=str(a.get("id", "")),
                headline=a.get("headline", ""),
                source=a.get("source", ""),
                datetime_=a.get("datetime", 0),
                url=a.get("url", ""),
            )
            for a in data
        ]


class TelegramNotifier:
    """Sends notifications to Telegram, serialized + throttled to avoid 429s."""

    def __init__(self, bot_token, default_chat_id, session):
        self.bot_token = bot_token
        self.default_chat_id = default_chat_id
        self.session = session
        self._lock = asyncio.Lock()
        self._last_send_ts = 0.0

    async def send_message(self, text, chat_id=None):
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id or self.default_chat_id,
            "text": text,
            "parse_mode": "HTML",
        }

        for attempt in range(TELEGRAM_MAX_RETRIES):
            async with self._lock:
                elapsed = time.monotonic() - self._last_send_ts
                if elapsed < TELEGRAM_MIN_INTERVAL:
                    await asyncio.sleep(TELEGRAM_MIN_INTERVAL - elapsed)

                try:
                    async with self.session.post(
                        url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
                    ) as response:
                        self._last_send_ts = time.monotonic()

                        if response.status == 200:
                            return True

                        if response.status == 429:
                            try:
                                body = await response.json()
                                retry_after = body.get("parameters", {}).get("retry_after", 2)
                            except Exception:
                                retry_after = 2
                            logger.warning(
                                f"Telegram 429, waiting {retry_after}s "
                                f"(attempt {attempt + 1}/{TELEGRAM_MAX_RETRIES})"
                            )
                            await asyncio.sleep(retry_after)
                            continue

                        logger.error(f"Telegram API error: HTTP {response.status}")
                        return False

                except Exception as e:
                    logger.error(f"Error sending Telegram message: {e}")
                    return False

        logger.error("Telegram send failed after retries (persistent 429)")
        return False


class NewsMonitor:
    def __init__(self, finnhub_client, telegram_notifier, semaphore):
        self.finnhub = finnhub_client
        self.telegram = telegram_notifier
        self.semaphore = semaphore

    async def run(self, tickers, state):
        logger.info(f"Starting news monitoring cycle for {len(tickers)} tickers")
        now = datetime.now(timezone.utc)

        state.last_check = now.isoformat()
        state.tickers_processed = len(tickers)
        state.tickers_succeeded = 0
        state.tickers_failed = []

        from_date = now.strftime("%Y-%m-%d")
        to_date = (now + timedelta(days=1)).strftime("%Y-%m-%d")

        tasks = [self._process_ticker(t, from_date, to_date, state) for t in tickers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Task failed: {result}")
            elif result is True:
                state.tickers_succeeded += 1

        logger.info(
            f"Monitoring cycle complete: {state.tickers_succeeded}/{state.tickers_processed} tickers succeeded"
        )
        if state.tickers_failed:
            logger.warning(f"Failed tickers: {', '.join(state.tickers_failed)}")

    async def _process_ticker(self, ticker, from_date, to_date, state):
        try:
            if ticker not in state.seen_news_ids:
                state.seen_news_ids[ticker] = []

            articles = await self.finnhub.fetch_company_news(ticker, from_date, to_date)
            new_articles = [a for a in articles if a.id not in state.seen_news_ids[ticker]]

            for article in new_articles:
                published = datetime.fromtimestamp(article.datetime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                message = (
                    f"<b>{ticker}</b>: {html.escape(article.headline)}\n"
                    f"Източник: {html.escape(article.source)} · {published} UTC\n"
                    f"{article.url}"
                )
                sent = await self.telegram.send_message(message)
                if sent:
                    state.seen_news_ids[ticker].append(article.id)

            return True
        except Exception as e:
            logger.error(f"Error processing {ticker}: {e}")
            state.tickers_failed.append(ticker)
            return False

    async def run_general_news(self, keywords, macro_chat_id, state):
        logger.info(f"Starting general/macro news scan ({len(keywords)} keywords)")
        try:
            articles = await self.finnhub.fetch_general_news(category="general")
        except Exception as e:
            logger.error(f"Error fetching general news: {e}")
            return

        keywords_lower = [k.lower() for k in keywords]
        matched = 0

        for article in articles:
            if article.id in state.general_seen_ids:
                continue

            haystack = article.headline.lower()
            if not any(kw in haystack for kw in keywords_lower):
                continue

            published = datetime.fromtimestamp(article.datetime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            message = (
                f"\U0001F310 {html.escape(article.headline)}\n"
                f"Източник: {html.escape(article.source)} · {published} UTC\n"
                f"{article.url}"
            )
            sent = await self.telegram.send_message(message, chat_id=macro_chat_id)
            if sent:
                state.general_seen_ids.append(article.id)
                matched += 1

        state.general_seen_ids = state.general_seen_ids[-2000:]
        logger.info(f"General/macro scan complete: {matched} new matches sent")


def load_lines(file_path):
    if not file_path.exists():
        return []
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def load_tickers(tickers_file=TICKERS_FILE):
    if not tickers_file.exists():
        logger.error(f"Tickers file not found: {tickers_file}")
        raise FileNotFoundError(f"Missing {tickers_file}")
    tickers = [line.strip().upper() for line in load_lines(tickers_file)]
    logger.info(f"Loaded {len(tickers)} tickers from {tickers_file}")
    return tickers


def load_keywords(keywords_file=KEYWORDS_FILE):
    keywords = load_lines(keywords_file)
    logger.info(f"Loaded {len(keywords)} keywords from {keywords_file}")
    return keywords


async def main():
    if not FINNHUB_API_KEY:
        logger.error("FINNHUB_API_KEY environment variable not set")
        sys.exit(1)
    if not TELEGRAM_BOT_TOKEN:
        logger.error("NEWS_5MIN_TELEGRAM_BOT_TOKEN environment variable not set")
        sys.exit(1)
    if not TELEGRAM_CHAT_ID:
        logger.error("NEWS_5MIN_TELEGRAM_CHAT_ID environment variable not set")
        sys.exit(1)

    try:
        tickers = load_tickers()
    except FileNotFoundError:
        tickers = []

    keywords = load_keywords()
    if keywords and not MACRO_CHAT_ID:
        logger.warning("NEWS_5MIN_TELEGRAM_MACRO_CHAT_ID not set - skipping general/macro scan")

    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    state_manager = StateManager()
    state = state_manager.load()

    async with aiohttp.ClientSession() as session:
        async with AsyncFinnhubClient(FINNHUB_API_KEY, semaphore) as finnhub_client:
            telegram_notifier = TelegramNotifier(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, session)
            monitor = NewsMonitor(finnhub_client, telegram_notifier, semaphore)

            try:
                if tickers:
                    await monitor.run(tickers, state)
                else:
                    logger.warning("No tickers to monitor")

                if keywords and MACRO_CHAT_ID:
                    await monitor.run_general_news(keywords, MACRO_CHAT_ID, state)

                logger.info("News monitoring cycle completed successfully")
            except Exception as e:
                logger.error(f"Fatal error during monitoring: {e}", exc_info=True)
                state_manager.save(state)
                sys.exit(1)

    state_manager.save(state)


if __name__ == "__main__":
    asyncio.run(main())

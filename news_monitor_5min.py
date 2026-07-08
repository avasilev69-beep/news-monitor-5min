#!/usr/bin/env python3
"""
News Monitor 5min - Real-time financial news monitoring.

Monitors 100+ financial instruments via Finnhub API and sends
new news alerts to Telegram every 5 minutes with async request handling.

Architecture:
- Async/await (asyncio + aiohttp)
- Semaphore-based rate limiting (10 concurrent requests)
- State persistence (JSON tracking of seen articles)
- Structured logging (production-ready)
- Error handling with exponential backoff
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import aiohttp

# Configuration
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("NEWS_5MIN_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("NEWS_5MIN_TELEGRAM_CHAT_ID")

FINNHUB_BASE_URL = "https://finnhub.io/api/v1"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

STATE_FILE = Path("state_5min.json")
TICKERS_FILE = Path("tickers.txt")

# Rate limiting and timeouts
SEMAPHORE_LIMIT = 10
REQUEST_TIMEOUT = 10
RETRY_MAX_ATTEMPTS = 3
RETRY_BASE_DELAY = 1

# Logging
LOG_FORMAT = "%(asctime)s | %(levelname)-5s | %(name)s | %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


# ============================================================================
# Type Definitions and Models
# ============================================================================

class NewsArticle:
    """Represents a single news article from Finnhub."""

    def __init__(self, article_id: str, headline: str, source: str, datetime_: int):
        self.id = article_id
        self.headline = headline
        self.source = source
        self.datetime = datetime_

    def __repr__(self) -> str:
        return f"NewsArticle(id={self.id}, headline={self.headline[:50]}...)"


class MonitorState:
    """Persistent state tracking for news monitoring."""

    def __init__(self):
        self.last_check: str = ""
        self.tickers_processed: int = 0
        self.tickers_succeeded: int = 0
        self.tickers_failed: list[str] = []
        self.seen_news_ids: dict[str, list[str]] = {}

    def to_dict(self) -> dict:
        """Serialize state to dictionary."""
        return {
            "last_check": self.last_check,
            "tickers_processed": self.tickers_processed,
            "tickers_succeeded": self.tickers_succeeded,
            "tickers_failed": self.tickers_failed,
            "seen_news_ids": self.seen_news_ids,
        }

    @staticmethod
    def from_dict(data: dict) -> "MonitorState":
        """Deserialize state from dictionary."""
        state = MonitorState()
        state.last_check = data.get("last_check", "")
        state.tickers_processed = data.get("tickers_processed", 0)
        state.tickers_succeeded = data.get("tickers_succeeded", 0)
        state.tickers_failed = data.get("tickers_failed", [])
        state.seen_news_ids = data.get("seen_news_ids", {})
        return state


# ============================================================================
# State Management
# ============================================================================

class StateManager:
    """Handles persistent state (JSON) with validation and recovery."""

    def __init__(self, state_file: Path = STATE_FILE):
        self.state_file = state_file

    def load(self) -> MonitorState:
        """Load state from file, handle corruption gracefully."""
        if not self.state_file.exists():
            logger.info(f"State file not found: {self.state_file}, creating fresh state")
            return MonitorState()

        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.info(f"Loaded state from {self.state_file}")
            return MonitorState.from_dict(data)
        except json.JSONDecodeError:
            logger.warning(
                f"State file corrupted or invalid JSON: {self.state_file}, creating fresh state"
            )
            return MonitorState()
        except Exception as e:
            logger.error(f"Error loading state: {e}, creating fresh state")
            return MonitorState()

    def save(self, state: MonitorState) -> None:
        """Save state to file atomically."""
        try:
            # Write to temp file first, then rename (atomic operation)
            temp_file = self.state_file.with_suffix(".tmp")
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(state.to_dict(), f, indent=2)
            temp_file.replace(self.state_file)
            logger.debug(f"State saved to {self.state_file}")
        except Exception as e:
            logger.error(f"Error saving state: {e}")


# ============================================================================
# Finnhub API Client (Async)
# ============================================================================

class AsyncFinnhubClient:
    """Async HTTP client for Finnhub API with rate limiting and retry logic."""

    def __init__(self, api_key: str, semaphore: asyncio.Semaphore):
        self.api_key = api_key
        self.semaphore = semaphore
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        """Context manager entry."""
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self.session:
            await self.session.close()

    async def fetch_company_news(
        self, symbol: str, from_date: str, to_date: str
    ) -> list[NewsArticle]:
        """
        Fetch company news for a symbol within date range.

        Args:
            symbol: Stock ticker symbol
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)

        Returns:
            List of NewsArticle objects

        Raises:
            Exception: If API call fails after retries
        """
        url = f"{FINNHUB_BASE_URL}/company-news"
        params = {
            "symbol": symbol,
            "from": from_date,
            "to": to_date,
            "token": self.api_key,
        }

        for attempt in range(RETRY_MAX_ATTEMPTS):
            try:
                async with self.semaphore:
                    async with self.session.get(
                        url, params=params, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            articles = [
                                NewsArticle(
                                    article_id=str(article.get("id", "")),
                                    headline=article.get("headline", ""),
                                    source=article.get("source", ""),
                                    datetime_=article.get("datetime", 0),
                                )
                                for article in data
                            ]
                            return articles

                        elif response.status == 429:
                            # Rate limited - implement exponential backoff
                            delay = RETRY_BASE_DELAY * (2 ** attempt)
                            logger.warning(
                                f"Rate limited for {symbol}, attempt {attempt + 1}/{RETRY_MAX_ATTEMPTS}, "
                                f"retrying in {delay}s"
                            )
                            await asyncio.sleep(delay)
                            continue

                        elif response.status == 401:
                            logger.error(f"Unauthorized: Invalid API key")
                            raise ValueError("Invalid Finnhub API key")

                        else:
                            logger.error(
                                f"API error for {symbol}: HTTP {response.status}"
                            )
                            raise Exception(f"HTTP {response.status}")

            except asyncio.TimeoutError:
                logger.warning(
                    f"Timeout for {symbol}, attempt {attempt + 1}/{RETRY_MAX_ATTEMPTS}"
                )
                if attempt == RETRY_MAX_ATTEMPTS - 1:
                    raise
                await asyncio.sleep(RETRY_BASE_DELAY * (2 ** attempt))

            except aiohttp.ClientError as e:
                logger.warning(
                    f"Connection error for {symbol}, attempt {attempt + 1}/{RETRY_MAX_ATTEMPTS}: {e}"
                )
                if attempt == RETRY_MAX_ATTEMPTS - 1:
                    raise
                await asyncio.sleep(RETRY_BASE_DELAY * (2 ** attempt))

        raise Exception(f"Failed to fetch news for {symbol} after {RETRY_MAX_ATTEMPTS} attempts")


# ============================================================================
# Telegram Notifier
# ============================================================================

class TelegramNotifier:
    """Sends notifications to Telegram channel."""

    def __init__(self, bot_token: str, chat_id: str, session: aiohttp.ClientSession):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.session = session

    async def send_message(self, text: str) -> bool:
        """
        Send message to Telegram channel.

        Args:
            text: Message text

        Returns:
            True if successful, False otherwise
        """
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
        }

        try:
            async with self.session.post(
                url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status == 200:
                    return True
                else:
                    logger.error(f"Telegram API error: HTTP {response.status}")
                    return False
        except Exception as e:
            logger.error(f"Error sending Telegram message: {e}")
            return False


# ============================================================================
# News Monitor (Orchestrator)
# ============================================================================

class NewsMonitor:
    """Main orchestrator for news monitoring workflow."""

    def __init__(
        self,
        finnhub_client: AsyncFinnhubClient,
        telegram_notifier: TelegramNotifier,
        state_manager: StateManager,
        semaphore: asyncio.Semaphore,
    ):
        self.finnhub = finnhub_client
        self.telegram = telegram_notifier
        self.state_manager = state_manager
        self.semaphore = semaphore

    async def run(self, tickers: list[str]) -> None:
        """
        Execute news monitoring cycle for all tickers.

        Args:
            tickers: List of stock symbols to monitor
        """
        logger.info(f"Starting news monitoring cycle for {len(tickers)} tickers")

        # Load previous state
        state = self.state_manager.load()
        now = datetime.now(timezone.utc)
        five_min_ago = now - timedelta(minutes=5)

        # Update state metadata
        state.last_check = now.isoformat()
        state.tickers_processed = len(tickers)
        state.tickers_succeeded = 0
        state.tickers_failed = []

        # Format date strings for Finnhub API (YYYY-MM-DD)
        from_date = now.strftime("%Y-%m-%d")
        to_date = (now + timedelta(days=1)).strftime("%Y-%m-%d")

        # Create tasks for all tickers
        tasks = [
            self._process_ticker(ticker, from_date, to_date, state)
            for ticker in tickers
        ]

        # Execute all tasks concurrently (semaphore limits concurrency)
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Count successes and failures
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Task failed: {result}")
            elif result is True:
                state.tickers_succeeded += 1

        # Save updated state
        logger.info(
            f"Monitoring cycle complete: {state.tickers_succeeded}/{state.tickers_processed} tickers succeeded"
        )
        if state.tickers_failed:
            logger.warning(f"Failed tickers: {', '.join(state.tickers_failed)}")

        self.state_manager.save(state)

    async def _process_ticker(
        self, ticker: str, from_date: str, to_date: str, state: MonitorState
    ) -> bool:
        """
        Process a single ticker: fetch news, filter, notify.

        Args:
            ticker: Stock symbol
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            state: Current monitoring state

        Returns:
            True if successful, False otherwise
        """
        try:
            # Initialize ticker's seen_news_ids if not present
            if ticker not in state.seen_news_ids:
                state.seen_news_ids[ticker] = []

            # Fetch news from Finnhub
            articles = await self.finnhub.fetch_company_news(ticker, from_date, to_date)

            # Filter new articles (not seen before)
            new_articles = [
                article
                for article in articles
                if article.id not in state.seen_news_ids[ticker]
            ]

            # Send notifications for new articles
            for article in new_articles:
                message = f"<b>{ticker}</b>: {article.headline}"
                sent = await self.telegram.send_message(message)

                if sent:
                    # Only mark as seen if notification was successful
                    state.seen_news_ids[ticker].append(article.id)
                    logger.debug(f"Notified: {ticker} - {article.headline[:50]}...")

            return True

        except Exception as e:
            logger.error(f"Error processing {ticker}: {e}")
            state.tickers_failed.append(ticker)
            return False


# ============================================================================
# Ticker Management
# ============================================================================

def load_tickers(tickers_file: Path = TICKERS_FILE) -> list[str]:
    """
    Load tickers from file (one per line).

    Args:
        tickers_file: Path to tickers.txt

    Returns:
        List of ticker symbols

    Raises:
        FileNotFoundError: If tickers file not found
    """
    if not tickers_file.exists():
        logger.error(f"Tickers file not found: {tickers_file}")
        raise FileNotFoundError(f"Missing {tickers_file}")

    try:
        with open(tickers_file, "r", encoding="utf-8") as f:
            tickers = [line.strip().upper() for line in f if line.strip()]
        logger.info(f"Loaded {len(tickers)} tickers from {tickers_file}")
        return tickers
    except Exception as e:
        logger.error(f"Error loading tickers: {e}")
        raise


# ============================================================================
# Main Entry Point
# ============================================================================

async def main() -> None:
    """Main entry point for news monitoring."""
    # Validate environment variables
    if not FINNHUB_API_KEY:
        logger.error("FINNHUB_API_KEY environment variable not set")
        sys.exit(1)

    if not TELEGRAM_BOT_TOKEN:
        logger.error("NEWS_5MIN_TELEGRAM_BOT_TOKEN environment variable not set")
        sys.exit(1)

    if not TELEGRAM_CHAT_ID:
        logger.error("NEWS_5MIN_TELEGRAM_CHAT_ID environment variable not set")
        sys.exit(1)

    # Load tickers
    try:
        tickers = load_tickers()
    except FileNotFoundError:
        logger.warning(
            f"Tickers file not found. Falling back to empty list. "
            f"Create {TICKERS_FILE} with one ticker per line."
        )
        tickers = []

    if not tickers:
        logger.warning("No tickers to monitor, exiting")
        sys.exit(0)

    # Create semaphore for rate limiting
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

    # Initialize components
    state_manager = StateManager()

    async with aiohttp.ClientSession() as session:
        async with AsyncFinnhubClient(FINNHUB_API_KEY, semaphore) as finnhub_client:
            telegram_notifier = TelegramNotifier(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, session)
            monitor = NewsMonitor(finnhub_client, telegram_notifier, state_manager, semaphore)

            # Run monitoring cycle
            try:
                await monitor.run(tickers)
                logger.info("News monitoring cycle completed successfully")
            except Exception as e:
                logger.error(f"Fatal error during monitoring: {e}", exc_info=True)
                sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

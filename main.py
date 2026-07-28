import json
import logging
import re
import threading
import time
from collections import deque
from typing import Any, List, Optional, Tuple

import requests
import websocket

from config import Config
from models import Market


log = logging.getLogger(__name__)


def parse_array(value):
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            result = json.loads(value)
            return result if isinstance(result, list) else []
        except json.JSONDecodeError:
            return []
    return []


class ChainlinkPriceFeed:
    SYMBOLS = {
        "BTC": "btc/usd",
        "ETH": "eth/usd",
    }

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.ws: Optional[websocket.WebSocketApp] = None
        self.history = {
            symbol: deque(maxlen=10000)
            for symbol in self.SYMBOLS
        }

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(
            target=self._run,
            name="polymarket-chainlink-rtds",
            daemon=True,
        )
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        ws = self.ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    def _subscription(self) -> str:
        subscriptions = []
        for feed_symbol in self.SYMBOLS.values():
            subscriptions.append({
                "topic": "crypto_prices_chainlink",
                "type": "*",
                "filters": json.dumps(
                    {"symbol": feed_symbol},
                    separators=(",", ":"),
                ),
            })
        return json.dumps({
            "action": "subscribe",
            "subscriptions": subscriptions,
        })

    def _on_open(self, ws) -> None:
        ws.send(self._subscription())
        log.info(
            "Polymarket RTDS Chainlink подключён: BTC/USD, ETH/USD"
        )

        def heartbeat():
            while (
                not self.stop_event.wait(5)
                and self.ws is ws
            ):
                try:
                    ws.send("PING")
                except Exception:
                    return

        threading.Thread(
            target=heartbeat,
            name="rtds-heartbeat",
            daemon=True,
        ).start()

    def _on_message(self, _ws, message: str) -> None:
        if message in {"PONG", "PING"}:
            return

        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return

        if isinstance(payload, list):
            for item in payload:
                self._consume(item)
        else:
            self._consume(payload)

    def _consume(self, message: Any) -> None:
        if not isinstance(message, dict):
            return

        topic = str(message.get("topic") or "")
        if topic not in {
            "crypto_prices_chainlink",
            "prices.crypto.chainlink",
        }:
            return

        payload = message.get("payload") or {}
        if not isinstance(payload, dict):
            return

        feed_symbol = str(
            payload.get("symbol") or ""
        ).lower()

        symbol = next(
            (
                coin
                for coin, value in self.SYMBOLS.items()
                if value == feed_symbol
            ),
            None,
        )
        if symbol is None:
            return

        try:
            value = float(payload["value"])
            timestamp_ms = float(
                payload.get("timestamp")
                or message.get("timestamp")
                or time.time() * 1000
            )
        except (KeyError, TypeError, ValueError):
            return

        timestamp_s = (
            timestamp_ms / 1000.0
            if timestamp_ms > 10_000_000_000
            else timestamp_ms
        )

        with self.lock:
            self.history[symbol].append(
                (timestamp_s, value)
            )

    def _on_error(self, _ws, error) -> None:
        log.warning("RTDS Chainlink error: %s", error)

    def _on_close(self, _ws, code, reason) -> None:
        log.warning(
            "RTDS Chainlink закрыт: code=%s reason=%s",
            code,
            reason,
        )

    def _run(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.ws = websocket.WebSocketApp(
                    self.cfg.rtds_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self.ws.run_forever(
                    ping_interval=0,
                    ping_timeout=None,
                )
            except Exception:
                log.exception("Ошибка цикла RTDS Chainlink")

            self.ws = None
            if not self.stop_event.wait(3):
                log.info(
                    "Повторное подключение RTDS Chainlink через 3 сек."
                )

    def latest(
        self,
        symbol: str,
        max_age: Optional[float] = None,
    ) -> Optional[Tuple[float, float]]:
        with self.lock:
            items = list(
                self.history.get(symbol.upper(), ())
            )

        if not items:
            return None

        timestamp_s, value = items[-1]
        allowed_age = (
            self.cfg.chainlink_max_tick_age_seconds
            if max_age is None
            else max_age
        )
        if time.time() - timestamp_s > allowed_age:
            return None
        return timestamp_s, value

    def nearest(
        self,
        symbol: str,
        boundary_ts: float,
        tolerance: Optional[float] = None,
        prefer_after: bool = False,
    ) -> Optional[Tuple[float, float]]:
        with self.lock:
            items = list(
                self.history.get(symbol.upper(), ())
            )

        if not items:
            return None

        tolerance_s = (
            self.cfg.chainlink_boundary_tolerance_seconds
            if tolerance is None
            else tolerance
        )

        candidates = [
            item
            for item in items
            if abs(item[0] - boundary_ts) <= tolerance_s
        ]
        if not candidates:
            return None

        if prefer_after:
            after = [
                item
                for item in candidates
                if item[0] >= boundary_ts
            ]
            if after:
                return min(
                    after,
                    key=lambda item: item[0] - boundary_ts,
                )

        return min(
            candidates,
            key=lambda item: abs(item[0] - boundary_ts),
        )


class Polymarket:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.http = requests.Session()
        self.http.headers.update({
            "User-Agent": "polymarket-demo-bot/5.0"
        })
        self.market_cache = {}
        self.chainlink = ChainlinkPriceFeed(cfg)

    def start(self) -> None:
        self.chainlink.start()

    def stop(self) -> None:
        self.chainlink.stop()

    @staticmethod
    def current_start(now: Optional[int] = None) -> int:
        """Округление времени вниз до ближайшего 5-минутного интервала (300 сек)"""
        now = int(now or time.time())
        return (now // 300) * 300

    def get_5m_markets(
        self, 
        symbols: List[str] = ["BTC", "ETH"], 
        offset_intervals: int = 0
    ) -> List[Market]:
        """
        Поиск и получение активных/предстоящих 5-минутных рынков.
        :param symbols: список монет (например, ['BTC', 'ETH'])
        :param offset_intervals: 0 - текущий рынок, 1 - следующий 5m рынок, -1 - предыдущий
        """
        start_ts = self.current_start() + (offset_intervals * 300)
        markets = []
        for symbol in symbols:
            market = self.get_market(symbol=symbol, start_ts=start_ts)
            if market:
                markets.append(market)
        return markets

    def get_market(
        self,
        symbol: str,
        start_ts: int,
    ) -> Optional[Market]:
        # Генерация слага 5-минутного рынка по стандарту Polymarket
        slug = f"{symbol.lower()}-updown-5m-{start_ts}"

        cached = self.market_cache.get(slug)
        if cached:
            return cached

        log.info("[%s] Ищу рынок: %s", symbol, slug)
        response = self.http.get(
            f"{self.cfg.gamma_url}/markets",
            params={"slug": slug, "limit": 10},
            timeout=15,
        )
        response.raise_for_status()
        items = response.json()

        if not isinstance(items, list) or not items:
            log.warning("[%s] Рынок не найден: %s", symbol, slug)
            return None

        item = next(
            (x for x in items if x.get("slug") == slug),
            None,
        )
        if not item:
            log.warning("[%s] Gamma вернула другой slug", symbol)
            return None

        outcomes = [
            str(x).strip().lower()
            for x in parse_array(item.get("outcomes"))
        ]
        token_ids = [
            str(x)
            for x in parse_array(item.get("clobTokenIds"))
        ]
        mapping = dict(zip(outcomes, token_ids))

        if "up" not in mapping or "down" not in mapping:
            raise RuntimeError(
                f"{slug}: нет токенов Up/Down; "
                f"outcomes={outcomes}"
            )

        market = Market(
            symbol=symbol,
            slug=slug,
            start_ts=start_ts,
            end_ts=start_ts + 300,
            condition_id=str(item.get("conditionId", "")),
            up_token_id=mapping["up"],
            down_token_id=mapping["down"],
        )
        self.market_cache[slug] = market
        return market

    def get_book(self, token_id: str) -> dict:
        response = self.http.get(
            f"{self.cfg.clob_url}/book",
            params={"token_id": token_id},
            timeout=8,
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _best(levels, side: str) -> Optional[float]:
        values = []
        for level in levels or []:
            try:
                price = float(level["price"])
                size = float(level["size"])
                if size > 0:
                    values.append(price)
            except (KeyError, TypeError, ValueError):
                continue
        if not values:
            return None
        return max(values) if side == "bid" else min(values)

    def bid_ask(
        self,
        token_id: str,
    ) -> Tuple[Optional[float], Optional[float]]:
        book = self.get_book(token_id)
        return (
            self._best(book.get("bids"), "bid"),
            self._best(book.get("asks"), "ask"),
        )

    def estimate_buy(
        self,
        token_id: str,
        amount: float,
    ) -> Tuple[float, float]:
        book = self.get_book(token_id)
        asks = []
        for level in book.get("asks", []):
            try:
                asks.append((
                    float(level["price"]),
                    float(level["size"]),
                ))
            except (KeyError, TypeError, ValueError):
                continue
        asks.sort()

        remaining = amount
        spent = 0.0
        shares = 0.0

        for price, available in asks:
            if remaining <= 0.0001:
                break
            cost = min(remaining, price * available)
            spent += cost
            shares += cost / price
            remaining -= cost

        if shares <= 0:
            raise RuntimeError("В стакане нет ask")
        if remaining > 0.01:
            raise RuntimeError(
                f"Недостаточно ликвидности: "
                f"найдено ${spent:.2f} из ${amount:.2f}"
            )
        return spent / shares, shares

    def chainlink_target(
        self,
        symbol: str,
        start_ts: float,
    ) -> Optional[Tuple[float, float]]:
        return self.chainlink.nearest(
            symbol=symbol,
            boundary_ts=start_ts,
            prefer_after=True,
        )

    def chainlink_latest(
        self,
        symbol: str,
    ) -> Optional[Tuple[float, float]]:
        return self.chainlink.latest(symbol)

    def chainlink_final(
        self,
        symbol: str,
        end_ts: float,
    ) -> Optional[Tuple[float, float]]:
        return self.chainlink.nearest(
            symbol=symbol,
            boundary_ts=end_ts,
            prefer_after=True,
        )

    def chainlink_result(
        self,
        symbol: str,
        start_ts: float,
        end_ts: Optional[float] = None,
        use_latest: bool = False,
    ) -> Optional[Tuple[str, float, float, float]]:
        target_item = self.chainlink_target(
            symbol,
            start_ts,
        )
        if not target_item:
            return None

        price_item = (
            self.chainlink_latest(symbol)
            if use_latest
            else self.chainlink_final(
                symbol,
                end_ts if end_ts is not None else time.time(),
            )
        )
        if not price_item:
            return None

        _target_ts, target_price = target_item
        price_ts, price = price_item
        result = "Up" if price >= target_price else "Down"
        return result, target_price, price, price_ts

    def resolved_result(self, slug: str) -> Optional[str]:
        response = self.http.get(
            f"{self.cfg.gamma_url}/markets",
            params={"slug": slug, "limit": 5},
            timeout=15,
        )
        response.raise_for_status()
        items = response.json()
        item = next(
            (x for x in items if x.get("slug") == slug),
            None,
        )
        if not item:
            return None

        outcomes = parse_array(item.get("outcomes"))
        prices = parse_array(item.get("outcomePrices"))
        if len(outcomes) != len(prices) or not prices:
            return None

        try:
            nums = [float(x) for x in prices]
        except (TypeError, ValueError):
            return None

        best = max(nums)
        if best < 0.99:
            return None
        winner = str(
            outcomes[nums.index(best)]
        ).strip().title()
        return winner if winner in {"Up", "Down"} else None
    

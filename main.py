import os
import json
import time
import asyncio
import logging
from datetime import datetime
from collections import deque

import aiohttp
import websockets
from aiohttp import web, ClientSession
from py_clob_client.client import ClobClient
from telebot.async_telebot import AsyncTeleBot
from telebot import types
from dotenv import load_dotenv


# ============================================================
# ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ
# ============================================================

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
ALLOWED_CHAT_ID_ENV = os.getenv("TELEGRAM_CHAT_ID")
INITIAL_BALANCE = float(os.getenv("INITIAL_BALANCE", "1000.0"))
PORT = int(os.getenv("PORT", "8080"))

ALLOWED_CHAT_ID = (
    int(ALLOWED_CHAT_ID_ENV)
    if ALLOWED_CHAT_ID_ENV and ALLOWED_CHAT_ID_ENV.lstrip("-").isdigit()
    else None
)


# ============================================================
# ЛОГИРОВАНИЕ
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

if not TELEGRAM_TOKEN:
    logging.critical(
        "❌ ОШИБКА: TELEGRAM_TOKEN или TELEGRAM_BOT_TOKEN не найден!"
    )
    raise RuntimeError(
        "Не задан TELEGRAM_TOKEN или TELEGRAM_BOT_TOKEN"
    )


# ============================================================
# КЛИЕНТЫ
# ============================================================

bot = AsyncTeleBot(TELEGRAM_TOKEN)

clob_client = ClobClient(
    host="https://clob.polymarket.com",
    chain_id=137
)


# ============================================================
# НАСТРОЙКИ 5-МИНУТНОЙ ГИБРИДНОЙ СТРАТЕГИИ
# ============================================================

TARGET_ASSETS = [
    "BTC",
    "ETH",
    "SOL",
    "DOGE",
    "BNB",
    "XRP",
    "HYPE"
]

MOMENTUM_THRESHOLD = 0.20
LOOKBACK_SECONDS = 5
MAKER_SPREAD_OFFSET = 0.01
TAKER_COMMISSION = 0.03
MARKET_REFRESH_INTERVAL = 30
ORDERBOOK_REFRESH_INTERVAL = 2
MARKET_SLOT_OFFSETS = (0, 1, -1, 2, -2)


# ============================================================
# SLUG-ПРЕФИКСЫ ПЯТИМИНУТНЫХ РЫНКОВ
# ============================================================

POLYMARKET_5M_SLUG_PREFIXES = {
    "BTC": "btc",
    "ETH": "eth",
    "SOL": "sol",
    "DOGE": "doge",
    "BNB": "bnb",
    "XRP": "xrp",
    "HYPE": "hype"
}


# ============================================================
# ГЛОБАЛЬНОЕ СОСТОЯНИЕ
# ============================================================

binance_prices = {
    asset: 0.0
    for asset in TARGET_ASSETS
}

binance_histories = {
    asset: deque()
    for asset in TARGET_ASSETS
}

last_signal_times = {
    asset: 0.0
    for asset in TARGET_ASSETS
}


# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def parse_json_list(value):
    """Преобразует список или JSON-строку со списком в обычный список."""
    if isinstance(value, list):
        return value

    if isinstance(value, tuple):
        return list(value)

    if isinstance(value, str):
        try:
            parsed = json.loads(value)

            if isinstance(parsed, list):
                return parsed
        except (json.JSONDecodeError, TypeError):
            return []

    return []


def parse_boolean(value, default=False):
    """Преобразует значение API в логический тип."""
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        return value.lower() in {
            "true",
            "1",
            "yes",
            "on"
        }

    if value is None:
        return default

    return bool(value)


def get_5m_slot_timestamp(offset=0):
    """Возвращает UNIX-время начала пятиминутного интервала."""
    interval_seconds = 5 * 60
    current_timestamp = int(time.time())
    current_slot = (current_timestamp // interval_seconds) * interval_seconds

    return current_slot + offset * interval_seconds


def build_5m_slug(asset, slot_timestamp):
    """Создаёт slug пятиминутного рынка для указанного актива."""
    prefix = POLYMARKET_5M_SLUG_PREFIXES.get(asset)

    if not prefix:
        return None

    return f"{prefix}-updown-5m-{slot_timestamp}"


def get_outcome_token_map(market):
    """Сопоставляет исходы UP/DOWN с CLOB token ID."""
    token_ids = parse_json_list(
        market.get("clobTokenIds")
    )

    outcomes = parse_json_list(
        market.get("outcomes")
    )

    if len(token_ids) < 2:
        return {}

    result = {}

    if len(outcomes) == len(token_ids):
        for outcome, token_id in zip(outcomes, token_ids):
            normalized_outcome = str(outcome).strip().upper()
            result[normalized_outcome] = str(token_id)

    if "YES" in result and "UP" not in result:
        result["UP"] = result["YES"]

    if "NO" in result and "DOWN" not in result:
        result["DOWN"] = result["NO"]

    if "UP" not in result:
        result["UP"] = str(token_ids[0])

    if "DOWN" not in result:
        result["DOWN"] = str(token_ids[1])

    return result


def get_market_end_timestamp(market):
    """Возвращает время окончания рынка в формате UNIX timestamp."""
    end_date = (
        market.get("endDate")
        or market.get("endDateIso")
        or market.get("end_date_iso")
    )

    if not end_date:
        return 0

    try:
        normalized = str(end_date).replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).timestamp()

    except (ValueError, TypeError):
        return 0


def is_market_usable(market):
    """Проверяет, подходит ли рынок для получения CLOB-стакана."""
    if not isinstance(market, dict):
        return False

    token_map = get_outcome_token_map(market)

    if not token_map.get("UP") or not token_map.get("DOWN"):
        return False

    if parse_boolean(market.get("closed"), default=False):
        return False

    if not parse_boolean(market.get("active"), default=True):
        return False

    if not parse_boolean(market.get("enableOrderBook"), default=True):
        return False

    if not parse_boolean(market.get("acceptingOrders"), default=True):
        return False

    return True


def extract_market_from_response(data):
    """Извлекает рынок из разных форматов ответа Gamma API."""
    if isinstance(data, dict):
        if data.get("clobTokenIds"):
            return data

        markets = data.get("markets")

        if isinstance(markets, list) and markets:
            return markets[0]

        data_items = data.get("data")

        if isinstance(data_items, list) and data_items:
            first_item = data_items[0]

            if isinstance(first_item, dict):
                return first_item

    if isinstance(data, list) and data:
        first_item = data[0]

        if isinstance(first_item, dict):
            if first_item.get("clobTokenIds"):
                return first_item

            markets = first_item.get("markets")

            if isinstance(markets, list) and markets:
                return markets[0]

    return None


def safe_markdown_text(value):
    """Удаляет символы, способные нарушить разметку Telegram Markdown."""
    text = str(value)

    for symbol in ["*", "_", "`", "[", "]"]:
        text = text.replace(symbol, "")

    return text


# ============================================================
# HYBRID 5M PAPER TRADER
# ============================================================

class HybridPaperTrader:
    def __init__(self, initial_balance):
        self.initial_balance = initial_balance
        self.balance = initial_balance
        self.positions = []
        self.active_limit_orders = {}
        self.trade_history = []
        self.current_mode = "MAKER"
        self.auto_trade_enabled = True
        self.markets = {}

    def cancel_all_limits(self):
        """Отменяет все моделируемые Maker-лимитные ордера."""
        count = sum(
            len(orders)
            for orders in self.active_limit_orders.values()
        )

        self.active_limit_orders.clear()
        return count

    def update_maker_orders(self, asset, best_bid, best_ask):
        """Моделирует установку Maker-ордеров внутри спреда."""
        if best_bid <= 0 or best_ask <= 0:
            return

        my_bid = round(best_bid + MAKER_SPREAD_OFFSET, 2)
        my_ask = round(best_ask - MAKER_SPREAD_OFFSET, 2)

        if my_bid < my_ask:
            self.active_limit_orders[asset] = [
                {
                    "side": "BUY_LIMIT",
                    "price": my_bid,
                    "amount": 20.0
                },
                {
                    "side": "SELL_LIMIT",
                    "price": my_ask,
                    "amount": 20.0
                }
            ]
        else:
            self.active_limit_orders.pop(asset, None)

    def execute_taker_trade(self, asset, side, reason):
        """Открывает бумажную Taker-позицию по текущей цене."""
        self.cancel_all_limits()

        market_data = self.markets.get(asset, {})

        yes_price = market_data.get("yes_price", 0.50)
        no_price = market_data.get("no_price", 0.50)

        entry_price = yes_price if side == "YES" else no_price
        entry_price = round(float(entry_price), 4)

        if entry_price <= 0 or entry_price >= 1:
            return False, "Некорректная цена в стакане"

        trade_amount = 25.0
        fee = trade_amount * TAKER_COMMISSION
        total_cost = trade_amount + fee

        if self.balance < total_cost:
            return False, "Недостаточно средств на балансе"

        shares = trade_amount / entry_price
        self.balance -= total_cost

        position = {
            "id": len(self.positions) + len(self.trade_history) + 1,
            "mode": "TAKER ⚡ 5M",
            "asset": asset,
            "question": market_data.get("question", asset)[:80],
            "side": side,
            "amount": trade_amount,
            "shares": round(shares, 4),
            "entry_price": entry_price,
            "fee": round(fee, 2),
            "reason": reason,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            "market_slug": market_data.get("slug", "")
        }

        self.positions.append(position)
        return True, position

    def get_stats(self):
        """Возвращает статистику бумажного счёта."""
        realized_pnl = sum(
            trade.get("pnl", 0)
            for trade in self.trade_history
        )

        unrealized_pnl = 0.0

        for position in self.positions:
            asset = position["asset"]
            side = position["side"]
            market_data = self.markets.get(asset, {})

            price_key = "yes_price" if side == "YES" else "no_price"

            current_price = market_data.get(
                price_key,
                position["entry_price"]
            )

            value = position["shares"] * current_price
            unrealized_pnl += value - position["amount"]

        total_pnl = realized_pnl + unrealized_pnl

        positions_value = sum(
            position["amount"]
            for position in self.positions
        )

        equity = self.balance + positions_value + unrealized_pnl

        roi = (
            (equity - self.initial_balance)
            / self.initial_balance
            * 100
            if self.initial_balance > 0
            else 0
        )

        total_limits = sum(
            len(orders)
            for orders in self.active_limit_orders.values()
        )

        return {
            "cash_balance": round(self.balance, 2),
            "equity": round(equity, 2),
            "total_pnl": round(total_pnl, 2),
            "roi": round(roi, 2),
            "active_positions": len(self.positions),
            "active_limits": total_limits
        }

    def close_all_positions(self):
        """Закрывает все бумажные позиции по текущим ценам."""
        closed = []

        for position in list(self.positions):
            asset = position["asset"]
            side = position["side"]
            market_data = self.markets.get(asset, {})

            price_key = "yes_price" if side == "YES" else "no_price"

            exit_price = market_data.get(
                price_key,
                position["entry_price"]
            )

            payout = position["shares"] * exit_price

            pnl = (
                payout
                - position["amount"]
                - position.get("fee", 0)
            )

            self.balance += payout

            record = {
                **position,
                "exit_price": exit_price,
                "pnl": round(pnl, 2)
            }

            self.trade_history.append(record)
            closed.append(record)
            self.positions.remove(position)

        return closed

    def reset_account(self):
        """Сбрасывает бумажный счёт и историю сделок."""
        self.balance = self.initial_balance
        self.positions.clear()
        self.active_limit_orders.clear()
        self.trade_history.clear()


trader = HybridPaperTrader(INITIAL_BALANCE)


# ============================================================
# ПОЛУЧЕНИЕ РЫНКА ПО SLUG
# ============================================================

async def fetch_market_by_slug(session, slug):
    """Получает конкретный рынок через Gamma API."""
    direct_url = (
        "https://gamma-api.polymarket.com/"
        f"markets/slug/{slug}"
    )

    try:
        async with session.get(direct_url) as response:
            if response.status == 200:
                data = await response.json(content_type=None)
                market = extract_market_from_response(data)

                if market:
                    return market

            elif response.status not in {400, 404}:
                body = await response.text()

                logging.warning(
                    f"⚠️ Gamma direct slug={slug}: "
                    f"HTTP {response.status}, ответ={body[:200]}"
                )

    except asyncio.TimeoutError:
        logging.warning(
            f"⚠️ Тайм-аут Gamma direct: {slug}"
        )

    except Exception as error:
        logging.warning(
            f"⚠️ Ошибка Gamma direct slug={slug}: {error}"
        )

    markets_url = "https://gamma-api.polymarket.com/markets"

    try:
        async with session.get(
            markets_url,
            params={
                "slug": slug,
                "limit": 10
            }
        ) as response:
            if response.status == 200:
                data = await response.json(content_type=None)
                market = extract_market_from_response(data)

                if market:
                    return market

    except asyncio.TimeoutError:
        logging.warning(
            f"⚠️ Тайм-аут Gamma markets: {slug}"
        )

    except Exception as error:
        logging.warning(
            f"⚠️ Ошибка Gamma markets slug={slug}: {error}"
        )

    events_url = "https://gamma-api.polymarket.com/events"

    try:
        async with session.get(
            events_url,
            params={
                "slug": slug,
                "limit": 10
            }
        ) as response:
            if response.status == 200:
                data = await response.json(content_type=None)
                market = extract_market_from_response(data)

                if market:
                    return market

    except asyncio.TimeoutError:
        logging.warning(
            f"⚠️ Тайм-аут Gamma events: {slug}"
        )

    except Exception as error:
        logging.warning(
            f"⚠️ Ошибка Gamma events slug={slug}: {error}"
        )

    return None


# ============================================================
# ПОИСК АКТИВНЫХ 5-МИНУТНЫХ РЫНКОВ
# ============================================================

async def find_asset_5m_market(session, asset):
    """Ищет активный пятиминутный рынок для указанного актива."""
    candidates = []

    for slot_offset in MARKET_SLOT_OFFSETS:
        slot_timestamp = get_5m_slot_timestamp(slot_offset)
        slug = build_5m_slug(asset, slot_timestamp)

        if not slug:
            continue

        market = await fetch_market_by_slug(session, slug)

        if not market:
            continue

        if not is_market_usable(market):
            logging.debug(
                f"Рынок найден, но недоступен: {asset} {slug}"
            )
            continue

        end_timestamp = get_market_end_timestamp(market)

        candidates.append(
            {
                "market": market,
                "slug": slug,
                "slot_timestamp": slot_timestamp,
                "end_timestamp": end_timestamp
            }
        )

    if not candidates:
        return None

    current_timestamp = time.time()

    future_candidates = [
        candidate
        for candidate in candidates
        if (
            candidate["end_timestamp"] == 0
            or candidate["end_timestamp"] > current_timestamp
        )
    ]

    if future_candidates:
        future_candidates.sort(
            key=lambda item: abs(
                item["slot_timestamp"] - current_timestamp
            )
        )

        return future_candidates[0]

    candidates.sort(
        key=lambda item: abs(
            item["slot_timestamp"] - current_timestamp
        )
    )

    return candidates[0]


async def fetch_5m_polymarket_markets():
    """Обновляет активные пятиминутные рынки всех токенов."""
    timeout = aiohttp.ClientTimeout(
        total=15,
        connect=5,
        sock_read=10
    )

    headers = {
        "Accept": "application/json",
        "User-Agent": "Polymarket-5m-Hybrid-Bot/1.0"
    }

    connector = aiohttp.TCPConnector(
        limit=20,
        ttl_dns_cache=300
    )

    found_markets = {}
    missing_assets = []

    async with ClientSession(
        timeout=timeout,
        headers=headers,
        connector=connector
    ) as session:
        search_tasks = [
            find_asset_5m_market(session, asset)
            for asset in TARGET_ASSETS
        ]

        search_results = await asyncio.gather(
            *search_tasks,
            return_exceptions=True
        )

        for asset, result in zip(TARGET_ASSETS, search_results):
            if isinstance(result, Exception):
                logging.error(
                    f"❌ Ошибка поиска рынка {asset}: {result}"
                )

                missing_assets.append(asset)
                continue

            if not result:
                missing_assets.append(asset)

                logging.warning(
                    f"⚠️ Активный 5m-рынок {asset} пока не найден"
                )

                continue

            market = result["market"]
            slug = result["slug"]
            token_map = get_outcome_token_map(market)
            old_market = trader.markets.get(asset, {})
            old_slug = old_market.get("slug")

            if old_slug == slug:
                old_yes_price = old_market.get("yes_price", 0.50)
                old_no_price = old_market.get("no_price", 0.50)
            else:
                old_yes_price = 0.50
                old_no_price = 0.50

            found_markets[asset] = {
                "question": (
                    market.get("question")
                    or market.get("title")
                    or f"{asset} Up or Down 5m"
                ),
                "yes_token": token_map["UP"],
                "no_token": token_map["DOWN"],
                "yes_price": old_yes_price,
                "no_price": old_no_price,
                "yes_best_bid": old_market.get("yes_best_bid", 0.0),
                "yes_best_ask": old_market.get("yes_best_ask", 0.0),
                "no_best_bid": old_market.get("no_best_bid", 0.0),
                "no_best_ask": old_market.get("no_best_ask", 0.0),
                "slug": slug,
                "condition_id": (
                    market.get("conditionId")
                    or market.get("condition_id")
                ),
                "market_id": market.get("id"),
                "end_date": (
                    market.get("endDate")
                    or market.get("endDateIso")
                ),
                "accepting_orders": parse_boolean(
                    market.get("acceptingOrders"),
                    default=True
                )
            }

            if old_slug != slug:
                logging.info(
                    f"✅ НОВЫЙ РЫНОК {asset} 5m"
                )
                logging.info(
                    f"   Slug: {slug}"
                )
                logging.info(
                    f"   Вопрос: {found_markets[asset]['question']}"
                )
                logging.info(
                    f"   UP token: {token_map['UP'][:20]}..."
                )
                logging.info(
                    f"   DOWN token: {token_map['DOWN'][:20]}..."
                )

    updated_markets = dict(trader.markets)

    for asset, market_data in found_markets.items():
        updated_markets[asset] = market_data

    current_timestamp = time.time()

    for asset in list(updated_markets.keys()):
        market_data = updated_markets[asset]
        end_date = market_data.get("end_date")

        if not end_date:
            continue

        try:
            end_timestamp = datetime.fromisoformat(
                str(end_date).replace("Z", "+00:00")
            ).timestamp()

            if (
                end_timestamp < current_timestamp - 300
                and asset not in found_markets
            ):
                logging.warning(
                    f"🗑 Удалён устаревший рынок "
                    f"{asset}: {market_data.get('slug')}"
                )

                updated_markets.pop(asset, None)
                trader.active_limit_orders.pop(asset, None)

        except (ValueError, TypeError):
            pass

    trader.markets = updated_markets

    logging.info(
        "🔄 Активные 5m-рынки: "
        f"{list(trader.markets.keys())}"
    )

    if missing_assets:
        logging.warning(
            "⚠️ При текущем обновлении не найдены: "
            f"{missing_assets}"
        )


# ============================================================
# BINANCE MULTI-STREAM WEBSOCKET
# ============================================================

async def binance_ws_loop():
    """Получает сделки Binance по поддерживаемым активам."""
    stream_assets = [
        asset
        for asset in TARGET_ASSETS
        if asset != "HYPE"
    ]

    streams = "/".join(
        f"{asset.lower()}usdt@trade"
        for asset in stream_assets
    )

    uri = (
        "wss://stream.binance.com:9443/"
        f"stream?streams={streams}"
    )

    while True:
        try:
            async with websockets.connect(
                uri,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_queue=2048
            ) as websocket:
                logging.info(
                    "🌐 Binance Multi-Stream WebSocket подключён"
                )

                while True:
                    raw_message = await websocket.recv()
                    message = json.loads(raw_message)
                    stream_data = message.get("data")

                    if not stream_data:
                        continue

                    symbol = (
                        stream_data.get("s", "")
                        .replace("USDT", "")
                    )

                    if symbol not in binance_histories:
                        continue

                    price = float(stream_data["p"])
                    current_time = time.time()

                    binance_prices[symbol] = price
                    history = binance_histories[symbol]

                    history.append(
                        (current_time, price)
                    )

                    while (
                        history
                        and current_time - history[0][0]
                        > LOOKBACK_SECONDS
                    ):
                        history.popleft()

                    if (
                        len(history) <= 1
                        or not trader.auto_trade_enabled
                    ):
                        continue

                    old_price = history[0][1]

                    if old_price <= 0:
                        continue

                    percent_change = (
                        (price - old_price)
                        / old_price
                        * 100
                    )

                    signal_cooldown_passed = (
                        current_time
                        - last_signal_times[symbol]
                        > 10
                    )

                    if (
                        abs(percent_change) >= MOMENTUM_THRESHOLD
                        and signal_cooldown_passed
                    ):
                        if symbol not in trader.markets:
                            logging.warning(
                                f"⚠️ Сигнал {symbol} есть, "
                                "но 5m-рынок ещё не найден"
                            )
                            continue

                        last_signal_times[symbol] = current_time
                        trader.current_mode = "TAKER"

                        side = "YES" if percent_change > 0 else "NO"

                        reason = (
                            f"Binance {symbol} "
                            f"{percent_change:+.2f}% "
                            f"за {LOOKBACK_SECONDS} с"
                        )

                        logging.info(
                            f"🚨 5M TAKER-СИГНАЛ: {reason}"
                        )

                        success, result = trader.execute_taker_trade(
                            symbol,
                            side,
                            reason
                        )

                        if success:
                            position = result

                            logging.info(
                                f"✅ PAPER ENTRY {symbol} {side} | "
                                f"цена={position['entry_price']} | "
                                f"сумма={position['amount']}"
                            )

                            if ALLOWED_CHAT_ID:
                                try:
                                    await bot.send_message(
                                        ALLOWED_CHAT_ID,
                                        (
                                            "⚡ **[HYBRID 5M EXECUTION]**\n"
                                            f"Рынок: **{symbol} (5m)**\n"
                                            f"Сигнал: `{reason}`\n"
                                            f"Исход: **{side}**\n"
                                            f"Вход: `${position['entry_price']}`\n"
                                            f"Объём: `${position['amount']}` USDC\n"
                                            f"Комиссия 3%: `${position['fee']}`"
                                        ),
                                        parse_mode="Markdown"
                                    )

                                except Exception as error:
                                    logging.error(
                                        "❌ Ошибка отправки уведомления "
                                        f"в Telegram: {error}"
                                    )

                        else:
                            logging.warning(
                                f"⚠️ Сделка не открыта: {result}"
                            )

                        await asyncio.sleep(2)
                        trader.current_mode = "MAKER"

        except asyncio.CancelledError:
            raise

        except Exception as error:
            logging.error(
                f"❌ Ошибка Binance WS: {error}"
            )

            await asyncio.sleep(3)


# ============================================================
# ПОЛУЧЕНИЕ СТАКАНА
# ============================================================

def get_order_book_sync(token_id):
    """Получает стакан CLOB в синхронном режиме."""
    return clob_client.get_order_book(token_id)


def extract_best_prices(orderbook):
    """Возвращает лучший bid и лучший ask из стакана."""
    best_bid = 0.0
    best_ask = 0.0

    if orderbook is None:
        return best_bid, best_ask

    bids = getattr(orderbook, "bids", None) or []
    asks = getattr(orderbook, "asks", None) or []

    valid_bids = []

    for level in bids:
        try:
            price = float(level.price)

            if 0 < price < 1:
                valid_bids.append(price)
        except (ValueError, TypeError, AttributeError):
            continue

    valid_asks = []

    for level in asks:
        try:
            price = float(level.price)

            if 0 < price < 1:
                valid_asks.append(price)
        except (ValueError, TypeError, AttributeError):
            continue

    if valid_bids:
        best_bid = max(valid_bids)

    if valid_asks:
        best_ask = min(valid_asks)

    return best_bid, best_ask


async def update_asset_orderbooks(asset, market_data):
    """Получает стаканы токенов UP и DOWN."""
    up_token = market_data.get("yes_token")
    down_token = market_data.get("no_token")

    if not up_token or not down_token:
        return

    try:
        up_book_task = asyncio.to_thread(
            get_order_book_sync,
            up_token
        )

        down_book_task = asyncio.to_thread(
            get_order_book_sync,
            down_token
        )

        up_book, down_book = await asyncio.gather(
            up_book_task,
            down_book_task
        )

        up_best_bid, up_best_ask = extract_best_prices(up_book)
        down_best_bid, down_best_ask = extract_best_prices(down_book)

        if up_best_bid > 0:
            market_data["yes_best_bid"] = up_best_bid

        if up_best_ask > 0:
            market_data["yes_best_ask"] = up_best_ask
            market_data["yes_price"] = round(up_best_ask, 4)

        if down_best_bid > 0:
            market_data["no_best_bid"] = down_best_bid

        if down_best_ask > 0:
            market_data["no_best_ask"] = down_best_ask
            market_data["no_price"] = round(down_best_ask, 4)

        if (
            market_data.get("yes_price", 0) <= 0
            and down_best_bid > 0
        ):
            market_data["yes_price"] = round(
                1 - down_best_bid,
                4
            )

        if (
            market_data.get("no_price", 0) <= 0
            and up_best_bid > 0
        ):
            market_data["no_price"] = round(
                1 - up_best_bid,
                4
            )

        if (
            trader.current_mode == "MAKER"
            and up_best_bid > 0
            and up_best_ask > 0
        ):
            trader.update_maker_orders(
                asset,
                up_best_bid,
                up_best_ask
            )

        logging.debug(
            f"📖 {asset} | "
            f"UP bid/ask={up_best_bid}/{up_best_ask} | "
            f"DOWN bid/ask={down_best_bid}/{down_best_ask}"
        )

    except Exception as error:
        logging.warning(
            f"⚠️ Ошибка получения стакана {asset}: {error}"
        )


# ============================================================
# ЦИКЛ CLOB И РОТАЦИИ РЫНКОВ
# ============================================================

async def polymarket_clob_loop():
    """Обновляет текущие рынки и их стаканы."""
    last_market_update_time = 0.0

    while True:
        try:
            current_time = time.time()

            if (
                current_time - last_market_update_time
                >= MARKET_REFRESH_INTERVAL
            ):
                await fetch_5m_polymarket_markets()
                last_market_update_time = current_time

            markets_snapshot = list(
                trader.markets.items()
            )

            if markets_snapshot:
                orderbook_tasks = [
                    update_asset_orderbooks(
                        asset,
                        market_data
                    )
                    for asset, market_data in markets_snapshot
                ]

                await asyncio.gather(
                    *orderbook_tasks,
                    return_exceptions=True
                )

        except asyncio.CancelledError:
            raise

        except Exception as error:
            logging.error(
                f"❌ Ошибка в цикле CLOB: {error}"
            )

        await asyncio.sleep(
            ORDERBOOK_REFRESH_INTERVAL
        )


# ============================================================
# TELEGRAM — КЛАВИАТУРА
# ============================================================

def get_main_keyboard():
    markup = types.InlineKeyboardMarkup(
        row_width=2
    )

    button_pnl = types.InlineKeyboardButton(
        "📊 5m-рынки и PnL",
        callback_data="stats"
    )

    button_positions = types.InlineKeyboardButton(
        "💼 Позиции",
        callback_data="positions"
    )

    button_close = types.InlineKeyboardButton(
        "🧹 Закрыть все",
        callback_data="close_all"
    )

    button_toggle = types.InlineKeyboardButton(
        (
            "🤖 Автоторговля: "
            f"{'ВКЛ 🟢' if trader.auto_trade_enabled else 'ВЫКЛ 🔴'}"
        ),
        callback_data="toggle_auto"
    )

    button_reset = types.InlineKeyboardButton(
        "🔄 Сбросить баланс",
        callback_data="reset"
    )

    markup.add(
        button_pnl,
        button_positions
    )

    markup.add(
        button_close,
        button_toggle
    )

    markup.add(button_reset)
    return markup


def is_authorized(chat_id):
    """Проверяет разрешённый Telegram chat ID."""
    global ALLOWED_CHAT_ID

    if ALLOWED_CHAT_ID is None:
        ALLOWED_CHAT_ID = chat_id

        logging.warning(
            "⚠️ TELEGRAM_CHAT_ID не задан. "
            f"Разрешён первый chat_id: {chat_id}"
        )

        return True

    return chat_id == ALLOWED_CHAT_ID


# ============================================================
# TELEGRAM — КОМАНДЫ
# ============================================================

@bot.message_handler(commands=["start", "menu"])
async def send_welcome(message):
    if not is_authorized(message.chat.id):
        await bot.send_message(
            message.chat.id,
            "⛔ Доступ ограничен."
        )
        return

    text = (
        "🤖 **Polymarket 5m Hybrid Bot**\n\n"
        "• **Мониторинг 5m-рынков:** "
        f"`{', '.join(TARGET_ASSETS)}`\n"
        "• **Поток Binance:** "
        "BTC/ETH/SOL/DOGE/BNB/XRP\n"
        "• **Авторотация:** "
        "контракты обновляются каждые 30 секунд\n"
        "• **MAKER:** "
        "пассивная работа внутри спреда\n"
        "• **TAKER:** "
        "сигнал при импульсе "
        f"`≥ {MOMENTUM_THRESHOLD:.2f}%`\n"
        "• **Режим торговли:** "
        "бумажный счёт"
    )

    await bot.send_message(
        message.chat.id,
        text,
        parse_mode="Markdown",
        reply_markup=get_main_keyboard()
    )


# ============================================================
# TELEGRAM — CALLBACK-КНОПКИ
# ============================================================

@bot.callback_query_handler(func=lambda call: True)
async def handle_callbacks(call):
    chat_id = call.message.chat.id

    if not is_authorized(chat_id):
        await bot.answer_callback_query(
            call.id,
            "⛔ Доступ ограничен.",
            show_alert=True
        )
        return

    try:
        if call.data == "stats":
            stats = trader.get_stats()
            pnl_icon = "🟩" if stats["total_pnl"] >= 0 else "🟥"
            prices_lines = []

            for asset in TARGET_ASSETS:
                binance_price = binance_prices.get(
                    asset,
                    0.0
                )

                market_data = trader.markets.get(asset)

                if not market_data:
                    prices_lines.append(
                        f"• **{asset}:** рынок не найден"
                    )
                    continue

                yes_price = market_data.get(
                    "yes_price",
                    0.0
                )

                no_price = market_data.get(
                    "no_price",
                    0.0
                )

                if binance_price > 0:
                    spot_text = f"${binance_price:,.4f}"
                elif asset == "HYPE":
                    spot_text = "нет на Binance"
                else:
                    spot_text = "ожидание"

                prices_lines.append(
                    f"• **{asset} (5m):** "
                    f"Spot `{spot_text}` | "
                    f"UP `${yes_price:.4f}` / "
                    f"DOWN `${no_price:.4f}`"
                )

            prices_text = "\n".join(prices_lines)

            text = (
                "📊 **5-МИНУТНЫЕ РЫНКИ И PnL**\n"
                "───────────────────\n"
                f"{prices_text}\n\n"
                f"📍 **Режим:** `{trader.current_mode}`\n"
                f"💵 **Баланс:** `${stats['cash_balance']}` USDC\n"
                f"💎 **Equity:** `${stats['equity']}` USDC\n"
                f"{pnl_icon} **PnL:** "
                f"`${stats['total_pnl']}` USDC "
                f"({stats['roi']}%)\n"
                f"📌 **Maker-лимитки:** "
                f"`{stats['active_limits']}` шт.\n"
                f"💼 **Taker-позиции:** "
                f"`{stats['active_positions']}` шт."
            )

            try:
                await bot.edit_message_text(
                    text,
                    chat_id,
                    call.message.message_id,
                    parse_mode="Markdown",
                    reply_markup=get_main_keyboard()
                )

            except Exception as error:
                if "message is not modified" not in str(error).lower():
                    raise

            await bot.answer_callback_query(call.id)

        elif call.data == "positions":
            if not trader.positions:
                await bot.answer_callback_query(
                    call.id,
                    "📭 Активных позиций нет!"
                )
                return

            text = "💼 **ОТКРЫТЫЕ 5M-ПОЗИЦИИ:**\n\n"

            for position in trader.positions:
                market_data = trader.markets.get(
                    position["asset"],
                    {}
                )

                price_key = (
                    "yes_price"
                    if position["side"] == "YES"
                    else "no_price"
                )

                current_price = market_data.get(
                    price_key,
                    position["entry_price"]
                )

                pnl = (
                    position["shares"]
                    * current_price
                    - position["amount"]
                    - position.get("fee", 0)
                )

                icon = "🟢" if pnl >= 0 else "🔴"
                safe_reason = safe_markdown_text(
                    position["reason"]
                )

                text += (
                    f"🔹 **[{position['asset']}] "
                    f"{position['side']}** "
                    f"({position['mode']})\n"
                    f"└ Сигнал: `{safe_reason}`\n"
                    f"└ Вход: `${position['entry_price']:.4f}`\n"
                    f"└ Текущая цена: `${current_price:.4f}`\n"
                    f"└ PnL: {icon} `${pnl:.2f}` USDC\n\n"
                )

            await bot.send_message(
                chat_id,
                text,
                parse_mode="Markdown"
            )

            await bot.answer_callback_query(call.id)

        elif call.data == "close_all":
            closed_positions = trader.close_all_positions()

            total_pnl = sum(
                position.get("pnl", 0)
                for position in closed_positions
            )

            await bot.send_message(
                chat_id,
                (
                    "🧹 Закрыто 5m-позиций: "
                    f"{len(closed_positions)}\n"
                    f"Результат: `{total_pnl:+.2f} USDC`"
                ),
                parse_mode="Markdown"
            )

            await bot.answer_callback_query(call.id)

        elif call.data == "toggle_auto":
            trader.auto_trade_enabled = (
                not trader.auto_trade_enabled
            )

            status = (
                "ВКЛЮЧЕНА"
                if trader.auto_trade_enabled
                else "ВЫКЛЮЧЕНА"
            )

            await bot.answer_callback_query(
                call.id,
                f"Автоторговля: {status}"
            )

            await bot.edit_message_reply_markup(
                chat_id,
                call.message.message_id,
                reply_markup=get_main_keyboard()
            )

        elif call.data == "reset":
            trader.reset_account()

            await bot.answer_callback_query(
                call.id,
                "🔄 Баланс и история сброшены!",
                show_alert=True
            )

            await bot.edit_message_reply_markup(
                chat_id,
                call.message.message_id,
                reply_markup=get_main_keyboard()
            )

        else:
            await bot.answer_callback_query(
                call.id,
                "Неизвестная команда"
            )

    except Exception as error:
        logging.exception(
            f"❌ Ошибка callback {call.data}: {error}"
        )

        try:
            await bot.answer_callback_query(
                call.id,
                "Произошла ошибка. Проверьте логи.",
                show_alert=True
            )
        except Exception:
            pass


# ============================================================
# ВЕБ-СЕРВЕР ДЛЯ RENDER
# ============================================================

async def handle_ping(request):
    stats = trader.get_stats()

    response_data = {
        "status": "ok",
        "service": "Polymarket 5m Hybrid Bot",
        "markets": list(trader.markets.keys()),
        "market_slugs": {
            asset: market.get("slug")
            for asset, market in trader.markets.items()
        },
        "balance": stats["cash_balance"],
        "equity": stats["equity"],
        "auto_trade_enabled": trader.auto_trade_enabled
    }

    return web.json_response(response_data)


async def start_web_server():
    app = web.Application()

    app.router.add_get("/", handle_ping)
    app.router.add_get("/health", handle_ping)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(
        runner,
        "0.0.0.0",
        PORT
    )

    await site.start()

    logging.info(
        f"🌐 Веб-сервер запущен на порту {PORT}"
    )

    return runner


# ============================================================
# ЗАПУСК
# ============================================================

async def main():
    web_runner = await start_web_server()

    try:
        bot_user = await bot.get_me()

        logging.info(
            "✅ Telegram-бот успешно запущен: "
            f"@{bot_user.username}"
        )

    except Exception as error:
        logging.critical(
            f"❌ Ошибка авторизации Telegram: {error}"
        )

        await web_runner.cleanup()
        raise

    try:
        await fetch_5m_polymarket_markets()

    except Exception as error:
        logging.error(
            f"❌ Ошибка первого поиска рынков: {error}"
        )

    tasks = [
        asyncio.create_task(
            binance_ws_loop(),
            name="binance_ws"
        ),
        asyncio.create_task(
            polymarket_clob_loop(),
            name="polymarket_clob"
        ),
        asyncio.create_task(
            bot.polling(
                non_stop=True,
                skip_pending=True,
                timeout=30,
                request_timeout=40
            ),
            name="telegram_polling"
        )
    ]

    try:
        await asyncio.gather(*tasks)

    except asyncio.CancelledError:
        logging.info(
            "🛑 Получен сигнал остановки"
        )
        raise

    except Exception as error:
        logging.exception(
            f"❌ Критическая ошибка main: {error}"
        )
        raise

    finally:
        for task in tasks:
            if not task.done():
                task.cancel()

        await asyncio.gather(
            *tasks,
            return_exceptions=True
        )

        await web_runner.cleanup()

        try:
            await bot.close_session()
        except Exception:
            pass

        logging.info(
            "🛑 Бот остановлен"
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        logging.info(
            "🛑 Бот остановлен пользователем"
        )

    except Exception as error:
        logging.critical(
            f"❌ Завершение работы с ошибкой: {error}"
        )

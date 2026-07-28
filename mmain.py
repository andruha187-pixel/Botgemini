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

TELEGRAM_TOKEN = (
    os.getenv("TELEGRAM_TOKEN")
    or os.getenv("TELEGRAM_BOT_TOKEN")
)

ALLOWED_CHAT_ID_ENV = os.getenv("TELEGRAM_CHAT_ID")
INITIAL_BALANCE = float(os.getenv("INITIAL_BALANCE", "1000.0"))
PORT = int(os.getenv("PORT", "8080"))

ALLOWED_CHAT_ID = (
    int(ALLOWED_CHAT_ID_ENV)
    if ALLOWED_CHAT_ID_ENV
    and ALLOWED_CHAT_ID_ENV.lstrip("-").isdigit()
    else None
)


# ============================================================
# ЛОГИ
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

# Порог импульса Binance в процентах за 5 секунд
MOMENTUM_THRESHOLD = 0.20

# Окно анализа Binance
LOOKBACK_SECONDS = 5

# Отступ Maker-лимитного ордера
MAKER_SPREAD_OFFSET = 0.01

# Моделируемая Taker-комиссия
TAKER_COMMISSION = 0.03

# Как часто искать новый пятиминутный контракт
MARKET_REFRESH_INTERVAL = 30

# Как часто обновлять стаканы
ORDERBOOK_REFRESH_INTERVAL = 2

# Количество временных интервалов вокруг текущего,
# которые будут проверяться при поиске рынка
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
    """
    Gamma API может вернуть массив либо настоящим list,
    либо JSON-строкой вида '["Up", "Down"]'.
    """

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
    """
    Преобразует значения Gamma API в bool.
    """

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
    """
    Возвращает UNIX timestamp начала пятиминутного интервала.

    offset:
       -1 — предыдущий рынок;
        0 — текущий рынок;
        1 — следующий рынок.
    """

    interval_seconds = 5 * 60
    current_timestamp = int(time.time())

    current_slot = (
        current_timestamp // interval_seconds
    ) * interval_seconds

    return current_slot + offset * interval_seconds


def build_5m_slug(asset, slot_timestamp):
    """
    Формирует slug вида:
    btc-updown-5m-1785266400
    """

    prefix = POLYMARKET_5M_SLUG_PREFIXES.get(asset)

    if not prefix:
        return None

    return f"{prefix}-updown-5m-{slot_timestamp}"


def get_outcome_token_map(market):
    """
    Сопоставляет названия исходов и CLOB token ID.

    Результат, например:
    {
        "UP": "...",
        "DOWN": "..."
    }
    """

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
            normalized_outcome = (
                str(outcome)
                .strip()
                .upper()
            )

            result[normalized_outcome] = str(token_id)

    # Дополнительная нормализация,
    # если API использует Yes/No вместо Up/Down.
    if "YES" in result and "UP" not in result:
        result["UP"] = result["YES"]

    if "NO" in result and "DOWN" not in result:
        result["DOWN"] = result["NO"]

    # Резервный вариант:
    # первый token соответствует первому outcome,
    # второй — второму.
    if "UP" not in result:
        result["UP"] = str(token_ids[0])

    if "DOWN" not in result:
        result["DOWN"] = str(token_ids[1])

    return result


def get_market_end_timestamp(market):
    """
    Получает время окончания рынка в UNIX timestamp.
    """

    end_date = (
        market.get("endDate")
        or market.get("endDateIso")
        or market.get("end_date_iso")
    )

    if not end_date:
        return 0

    try:
        normalized = str(end_date).replace(
            "Z",
            "+00:00"
        )

        return datetime.fromisoformat(
            normalized
        ).timestamp()

    except (ValueError, TypeError):
        return 0


def is_market_usable(market):
    """
    Проверяет, можно ли использовать рынок для CLOB.
    """

    if not isinstance(market, dict):
        return False

    token_map = get_outcome_token_map(market)

    if not token_map.get("UP"):
        return False

    if not token_map.get("DOWN"):
        return False

    is_closed = parse_boolean(
        market.get("closed"),
        default=False
    )

    if is_closed:
        return False

    is_active = parse_boolean(
        market.get("active"),
        default=True
    )

    if not is_active:
        return False

    enable_order_book = parse_boolean(
        market.get("enableOrderBook"),
        default=True
    )

    if not enable_order_book:
        return False

    accepting_orders = parse_boolean(
        market.get("acceptingOrders"),
        default=True
    )

    if not accepting_orders:
        return False

    return True


def extract_market_from_response(data):
    """
    Извлекает market из разных возможных форматов ответа.
    """

    if isinstance(data, dict):
        # Прямой ответ /markets/slug/{slug}
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
    """
    Убирает символы, способные сломать обычный Markdown Telegram.
    """

    text = str(value)

    for symbol in [
        "*",
        "_",
        "`",
        "[",
        "]"
    ]:
        text = text.replace(symbol, "")

    return text


# ============================================================
# HYBRID 5M PAPER TRADER
# ============================================================

class HybridPaperTrader:

    def __init__(self, initial_balance):
        self.initial_balance = initial_balance
        self.balance = initial_balance

        # Taker-сделки
        self.positions = []

        # Maker-ордера:
        # {asset: [orders]}
        self.active_limit_orders = {}

        self.trade_history = []

        self.current_mode = "MAKER"
        self.auto_trade_enabled = True

        # Структура рынка:
        #
        # {
        #   "BTC": {
        #       "question": str,
        #       "yes_token": str,
        #       "no_token": str,
        #       "yes_price": float,
        #       "no_price": float,
        #       "slug": str
        #   }
        # }
        #
        # Поля yes_token/no_token сохранены,
        # чтобы не менять остальную стратегию.
        # Фактически:
        # yes_token = UP token
        # no_token = DOWN token
        self.markets = {}

    def cancel_all_limits(self):
        """
        Мгновенная отмена всех моделируемых Maker-лимиток
        при обнаружении импульса.
        """

        count = sum(
            len(orders)
            for orders in self.active_limit_orders.values()
        )

        self.active_limit_orders.clear()

        return count

    def update_maker_orders(
        self,
        asset,
        best_bid,
        best_ask
    ):
        """
        Режим MAKER:
        моделирование установки лимиток внутри спреда.
        """

        if best_bid <= 0 or best_ask <= 0:
            return

        my_bid = round(
            best_bid + MAKER_SPREAD_OFFSET,
            2
        )

        my_ask = round(
            best_ask - MAKER_SPREAD_OFFSET,
            2
        )

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
            self.active_limit_orders.pop(
                asset,
                None
            )

    def execute_taker_trade(
        self,
        asset,
        side,
        reason
    ):
        """
        Режим TAKER:
        вход по текущей цене с учетом моделируемой комиссии.
        """

        self.cancel_all_limits()

        market_data = self.markets.get(
            asset,
            {}
        )

        yes_price = market_data.get(
            "yes_price",
            0.50
        )

        no_price = market_data.get(
            "no_price",
            0.50
        )

        if side == "YES":
            entry_price = yes_price
        else:
            entry_price = no_price

        entry_price = round(
            float(entry_price),
            4
        )

        if entry_price <= 0 or entry_price >= 1:
            return (
                False,
                "Некорректная цена в стакане"
            )

        trade_amount = 25.0
        fee = trade_amount * TAKER_COMMISSION
        total_cost = trade_amount + fee

        if self.balance < total_cost:
            return (
                False,
                "Недостаточно баланса"
            )

        shares = trade_amount / entry_price

        self.balance -= total_cost

        position = {
            "id": (
                len(self.positions)
                + len(self.trade_history)
                + 1
            ),
            "mode": "TAKER ⚡ 5M",
            "asset": asset,
            "question": market_data.get(
                "question",
                asset
            )[:80],
            "side": side,
            "amount": trade_amount,
            "shares": round(shares, 4),
            "entry_price": entry_price,
            "fee": round(fee, 2),
            "reason": reason,
            "timestamp": datetime.now().strftime(
                "%H:%M:%S"
            ),
            "market_slug": market_data.get(
                "slug",
                ""
            )
        }

        self.positions.append(position)

        return True, position

    def get_stats(self):
        """
        Возвращает статистику бумажного счета.
        """

        realized_pnl = sum(
            trade.get("pnl", 0)
            for trade in self.trade_history
        )

        unrealized_pnl = 0.0

        for position in self.positions:
            asset = position["asset"]
            side = position["side"]

            market_data = self.markets.get(
                asset,
                {}
            )

            price_key = (
                "yes_price"
                if side == "YES"
                else "no_price"
            )

            current_price = market_data.get(
                price_key,
                position["entry_price"]
            )

            value = (
                position["shares"]
                * current_price
            )

            unrealized_pnl += (
                value - position["amount"]
            )

        total_pnl = (
            realized_pnl
            + unrealized_pnl
        )

        positions_value = sum(
            position["amount"]
            for position in self.positions
        )

        equity = (
            self.balance
            + positions_value
            + unrealized_pnl
        )

        roi = (
            (
                equity - self.initial_balance
            )
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
            "cash_balance": round(
                self.balance,
                2
            ),
            "equity": round(
                equity,
                2
            ),
            "total_pnl": round(
                total_pnl,
                2
            ),
            "roi": round(
                roi,
                2
            ),
            "active_positions": len(
                self.positions
            ),
            "active_limits": total_limits
        }

    def close_all_positions(self):
        """
        Закрывает все бумажные позиции
        по текущим ценам.
        """

        closed = []

        for position in list(self.positions):
            asset = position["asset"]
            side = position["side"]

            market_data = self.markets.get(
                asset,
                {}
            )

            price_key = (
                "yes_price"
                if side == "YES"
                else "no_price"
            )

            exit_price = market_data.get(
                price_key,
                position["entry_price"]
            )

            payout = (
                position["shares"]
                * exit_price
            )

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
        """
        Сбрасывает бумажный счет.
        """

        self.balance = self.initial_balance

        self.positions.clear()
        self.active_limit_orders.clear()
        self.trade_history.clear()


trader = HybridPaperTrader(
    INITIAL_BALANCE
)


# ============================================================
# ПОЛУЧЕНИЕ РЫНКА ПО SLUG
# ============================================================

async def fetch_market_by_slug(
    session,
    slug
):
    """
    Получает конкретный рынок через Gamma API.

    Основной endpoint:
    /markets/slug/{slug}

    Дополнительные резервные endpoints:
    /markets?slug={slug}
    /events?slug={slug}
    """

    direct_url = (
        "https://gamma-api.polymarket.com/"
        f"markets/slug/{slug}"
    )

    try:
        async with session.get(
            direct_url
        ) as response:

            if response.status == 200:
                data = await response.json(
                    content_type=None
                )

                market = extract_market_from_response(
                    data
                )

                if market:
                    return market

            elif response.status not in {
                400,
                404
            }:
                body = await response.text()

                logging.warning(
                    f"⚠️ Gamma direct slug={slug}: "
                    f"HTTP {response.status}, "
                    f"ответ={body[:200]}"
                )

    except asyncio.TimeoutError:
        logging.warning(
            f"⚠️ Таймаут Gamma direct: {slug}"
        )

    except Exception as error:
        logging.warning(
            f"⚠️ Ошибка Gamma direct "
            f"slug={slug}: {error}"
        )

    # Резервный поиск через markets?slug=
    markets_url = (
        "https://gamma-api.polymarket.com/markets"
    )

    try:
        async with session.get(
            markets_url,
            params={
                "slug": slug,
                "limit": 10
            }
        ) as response:

            if response.status == 200:
                data = await response.json(
                    content_type=None
                )

                market = extract_market_from_response(
                    data
                )

                if market:
                    return market

    except asyncio.TimeoutError:
        logging.warning(
            f"⚠️ Таймаут Gamma markets: {slug}"
        )

    except Exception as error:
        logging.warning(
            f"⚠️ Ошибка Gamma markets "
            f"slug={slug}: {error}"
        )

    # Последний резервный поиск через event
    events_url = (
        "https://gamma-api.polymarket.com/events"
    )

    try:
        async with session.get(
            events_url,
            params={
                "slug": slug,
                "limit": 10
            }
        ) as response:

            if response.status == 200:
                data = await response.json(
                    content_type=None
                )

                market = extract_market_from_response(
                    data
                )

                if market:
                    return market

    except asyncio.TimeoutError:
        logging.warning(
            f"⚠️ Таймаут Gamma events: {slug}"
        )

    except Exception as error:
        logging.warning(
            f"⚠️ Ошибка Gamma events "
            f"slug={slug}: {error}"
        )

    return None


# ============================================================
# ПОИСК АКТИВНЫХ 5-МИНУТНЫХ РЫНКОВ
# ============================================================

async def find_asset_5m_market(
    session,
    asset
):
    """
    Ищет активный пятиминутный рынок конкретного актива.

    Проверяются текущий, следующий, предыдущий
    и соседние пятиминутные интервалы.
    """

    candidates = []

    for slot_offset in MARKET_SLOT_OFFSETS:
        slot_timestamp = get_5m_slot_timestamp(
            slot_offset
        )

        slug = build_5m_slug(
            asset,
            slot_timestamp
        )

        if not slug:
            continue

        market = await fetch_market_by_slug(
            session,
            slug
        )

        if not market:
            continue

        if not is_market_usable(market):
            logging.debug(
                f"Рынок найден, но неактивен: "
                f"{asset} {slug}"
            )
            continue

        end_timestamp = get_market_end_timestamp(
            market
        )

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

    # В первую очередь выбираем контракт,
    # который ещё не закончился и ближе всего к текущему времени.
    future_candidates = [
        candidate
        for candidate in candidates
        if (
            candidate["end_timestamp"] == 0
            or candidate["end_timestamp"]
            > current_timestamp
        )
    ]

    if future_candidates:
        future_candidates.sort(
            key=lambda item: abs(
                item["slot_timestamp"]
                - current_timestamp
            )
        )

        return future_candidates[0]

    candidates.sort(
        key=lambda item: abs(
            item["slot_timestamp"]
            - current_timestamp
        )
    )

    return candidates[0]


async def fetch_5m_polymarket_markets():
    """
    Обновляет все активные пятиминутные рынки.
    """

    timeout = aiohttp.ClientTimeout(
        total=15,
        connect=5,
        sock_read=10
    )

    headers = {
        "Accept": "application/json",
        "User-Agent": (
            "Polymarket-5m-Hybrid-Bot/1.0"
        )
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
            find_asset_5m_market(
                session,
                asset
            )
            for asset in TARGET_ASSETS
        ]

        search_results = await asyncio.gather(
            *search_tasks,
            return_exceptions=True
        )

        for asset, result in zip(
            TARGET_ASSETS,
            search_results
        ):
            if isinstance(result, Exception):
                logging.error(
                    f"❌ Ошибка поиска рынка "
                    f"{asset}: {result}"
                )

                missing_assets.append(asset)
                continue

            if not result:
                missing_assets.append(asset)

                logging.warning(
                    f"⚠️ Активный 5m рынок "
                    f"{asset} пока не найден"
                )

                continue

            market = result["market"]
            slug = result["slug"]

            token_map = get_outcome_token_map(
                market
            )

            old_market = trader.markets.get(
                asset,
                {}
            )

            old_slug = old_market.get(
                "slug"
            )

            # При переходе на новый контракт
            # цена сбрасывается до 0.50 до получения стакана.
            if old_slug == slug:
                old_yes_price = old_market.get(
                    "yes_price",
                    0.50
                )

                old_no_price = old_market.get(
                    "no_price",
                    0.50
                )
            else:
                old_yes_price = 0.50
                old_no_price = 0.50

            found_markets[asset] = {
                "question": market.get(
                    "question"
                )
                or market.get("title")
                or f"{asset} Up or Down 5m",

                # Имена сохранены для совместимости
                # с исходной стратегией:
                #
                # yes_token = UP token
                # no_token = DOWN token
                "yes_token": token_map["UP"],
                "no_token": token_map["DOWN"],

                "yes_price": old_yes_price,
                "no_price": old_no_price,

                "yes_best_bid": old_market.get(
                    "yes_best_bid",
                    0.0
                ),

                "yes_best_ask": old_market.get(
                    "yes_best_ask",
                    0.0
                ),

                "no_best_bid": old_market.get(
                    "no_best_bid",
                    0.0
                ),

                "no_best_ask": old_market.get(
                    "no_best_ask",
                    0.0
                ),

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
                    f"   Вопрос: "
                    f"{found_markets[asset]['question']}"
                )

                logging.info(
                    f"   UP token: "
                    f"{token_map['UP'][:20]}..."
                )

                logging.info(
                    f"   DOWN token: "
                    f"{token_map['DOWN'][:20]}..."
                )

    # Сохраняем найденные рынки.
    #
    # Для временно не найденного актива оставляем прежний рынок,
    # чтобы кратковременная ошибка Gamma API не очищала стаканы.
    updated_markets = dict(
        trader.markets
    )

    for asset, market_data in found_markets.items():
        updated_markets[asset] = market_data

    # Старый рынок удаляется только тогда,
    # когда он уже явно закончился.
    current_timestamp = time.time()

    for asset in list(updated_markets.keys()):
        market_data = updated_markets[asset]

        end_date = market_data.get("end_date")

        if not end_date:
            continue

        try:
            end_timestamp = datetime.fromisoformat(
                str(end_date).replace(
                    "Z",
                    "+00:00"
                )
            ).timestamp()

            if (
                end_timestamp
                < current_timestamp - 300
                and asset not in found_markets
            ):
                logging.warning(
                    f"🗑 Удалён устаревший рынок "
                    f"{asset}: "
                    f"{market_data.get('slug')}"
                )

                updated_markets.pop(
                    asset,
                    None
                )

                trader.active_limit_orders.pop(
                    asset,
                    None
                )

        except (ValueError, TypeError):
            pass

    trader.markets = updated_markets

    logging.info(
        "🔄 Активные 5m рынки: "
        f"{list(trader.markets.keys())}"
    )

    if missing_assets:
        logging.warning(
            "⚠️ При текущем обновлении "
            "не найдены: "
            f"{missing_assets}"
        )


# ============================================================
# BINANCE MULTI-STREAM WEBSOCKET
# ============================================================

async def binance_ws_loop():
    """
    Слушает сделки Binance по всем поддерживаемым активам.
    """

    # Для HYPE нет обычного HYPEUSDT-стрима Binance,
    # поэтому он исключён только из Binance WebSocket.
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
                    "🌐 Binance Multi-Stream "
                    "WebSocket подключен"
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

                    price = float(
                        stream_data["p"]
                    )

                    current_time = time.time()

                    binance_prices[symbol] = price

                    history = binance_histories[
                        symbol
                    ]

                    history.append(
                        (
                            current_time,
                            price
                        )
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
                        abs(percent_change)
                        >= MOMENTUM_THRESHOLD
                        and signal_cooldown_passed
                    ):
                        if symbol not in trader.markets:
                            logging.warning(
                                f"⚠️ Сигнал {symbol} есть, "
                                "но 5m рынок ещё не найден"
                            )
                            continue

                        last_signal_times[
                            symbol
                        ] = current_time

                        trader.current_mode = "TAKER"

                        side = (
                            "YES"
                            if percent_change > 0
                            else "NO"
                        )

                        reason = (
                            f"Binance {symbol} "
                            f"{percent_change:+.2f}% "
                            f"за {LOOKBACK_SECONDS}s"
                        )

                        logging.info(
                            f"🚨 5M TAKER СИГНАЛ: "
                            f"{reason}"
                        )

                        success, result = (
                            trader.execute_taker_trade(
                                symbol,
                                side,
                                reason
                            )
                        )

                        if success:
                            position = result

                            logging.info(
                                f"✅ PAPER ENTRY "
                                f"{symbol} {side} | "
                                f"цена={position['entry_price']} | "
                                f"сумма={position['amount']}"
                            )

                            if ALLOWED_CHAT_ID:
                                try:
                                    await bot.send_message(
                                        ALLOWED_CHAT_ID,
                                        (
                                            "⚡ "
                                            "**[HYBRID 5M EXECUTION]**\n"
                                            f"Рынок: **{symbol} (5m)**\n"
                                            f"Сигнал: `{reason}`\n"
                                            f"Исход: **{side}**\n"
                                            f"Вход: "
                                            f"`${position['entry_price']}`\n"
                                            f"Объём: "
                                            f"`${position['amount']}` "
                                            "USDC\n"
                                            f"Комиссия 3%: "
                                            f"`${position['fee']}`"
                                        ),
                                        parse_mode="Markdown"
                                    )

                                except Exception as error:
                                    logging.error(
                                        "❌ Ошибка Telegram "
                                        f"уведомления: {error}"
                                    )

                        else:
                            logging.warning(
                                "⚠️ Сделка не открыта: "
                                f"{result}"
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
    """
    Синхронный вызов CLOB-клиента.
    Выполняется через asyncio.to_thread().
    """

    return clob_client.get_order_book(
        token_id
    )


def extract_best_prices(orderbook):
    """
    Возвращает лучший bid и ask.

    Лучший bid — максимальная цена покупки.
    Лучший ask — минимальная цена продажи.
    """

    best_bid = 0.0
    best_ask = 0.0

    if orderbook is None:
        return best_bid, best_ask

    bids = getattr(
        orderbook,
        "bids",
        None
    ) or []

    asks = getattr(
        orderbook,
        "asks",
        None
    ) or []

    valid_bids = []

    for level in bids:
        try:
            price = float(level.price)

            if 0 < price < 1:
                valid_bids.append(price)
        except (
            ValueError,
            TypeError,
            AttributeError
        ):
            continue

    valid_asks = []

    for level in asks:
        try:
            price = float(level.price)

            if 0 < price < 1:
                valid_asks.append(price)
        except (
            ValueError,
            TypeError,
            AttributeError
        ):
            continue

    if valid_bids:
        best_bid = max(valid_bids)

    if valid_asks:
        best_ask = min(valid_asks)

    return best_bid, best_ask


async def update_asset_orderbooks(
    asset,
    market_data
):
    """
    Получает стаканы токенов UP и DOWN.
    """

    up_token = market_data.get(
        "yes_token"
    )

    down_token = market_data.get(
        "no_token"
    )

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

        up_best_bid, up_best_ask = (
            extract_best_prices(
                up_book
            )
        )

        down_best_bid, down_best_ask = (
            extract_best_prices(
                down_book
            )
        )

        if up_best_bid > 0:
            market_data[
                "yes_best_bid"
            ] = up_best_bid

        if up_best_ask > 0:
            market_data[
                "yes_best_ask"
            ] = up_best_ask

            # Цена покупки UP
            market_data[
                "yes_price"
            ] = round(
                up_best_ask,
                4
            )

        if down_best_bid > 0:
            market_data[
                "no_best_bid"
            ] = down_best_bid

        if down_best_ask > 0:
            market_data[
                "no_best_ask"
            ] = down_best_ask

            # Цена покупки DOWN
            market_data[
                "no_price"
            ] = round(
                down_best_ask,
                4
            )

        # Резервный расчёт, если один из стаканов пуст.
        if (
            market_data.get(
                "yes_price",
                0
            ) <= 0
            and down_best_bid > 0
        ):
            market_data[
                "yes_price"
            ] = round(
                1 - down_best_bid,
                4
            )

        if (
            market_data.get(
                "no_price",
                0
            ) <= 0
            and up_best_bid > 0
        ):
            market_data[
                "no_price"
            ] = round(
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
            f"UP bid/ask="
            f"{up_best_bid}/{up_best_ask} | "
            f"DOWN bid/ask="
            f"{down_best_bid}/{down_best_ask}"
        )

    except Exception as error:
        logging.warning(
            f"⚠️ Ошибка стакана {asset}: "
            f"{error}"
        )


# ============================================================
# ЦИКЛ CLOB И РОТАЦИИ РЫНКОВ
# ============================================================

async def polymarket_clob_loop():
    """
    Обновляет текущие рынки и их стаканы.
    """

    last_market_update_time = 0.0

    while True:
        try:
            current_time = time.time()

            if (
                current_time
                - last_market_update_time
                >= MARKET_REFRESH_INTERVAL
            ):
                await fetch_5m_polymarket_ma

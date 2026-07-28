import os
import json
import time
import asyncio
import logging
from datetime import datetime
from collections import deque
import websockets
from aiohttp import web, ClientSession
from py_clob_client.client import ClobClient
from telebot.async_telebot import AsyncTeleBot
from telebot import types
from dotenv import load_dotenv

load_dotenv()

# --- Переменные окружения ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
ALLOWED_CHAT_ID_ENV = os.getenv("TELEGRAM_CHAT_ID")
INITIAL_BALANCE = float(os.getenv("INITIAL_BALANCE", "1000.0"))
PORT = int(os.getenv("PORT", "8080"))

ALLOWED_CHAT_ID = int(ALLOWED_CHAT_ID_ENV) if ALLOWED_CHAT_ID_ENV and ALLOWED_CHAT_ID_ENV.isdigit() else None

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

if not TELEGRAM_TOKEN:
    logging.critical("❌ ОШИБКА: TELEGRAM_TOKEN не найден!")

bot = AsyncTeleBot(TELEGRAM_TOKEN)
clob_client = ClobClient(host="https://clob.polymarket.com", chain_id=137)

# --- Настройки 5-минутной гибридной стратегии ---
TARGET_ASSETS = ["BTC", "ETH", "SOL", "DOGE", "BNB", "XRP", "HYPE"]
MOMENTUM_THRESHOLD = 0.20  # Порог импульса на Binance в % за 5 сек
LOOKBACK_SECONDS = 5       # Окно анализа тиков
MAKER_SPREAD_OFFSET = 0.01 # Отступ для лимитного ордера ($0.01)
TAKER_COMMISSION = 0.03    # Повышенная Taker-комиссия Polymarket на 5m рынках (~3%)

# Глобальное состояние
binance_prices = {asset: 0.0 for asset in TARGET_ASSETS}
binance_histories = {asset: deque() for asset in TARGET_ASSETS}
last_signal_times = {asset: 0 for asset in TARGET_ASSETS}

# ==========================================
# 📊 HYBRID 5M PAPER TRADER
# ==========================================
class HybridPaperTrader:
    def __init__(self, initial_balance: float):
        self.initial_balance = initial_balance
        self.balance = initial_balance
        self.positions = []           # Taker сделки
        self.active_limit_orders = {} # Maker ордера: {asset: [orders]}
        self.trade_history = []
        self.current_mode = "MAKER"
        self.auto_trade_enabled = True
        self.markets = {} # {asset: {"question": str, "yes_token": str, "no_token": str, "yes_price": float, "no_price": float}}

    def cancel_all_limits(self):
        """Мгновенная отмена всех Maker-лимиток при обнаружении импульса"""
        count = sum(len(v) for v in self.active_limit_orders.values())
        self.active_limit_orders.clear()
        return count

    def update_maker_orders(self, asset: str, best_bid: float, best_ask: float):
        """Режим MAKER: Моделирование установки лимиток во внутренний спред"""
        if best_bid <= 0 or best_ask <= 0:
            return
        
        my_bid = round(best_bid + MAKER_SPREAD_OFFSET, 2)
        my_ask = round(best_ask - MAKER_SPREAD_OFFSET, 2)

        if my_bid < my_ask:
            self.active_limit_orders[asset] = [
                {"side": "BUY_LIMIT", "price": my_bid, "amount": 20.0},
                {"side": "SELL_LIMIT", "price": my_ask, "amount": 20.0}
            ]

    def execute_taker_trade(self, asset: str, side: str, reason: str):
        """Режим TAKER: Вход по маркету с учетом 5m Taker-комиссии"""
        self.cancel_all_limits()
        
        m_data = self.markets.get(asset, {})
        ask_price = m_data.get('yes_price', 0.50)
        bid_price = round(1.0 - m_data.get('no_price', 0.50), 4)

        entry_price = ask_price if side == "YES" else round(1.0 - bid_price, 4)
        if entry_price <= 0 or entry_price >= 1.0:
            return False, "Некорректная цена в стакане"

        trade_amount = 25.0
        fee = trade_amount * TAKER_COMMISSION
        total_cost = trade_amount + fee

        if self.balance < total_cost:
            return False, "Недостаточно баланса"

        shares = trade_amount / entry_price
        self.balance -= total_cost

        pos = {
            "id": len(self.positions) + len(self.trade_history) + 1,
            "mode": "TAKER ⚡ 5M",
            "asset": asset,
            "question": m_data.get("question", asset)[:35] + "...",
            "side": side,
            "amount": trade_amount,
            "shares": round(shares, 2),
            "entry_price": entry_price,
            "fee": round(fee, 2),
            "reason": reason,
            "timestamp": datetime.now().strftime("%H:%M:%S")
        }
        self.positions.append(pos)
        return True, pos

    def get_stats(self):
        realized_pnl = sum(t.get('pnl', 0) for t in self.trade_history)
        unrealized_pnl = 0.0

        for pos in self.positions:
            asset = pos['asset']
            side = pos['side']
            m_data = self.markets.get(asset, {})
            curr_price = m_data.get('yes_price' if side == 'YES' else 'no_price', pos['entry_price'])
            val = pos['shares'] * curr_price
            unrealized_pnl += (val - pos['amount'])

        total_pnl = realized_pnl + unrealized_pnl
        equity = self.balance + sum(p['amount'] for p in self.positions) + unrealized_pnl
        roi = ((equity - self.initial_balance) / self.initial_balance) * 100 if self.initial_balance > 0 else 0
        total_limits = sum(len(v) for v in self.active_limit_orders.values())

        return {
            "cash_balance": round(self.balance, 2),
            "equity": round(equity, 2),
            "total_pnl": round(total_pnl, 2),
            "roi": round(roi, 2),
            "active_positions": len(self.positions),
            "active_limits": total_limits
        }

    def close_all_positions(self):
        closed = []
        for pos in list(self.positions):
            asset = pos['asset']
            side = pos['side']
            m_data = self.markets.get(asset, {})
            exit_price = m_data.get('yes_price' if side == 'YES' else 'no_price', pos['entry_price'])
            
            payout = pos['shares'] * exit_price
            pnl = payout - pos['amount']
            self.balance += payout

            record = {**pos, "exit_price": exit_price, "pnl": round(pnl, 2)}
            self.trade_history.append(record)
            closed.append(record)
            self.positions.remove(pos)
        return closed

    def reset_account(self):
        self.balance = self.initial_balance
        self.positions.clear()
        self.active_limit_orders.clear()
        self.trade_history.clear()

trader = HybridPaperTrader(INITIAL_BALANCE)

# ==========================================
# 🔍 АВТО-ПОИСК 5-МИНУТНЫХ РЫНКОВ (GAMMA API)
# ==========================================
async def fetch_5m_polymarket_markets():
    """Динамический поиск и переподключение к свежим 5-минутным рынкам"""
    url = "https://gamma-api.polymarket.com/events?limit=100&active=true&closed=false&order=startDate&ascending=false"
    async with ClientSession() as session:
        try:
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    events = await resp.json()
                    for event in events:
                        title = event.get('title', '').upper()
                        
                        # Фильтруем события с меткой 5m / 5-minute
                        if any(m in title for m in ["5M", "5 MINUTE", "5-MINUTE"]):
                            for asset in TARGET_ASSETS:
                                if asset in title:
                                    markets = event.get('markets', [])
                                    if markets:
                                        m = markets[0]
                                        clob_ids = m.get('clobTokenIds')
                                        if clob_ids and len(clob_ids) >= 2:
                                            trader.markets[asset] = {
                                                "question": m.get('question', title),
                                                "yes_token": clob_ids[0],
                                                "no_token": clob_ids[1],
                                                "yes_price": trader.markets.get(asset, {}).get('yes_price', 0.50),
                                                "no_price": trader.markets.get(asset, {}).get('no_price', 0.50)
                                            }
                    logging.info(f"🔄 Активные 5m рынки обновлены: {list(trader.markets.keys())}")
        except Exception as e:
            logging.error(f"❌ Ошибка Gamma API 5m: {e}")

# ==========================================
# ⚡ BINANCE MULTI-STREAM WEBSOCKET
# ==========================================
async def binance_ws_loop():
    """Слушает тики по монетам в реальном времени"""
    stream_assets = [a for a in TARGET_ASSETS if a != "HYPE"]
    streams = "/".join([f"{asset.lower()}usdt@trade" for asset in stream_assets])
    uri = f"wss://stream.binance.com:9443/stream?streams={streams}"

    while True:
        try:
            async with websockets.connect(uri) as ws:
                logging.info("🌐 Binance Multi-Stream WebSocket подключен!")
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if 'data' not in data:
                        continue
                    
                    trade_data = data['data']
                    symbol = trade_data['s'].replace("USDT", "")
                    price = float(trade_data['p'])
                    now = time.time()

                    binance_prices[symbol] = price
                    history = binance_histories[symbol]
                    history.append((now, price))

                    while history and (now - history[0][0]) > LOOKBACK_SECONDS:
                        history.popleft()

                    # Проверка условий TAKER-снайпинга
                    if len(history) > 1 and trader.auto_trade_enabled:
                        old_price = history[0][1]
                        pct_change = ((price - old_price) / old_price) * 100

                        if abs(pct_change) >= MOMENTUM_THRESHOLD and (now - last_signal_times[symbol]) > 10:
                            if symbol in trader.markets:
                                last_signal_times[symbol] = now
                                trader.current_mode = "TAKER"

                                side = "YES" if pct_change > 0 else "NO"
                                reason = f"Binance {symbol} {pct_change:+.2f}% за {LOOKBACK_SECONDS}s"

                                logging.info(f"🚨 5M TAKER СИГНАЛ: {reason}")
                                success, pos = trader.execute_taker_trade(symbol, side, reason)

                                if success and ALLOWED_CHAT_ID:
                                    await bot.send_message(
                                        ALLOWED_CHAT_ID,
                                        f"⚡ **[HYBRID 5M EXECUTION]**\n"
                                        f"Рынок: **{symbol} (5m)** | Сигнал: `{reason}`\n"
                                        f"Исход: **{side}** | Вход: `${pos['entry_price']}`\n"
                                        f"Объем: `${pos['amount']}` USDC (Комиссия 3%: `${pos['fee']}`)",
                                        parse_mode="Markdown"
                                    )

                                await asyncio.sleep(2)
                                trader.current_mode = "MAKER"

        except Exception as e:
            logging.error(f"❌ Ошибка Binance WS: {e}")
            await asyncio.sleep(3)

# ==========================================
# 📖 ЧТЕНИЕ СТАКАНОФ И ВЕДЕНИЕ MAKER-РЕЖИМА
# ==========================================
async def polymarket_clob_loop():
    last_update_time = 0
    while True:
        try:
            now = time.time()
            # Обновляем 5-минутные контракты каждые 30 секунд
            if now - last_update_time > 30:
                await fetch_5m_polymarket_markets()
                last_update_time = now

            for asset, m_data in list(trader.markets.items()):
                try:
                    orderbook = clob_client.get_order_book(m_data['yes_token'])
                    best_ask = float(orderbook.asks[0].price) if orderbook.asks else 0.0
                    best_bid = float(orderbook.bids[0].price) if orderbook.bids else 0.0

                    if best_ask > 0:
                        m_data['yes_price'] = best_ask
                        m_data['no_price'] = round(1.0 - best_bid, 4)

                        if trader.current_mode == "MAKER":
                            trader.update_maker_orders(asset, best_bid, best_ask)
                except Exception:
                    pass

        except Exception as e:
            logging.error(f"Ошибка в цикле CLOB: {e}")

        await asyncio.sleep(2)

# ==========================================
# 🤖 ТЕЛЕГРАМ ИНТЕРФЕЙС
# ==========================================
def get_main_keyboard():
    markup = types.InlineKeyboardMarkup(row_width=2)
    btn_pnl = types.InlineKeyboardButton("📊 5m Рынки & PnL", callback_data="stats")
    btn_pos = types.InlineKeyboardButton("💼 Позиции", callback_data="positions")
    btn_close = types.InlineKeyboardButton("🧹 Закрыть все", callback_data="close_all")
    btn_toggle = types.InlineKeyboardButton(
        f"🤖 Авто-трейд: {'ВКЛ 🟢' if trader.auto_trade_enabled else 'ВЫКЛ 🔴'}", 
        callback_data="toggle_auto"
    )
    btn_reset = types.InlineKeyboardButton("🔄 Сброс баланса", callback_data="reset")
    markup.add(btn_pnl, btn_pos)
    markup.add(btn_close, btn_toggle)
    markup.add(btn_reset)
    return markup

def is_authorized(chat_id):
    global ALLOWED_CHAT_ID
    if ALLOWED_CHAT_ID is None:
        ALLOWED_CHAT_ID = chat_id
        return True
    return chat_id == ALLOWED_CHAT_ID

@bot.message_handler(commands=['start', 'menu'])
async def send_welcome(message):
    if not is_authorized(message.chat.id):
        await bot.send_message(message.chat.id, "⛔ Доступ ограничен.")
        return

    text = (
        "🤖 **Polymarket 5m Hybrid Bot**\n\n"
        f"• **Мониторинг 5m рынков:** `{', '.join(TARGET_ASSETS)}`\n"
        "• **Binance Stream:** Multi-WebSocket (BTC/ETH/SOL/DOGE/BNB/XRP)\n"
        "• **Авто-ротация:** Ротация 5-минутных контрактов каждые 30 сек\n"
        "• **MAKER:** Пассивный сбор спреда в спокойный рынок\n"
        "• **TAKER:** Снайпинг импульсов $\ge 0.20\%$ (учитывает 3% комиссию)"
    )
    await bot.send_message(message.chat.id, text, parse_mode="Markdown", reply_markup=get_main_keyboard())

@bot.callback_query_handler(func=lambda call: True)
async def handle_callbacks(call):
    if not is_authorized(call.message.chat.id):
        return

    if call.data == "stats":
        s = trader.get_stats()
        pnl_icon = "🟩" if s['total_pnl'] >= 0 else "🟥"

        prices_str = ""
        for asset in TARGET_ASSETS:
            b_price = binance_prices.get(asset, 0.0)
            m_data = trader.markets.get(asset, {})
            y_price = m_data.get('yes_price', 0.0)
            n_price = m_data.get('no_price', 0.0)
            prices_str += f"• **{asset} (5m):** Spot `${b_price:,.2f}` | YES `${y_price}` / NO `${n_price}`\n"

        text = (
            f"📊 **5-МИНУТНЫЕ РЫНКИ & PnL**\n"
            f"───────────────────\n"
            f"{prices_str}\n"
            f"📍 **Режим:** `{trader.current_mode}`\n"
            f"💵 **Депозит:** `${s['cash_balance']}` USDC\n"
            f"💎 **Equity:** `${s['equity']}` USDC\n"
            f"{pnl_icon} **PnL:** `${s['total_pnl']}` USDC ({s['roi']}%)\n"
            f"📌 **Maker-лимитки:** `{s['active_limits']}` шт.\n"
            f"💼 **Taker-позиции:** `{s['active_positions']}` шт."
        )
        await bot.edit_message_text(text, call.message.chat.id, call.message.message_id, 
                                   parse_mode="Markdown", reply_markup=get_main_keyboard())

    elif call.data == "positions":
        if not trader.positions:
            await bot.answer_callback_query(call.id, "📭 Нет активных позиций!")
            return

        text = "💼 **ОТКРЫТЫЕ 5M ПОЗИЦИИ:**\n\n"
        for p in trader.positions:
            m_data = trader.markets.get(p['asset'], {})
            curr_p = m_data.get('yes_price' if p['side'] == 'YES' else 'no_price', p['entry_price'])
            pnl = (p['shares'] * curr_p) - p['amount']
            icon = "🟢" if pnl >= 0 else "🔴"
            text += (
                f"🔹 **[{p['asset']}] {p['side']}** ({p['mode']})\n"
                f"└ Сигнал: `{p['reason']}`\n"
                f"└ Вход: `${p['entry_price']}` | Текущая: `${curr_p}`\n"
                f"└ PnL: {icon} `${round(pnl, 2)}` USDC\n\n"
            )
        await bot.send_message(call.message.chat.id, text, parse_mode="Markdown")
        await bot.answer_callback_query(call.id)

    elif call.data == "close_all":
        closed = trader.close_all_positions()
        await bot.send_message(call.message.chat.id, f"🧹 Закрыто 5m позиций: {len(closed)}.")

    elif call.data == "toggle_auto":
        trader.auto_trade_enabled = not trader.auto_trade_enabled
        await bot.answer_callback_query(call.id, f"Авто-трейдинг: {trader.auto_trade_enabled}")
        await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=get_main_keyboard())

    elif call.data == "reset":
        trader.reset_account()
        await bot.answer_callback_query(call.id, "🔄 Баланс сброшен!", show_alert=True)

# ==========================================
# 🌐 WEB SERVER & MAIN
# ==========================================
async def handle_ping(request):
    return web.Response(text="5m Hybrid Bot OK")

async def main():
    app = web.Application()
    app.router.add_get('/', handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()

    try:
        bot_user = await bot.get_me()
        logging.info(f"✅ Успешный старт бота: @{bot_user.username}")
    except Exception as e:
        logging.error(f"❌ Ошибка авторизации: {e}")

    await asyncio.gather(
        binance_ws_loop(),
        polymarket_clob_loop(),
        bot.polling(non_stop=True)
    )

if __name__ == "__main__":
    asyncio.run(main())
    

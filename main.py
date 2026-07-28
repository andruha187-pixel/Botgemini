import os
import asyncio
import json
import logging
from datetime import datetime
from aiohttp import web
import websockets
from telebot.async_telebot import AsyncTeleBot
from telebot import types
from dotenv import load_dotenv

load_dotenv()

# Поддержка различных вариантов названия переменной токена
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
ALLOWED_CHAT_ID_ENV = os.getenv("TELEGRAM_CHAT_ID")
INITIAL_BALANCE = float(os.getenv("INITIAL_BALANCE", "1000.0"))
PORT = int(os.getenv("PORT", "8080"))

# Динамический Chat ID (если не был задан заранее)
ALLOWED_CHAT_ID = int(ALLOWED_CHAT_ID_ENV) if ALLOWED_CHAT_ID_ENV and ALLOWED_CHAT_ID_ENV.isdigit() else None

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

if not TELEGRAM_TOKEN:
    logging.critical("❌ ОШИБКА: TELEGRAM_TOKEN не найден в переменных окружения! Укажите его в панеле Render.")

# Инициализация асинхронного бота
bot = AsyncTeleBot(TELEGRAM_TOKEN)

# ==========================================
# 📊 ГИБРИДНЫЙ PAPER TRADER (LIMIT + MARKET)
# ==========================================
class HybridPaperTrader:
    def __init__(self, initial_balance: float):
        self.initial_balance = initial_balance
        self.balance = initial_balance
        self.positions = []       # Активные позиции
        self.pending_limits = []   # Ожидающие лимитные ордера
        self.trade_history = []    # История закрытых сделок
        self.current_btc_price = 0.0
        self.auto_trade_enabled = True
        self.liquidity_rewards = 0.0  # Заработанные ребейты/очки

    def get_stats(self):
        realized_pnl = sum(t['pnl'] for t in self.trade_history) + self.liquidity_rewards
        
        unrealized_pnl = 0.0
        for pos in self.positions:
            price_diff = (self.current_btc_price - pos['entry_btc_price']) / pos['entry_btc_price']
            if pos['side'] == 'YES':
                current_val = pos['amount'] * (1 + price_diff * 2)
            else:
                current_val = pos['amount'] * (1 - price_diff * 2)
            unrealized_pnl += (current_val - pos['amount'])

        total_pnl = realized_pnl + unrealized_pnl
        equity = self.balance + sum(p['amount'] for p in self.positions) + unrealized_pnl
        roi = ((equity - self.initial_balance) / self.initial_balance) * 100
        
        wins = sum(1 for t in self.trade_history if t['pnl'] > 0)
        total_trades = len(self.trade_history)
        win_rate = (wins / total_trades * 100) if total_trades > 0 else 0.0

        return {
            "cash_balance": round(self.balance, 2),
            "equity": round(equity, 2),
            "realized_pnl": round(realized_pnl, 2),
            "unrealized_pnl": round(unrealized_pnl, 2),
            "total_pnl": round(total_pnl, 2),
            "rewards": round(self.liquidity_rewards, 2),
            "roi": round(roi, 2),
            "total_trades": total_trades,
            "win_rate": round(win_rate, 1),
            "active_positions": len(self.positions),
            "pending_limits": len(self.pending_limits),
            "btc_price": round(self.current_btc_price, 2)
        }

    # Исполнение Маркет-ордера (Taker, комиссия 1.5%)
    def execute_market_order(self, market_name: str, side: str, amount: float):
        fee = amount * 0.015  # 1.5% Taker Fee
        total_cost = amount + fee

        if self.balance < total_cost:
            return False, "Недостаточно баланса с учётом комиссии (1.5%)"

        self.balance -= total_cost
        pos = {
            "id": len(self.positions) + len(self.trade_history) + 1,
            "type": "MARKET (Taker)",
            "market": market_name,
            "side": side,
            "amount": amount,
            "fee_paid": round(fee, 2),
            "entry_btc_price": self.current_btc_price,
            "timestamp": datetime.now().strftime("%H:%M:%S")
        }
        self.positions.append(pos)
        return True, pos

    # Выставление Лимитного ордера (Maker, 0% комиссии)
    def place_limit_order(self, market_name: str, side: str, amount: float, target_btc_price: float):
        if self.balance < amount:
            return False, "Недостаточно баланса"

        self.balance -= amount
        limit_order = {
            "id": len(self.pending_limits) + 1,
            "type": "LIMIT (Maker)",
            "market": market_name,
            "side": side,
            "amount": amount,
            "target_btc_price": target_btc_price,
            "timestamp": datetime.now().strftime("%H:%M:%S")
        }
        self.pending_limits.append(limit_order)
        return True, limit_order

    # Проверка исполнения Лимитных ордеров при движении цены
    def check_limit_fills(self):
        filled_events = []
        for order in list(self.pending_limits):
            is_filled = False
            if order['side'] == 'YES' and self.current_btc_price <= order['target_btc_price']:
                is_filled = True
            elif order['side'] == 'NO' and self.current_btc_price >= order['target_btc_price']:
                is_filled = True

            if is_filled:
                pos = {
                    "id": len(self.positions) + len(self.trade_history) + 1,
                    "type": "LIMIT (Maker)",
                    "market": order['market'],
                    "side": order['side'],
                    "amount": order['amount'],
                    "fee_paid": 0.0,
                    "entry_btc_price": self.current_btc_price,
                    "timestamp": datetime.now().strftime("%H:%M:%S")
                }
                self.positions.append(pos)
                self.pending_limits.remove(order)
                self.liquidity_rewards += 0.10  # Maker-бонус
                filled_events.append(pos)
        return filled_events

    def close_all_positions(self):
        closed = []
        for pos in list(self.positions):
            price_diff = (self.current_btc_price - pos['entry_btc_price']) / pos['entry_btc_price']
            if pos['side'] == 'YES':
                pnl = pos['amount'] * (price_diff * 2)
            else:
                pnl = pos['amount'] * (-price_diff * 2)
            
            pnl = round(pnl, 2)
            self.balance += (pos['amount'] + pnl)
            
            trade_record = {
                **pos,
                "exit_btc_price": self.current_btc_price,
                "pnl": pnl,
                "close_time": datetime.now().strftime("%H:%M:%S")
            }
            self.trade_history.append(trade_record)
            closed.append(trade_record)
            self.positions.remove(pos)
        return closed

    def reset_account(self):
        self.balance = self.initial_balance
        self.positions.clear()
        self.pending_limits.clear()
        self.trade_history.clear()
        self.liquidity_rewards = 0.0

trader = HybridPaperTrader(INITIAL_BALANCE)

# ==========================================
# 🤖 ТЕЛЕГРАМ ИНТЕРФЕЙС
# ==========================================
def get_main_keyboard():
    markup = types.InlineKeyboardMarkup(row_width=2)
    btn_pnl = types.InlineKeyboardButton("📈 PnL & Гибрид Стата", callback_data="stats")
    btn_pos = types.InlineKeyboardButton("💼 Открытые Сделки", callback_data="positions")
    btn_trade_mkt = types.InlineKeyboardButton("⚡ Taker (Market)", callback_data="trade_market")
    btn_trade_lmt = types.InlineKeyboardButton("🎯 Maker (Limit)", callback_data="trade_limit")
    btn_close = types.InlineKeyboardButton("❌ Закрыть все", callback_data="close_all")
    btn_toggle = types.InlineKeyboardButton(
        f"🤖 Авто-гибрид: {'ВКЛ 🟢' if trader.auto_trade_enabled else 'ВЫКЛ 🔴'}", 
        callback_data="toggle_auto"
    )
    btn_reset = types.InlineKeyboardButton("🔄 Сбросить баланс", callback_data="reset")
    markup.add(btn_pnl, btn_pos)
    markup.add(btn_trade_mkt, btn_trade_lmt)
    markup.add(btn_close, btn_toggle)
    markup.add(btn_reset)
    return markup

def is_authorized(chat_id):
    global ALLOWED_CHAT_ID
    if ALLOWED_CHAT_ID is None:
        ALLOWED_CHAT_ID = chat_id
        logging.info(f"🔑 Зафиксирован владелец бота: Chat ID = {chat_id}")
        return True
    return chat_id == ALLOWED_CHAT_ID

@bot.message_handler(commands=['start', 'menu'])
async def send_welcome(message):
    if not is_authorized(message.chat.id):
        await bot.send_message(message.chat.id, "⛔ Доступ ограничен владельцем бота.")
        return

    text = (
        "⚙️ **Polymarket Hybrid Bot (Limit + Market)**\n\n"
        "• **Maker-ордера:** 0% комиссия + начисление $0.10 ребейтов за ликвидность.\n"
        "• **Taker-ордера:** 1.5% комиссия при сильных аномальных импульсах.\n\n"
        f"💰 **Стартовый капитал:** `${INITIAL_BALANCE}` USDT"
    )
    await bot.send_message(message.chat.id, text, parse_mode="Markdown", reply_markup=get_main_keyboard())

@bot.callback_query_handler(func=lambda call: True)
async def handle_callbacks(call):
    if not is_authorized(call.message.chat.id):
        return

    if call.data == "stats":
        s = trader.get_stats()
        pnl_icon = "🟩" if s['total_pnl'] >= 0 else "🟥"
        text = (
            f"📊 **ГИБРИДНАЯ СТАТИСТИКА & PnL**\n"
            f"───────────────────\n"
            f"🪙 **BTC Price:** `${s['btc_price']}` USDT\n\n"
            f"💵 **Депозит:** `${s['cash_balance']}` USDT\n"
            f"💎 **Equity (Всего):** `${s['equity']}` USDT\n"
            f"{pnl_icon} **Общий PnL:** `${s['total_pnl']}` USDT ({s['roi']}%)\n"
            f"├ **Реализованный:** `${s['realized_pnl']}` USDT\n"
            f"├ **Нереализованный:** `${s['unrealized_pnl']}` USDT\n"
            f"└ **Maker-Ребейты:** `${s['rewards']}` USDT\n\n"
            f"🎯 **Win Rate:** `{s['win_rate']}%` ({s['total_trades']} сделок)\n"
            f"📌 **Активные позиции:** `{s['active_positions']}`\n"
            f"⏳ **Ожидающие лимитки:** `{s['pending_limits']}`"
        )
        await bot.edit_message_text(text, call.message.chat.id, call.message.message_id, 
                                   parse_mode="Markdown", reply_markup=get_main_keyboard())

    elif call.data == "positions":
        if not trader.positions and not trader.pending_limits:
            await bot.answer_callback_query(call.id, "📭 Нет активных позиций или ордеров!")
            return
        
        text = "💼 **АКТИВНЫЕ ПОЗИЦИИ И ЛИМИТКИ:**\n\n"
        for p in trader.positions:
            price_diff = (trader.current_btc_price - p['entry_btc_price']) / p['entry_btc_price']
            pnl = p['amount'] * (price_diff * 2 if p['side'] == 'YES' else -price_diff * 2)
            icon = "🟢" if pnl >= 0 else "🔴"
            text += (
                f"🔹 **#{p['id']} [{p['type']}]** -> {p['side']}\n"
                f"└ Объем: `${p['amount']}` | Вход BTC: `${p['entry_btc_price']}` | Fee: `${p['fee_paid']}`\n"
                f"└ PnL: {icon} `${round(pnl, 2)}` USDT\n\n"
            )
        for l in trader.pending_limits:
            text += f"⏳ **[LIMIT PENDING]** {l['side']} на `${l['amount']}` | Цель BTC: `${l['target_btc_price']}`\n"

        await bot.send_message(call.message.chat.id, text, parse_mode="Markdown")
        await bot.answer_callback_query(call.id)

    elif call.data == "trade_market":
        success, pos = trader.execute_market_order("BTC Market Surge", "YES", 50.0)
        if success:
            await bot.send_message(
                call.message.chat.id, 
                f"⚡ **Taker-сделка исполнена!**\nТип: Market | Направление: **YES**\nКомиссия (1.5%): `${pos['fee_paid']}` USDT",
                parse_mode="Markdown"
            )
        else:
            await bot.answer_callback_query(call.id, pos, show_alert=True)

    elif call.data == "trade_limit":
        target = trader.current_btc_price * 0.9995
        success, order = trader.place_limit_order("BTC Limit Grid", "YES", 50.0, round(target, 2))
        if success:
            await bot.send_message(
                call.message.chat.id, 
                f"🎯 **Maker-ордер выставлен!**\nЦель BTC: `${order['target_btc_price']}`\nКомиссия: **0% (+Ребейт)**",
                parse_mode="Markdown"
            )
        else:
            await bot.answer_callback_query(call.id, order, show_alert=True)

    elif call.data == "close_all":
        closed = trader.close_all_positions()
        await bot.send_message(call.message.chat.id, f"🧹 Закрыто позиций: {len(closed)}.")

    elif call.data == "toggle_auto":
        trader.auto_trade_enabled = not trader.auto_trade_enabled
        await bot.answer_callback_query(call.id, f"Авто-трейдинг: {trader.auto_trade_enabled}")
        await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=get_main_keyboard())

    elif call.data == "reset":
        trader.reset_account()
        await bot.answer_callback_query(call.id, "🔄 Депозит сброшен до $1000!", show_alert=True)

# ==========================================
# ⚡ BINANCE WEBSOCKET + ГИБРИДНЫЙ АЛГОРИТМ
# ==========================================
async def binance_websocket_loop():
    url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    last_price = 0.0

    while True:
        try:
            async with websockets.connect(url) as ws:
                logging.info("Подключено к Binance WS (Гибридный режим)")
                async for message in ws:
                    data = json.loads(message)
                    current_price = float(data['p'])
                    trader.current_btc_price = current_price

                    # Проверяем исполнение лимитных Maker-ордеров
                    fills = trader.check_limit_fills()
                    if ALLOWED_CHAT_ID:
                        for f in fills:
                            await bot.send_message(
                                ALLOWED_CHAT_ID,
                                f"🎯 **[MAKER FILL] Лимитный ордер исполнен!**\nВход по BTC: `${f['entry_btc_price']}`\nНачислено ребейтов: `+$0.10`",
                                parse_mode="Markdown"
                            )

                    # Логика Авто-трейдинга
                    if trader.auto_trade_enabled and last_price > 0:
                        change_pct = ((current_price - last_price) / last_price) * 100

                        # 🔥 ИМПУЛЬС (> 0.20%) -> Taker (Market)
                        if abs(change_pct) >= 0.20 and trader.balance >= 50:
                            side = "YES" if change_pct > 0 else "NO"
                            success, pos = trader.execute_market_order("BTC Pulse", side, 50.0)
                            if success and ALLOWED_CHAT_ID:
                                last_price = current_price
                                await bot.send_message(
                                    ALLOWED_CHAT_ID,
                                    f"🚨 **[TAKER SIGNAL] Сильный импульс `{round(change_pct, 2)}%`**\n"
                                    f"Заход по Маркету [{side}] | Списана комиссия 1.5%",
                                    parse_mode="Markdown"
                                )

                        # 📈 СПОКОЙНЫЙ РЫНОК -> Maker (Limit)
                        elif 0.05 <= abs(change_pct) < 0.20 and len(trader.pending_limits) < 2 and trader.balance >= 50:
                            side = "YES" if change_pct > 0 else "NO"
                            target_price = current_price * (0.9997 if side == "YES" else 1.0003)
                            success, order = trader.place_limit_order("BTC Grid", side, 50.0, round(target_price, 2))
                            if success and ALLOWED_CHAT_ID:
                                last_price = current_price
                                await bot.send_message(
                                    ALLOWED_CHAT_ID,
                                    f"⏳ **[MAKER GRID] Выставлен Лимит [{side}]**\n"
                                    f"Цель BTC: `${order['target_btc_price']}` (0% комиссия)",
                                    parse_mode="Markdown"
                                )

                    if last_price == 0.0:
                        last_price = current_price

        except Exception as e:
            logging.error(f"WS Error: {e}")
            await asyncio.sleep(5)

async def handle_ping(request):
    return web.Response(text="Hybrid Bot OK")

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()

async def main():
    # 1. Предварительная загрузка профиля бота (Решает ошибку AttributeError)
    try:
        bot_user = await bot.get_me()
        logging.info(f"✅ Успешная авторизация бота: @{bot_user.username}")
    except Exception as e:
        logging.error(f"❌ Не удалось авторизоваться в Telegram. Проверьте TELEGRAM_TOKEN в Render. Ошибка: {e}")
        return

    # 2. Параллельный запуск веб-сервера, WebSocket и бота
    await asyncio.gather(
        start_web_server(),
        binance_websocket_loop(),
        bot.polling(non_stop=True)
    )

if __name__ == "__main__":
    asyncio.run(main())
        

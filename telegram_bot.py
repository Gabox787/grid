"""
telegram_bot.py — Telegram-бот для мониторинга и управления Grid-ботом.

Работает поверх aiogram 3.x как отдельная asyncio-задача рядом с торговым
циклом (main.py поднимает её через create_task, если заданы
TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID). Owner-only: отвечает только
владельцу (TELEGRAM_CHAT_ID), все остальные чаты игнорируются — тот же
паттерн, что и в прошлых ботах.

Push-уведомления о входе/выходе в сделку шлёт сам grid_bot.py через
GridBot._notify (простой POST к Bot API) — это не пересекается с polling
здесь, оба канала используют один и тот же токен независимо.
"""

import csv
import io
import logging
import os
import tempfile
import time

logger = logging.getLogger("telegram_bot")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


def enabled() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def _fmt_money(x, currency="") -> str:
    if x is None:
        return "—"
    return f"{x:,.2f} {currency}".strip()


def _fmt_uptime(seconds: float) -> str:
    seconds = int(seconds)
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}д")
    if hours or days:
        parts.append(f"{hours}ч")
    parts.append(f"{minutes}м")
    return " ".join(parts)


HELP_TEXT = (
    "🤖 Grid Bot — команды:\n"
    "/status — общая сводка (цена, статус, бюджет, профит)\n"
    "/grid — какие уровни сетки заняты, какие свободны\n"
    "/trades [N] — последние N сделок (по умолчанию 10)\n"
    "/pnl — профит, комиссии, ROI, винрейт\n"
    "/fees — суммарные комиссии\n"
    "/uptime — сколько времени работает бот\n"
    "/pause — остановить новые входы (открытые позиции не трогает)\n"
    "/resume — снять паузу\n"
    "/export — выгрузить весь журнал сделок в CSV"
)


async def create_and_run(grid_bot, db_module):
    """Регистрирует хендлеры команд и запускает long-polling. No-op, если бот не настроен."""
    if not enabled():
        logger.info("TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID не заданы — Telegram-бот управления отключён")
        return

    from aiogram import Bot, Dispatcher
    from aiogram.filters import Command
    from aiogram.types import Message, FSInputFile

    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    dp = Dispatcher()
    owner_id = int(TELEGRAM_CHAT_ID)

    def _is_owner(message: Message) -> bool:
        return message.chat.id == owner_id

    async def _guard(message: Message) -> bool:
        """True — можно выполнять команду. False — уже ответили (не владелец, либо бот ещё стартует/упал)."""
        if not _is_owner(message):
            return False
        if not grid_bot.ready.is_set():
            if grid_bot.state.status == "error":
                await message.answer(
                    f"❌ Бот не смог инициализировать сетку и стоит с ошибкой:\n\n"
                    f"{grid_bot.state.last_error or 'неизвестная ошибка'}\n\n"
                    f"Смотри Render → Logs для полного трейсбэка. Бот НЕ перезапускается сам — "
                    f"нужен redeploy после исправления."
                )
            else:
                await message.answer("⏳ Бот ещё запускается (инициализация сетки / восстановление из БД). Попробуй через 10-15 секунд.")
            return False
        return True

    @dp.message(Command("start", "help"))
    async def cmd_help(message: Message):
        if not _is_owner(message):
            return
        await message.answer(HELP_TEXT)

    @dp.message(Command("status"))
    async def cmd_status(message: Message):
        if not await _guard(message):
            return
        s = grid_bot.get_stats()
        c = s["capital"]
        safety = s["safety"]
        q = grid_bot.quote_currency
        paused_tag = " ⏸ ПАУЗА" if s["paused"] else ""

        text = (
            f"📊 {s['symbol']} · {s['status'].upper()}{paused_tag}\n"
            f"Цена: {s['current_price']}\n"
            f"Аптайм: {_fmt_uptime(s['uptime_seconds'])}\n\n"
            f"Депозит: {_fmt_money(c['deposit_usdt'], q)}\n"
            f"Core / Extension: {_fmt_money(c['core_capital'], q)} / {_fmt_money(c['extension_capital'], q)}\n"
            f"Резерв (не трогаем): {_fmt_money(c['reserve_usdt'], q)}\n"
            f"Заблокировано в Buy: {_fmt_money(c['locked_in_buys'], q)}\n\n"
            f"Сделок: {s['trades_completed']}\n"
            f"Профит (брутто): {s['total_profit']:.4f} {q}\n"
            f"Комиссии: ~{s['total_fees']:.4f} {q}\n"
            f"Профит (нетто): {s['net_profit']:.4f} {q}\n"
        )
        if safety["budget_exhausted"]:
            text += "\n🛑 Бюджет (core+extension) исчерпан, ждём восстановления цены"
        elif safety["extension_active"]:
            text += "\n⚠️ Цена ниже LOWER_BOUND — работаем в резервной зоне"
        if s["last_error"]:
            text += f"\n\n⚠ Последняя ошибка: {s['last_error']}"

        await message.answer(text)

    @dp.message(Command("grid"))
    async def cmd_grid(message: Message):
        if not await _guard(message):
            return
        q = grid_bot.quote_currency
        b = grid_bot.base_currency
        fee_rate = grid_bot.fee_rate_pct

        lines = []
        free_count = 0
        levels = grid_bot.levels

        for i, lvl in enumerate(levels):
            if not lvl.side:
                free_count += 1
                continue

            amount = lvl.amount or 0

            if lvl.side == "buy":
                entry_price = lvl.price  # ещё не сработал — это и есть цена срабатывания
                next_lvl = levels[i + 1] if i + 1 < len(levels) else None
                exit_price = next_lvl.price if next_lvl else None
                trigger_note = f"BUY @ {entry_price}"
            else:
                entry_price = lvl.entry_price
                exit_price = lvl.price  # это и есть цена срабатывания для sell
                trigger_note = f"SELL @ {exit_price}"

            sum_str = f"~{entry_price * amount:.2f} {q}" if entry_price else "—"

            if entry_price and exit_price:
                profit = (exit_price - entry_price) * amount
                profit_pct = (exit_price - entry_price) / entry_price * 100
                fee = fee_rate * amount * (entry_price + exit_price)
                profit_str = f"~{profit:+.2f} {q} ({profit_pct:+.2f}%)"
                fee_str = f"~{fee:.2f} {q}"
            else:
                profit_str = "— (нет уровня для цикла)"
                fee_str = "—"

            emoji = "🟢" if lvl.side == "buy" else "🔴"
            lines.append(
                f"{emoji} Сетка #{i}\n"
                f"Сработает: {trigger_note}\n"
                f"Вход: {entry_price if entry_price else '—'} → Выход: {exit_price if exit_price else '—'}\n"
                f"Объём: {amount} {b} ({sum_str})\n"
                f"Профит: {profit_str}\n"
                f"Комиссия: {fee_str}"
            )

        text = "\n\n".join(lines) if lines else "Нет открытых ордеров"
        text += f"\n\nСвободных уровней: {free_count} / {len(levels)}"

        # Telegram режет сообщения длиннее ~4096 символов — на всякий случай бьём на части
        for chunk_start in range(0, len(text), 3800):
            await message.answer(text[chunk_start:chunk_start + 3800])

    @dp.message(Command("trades"))
    async def cmd_trades(message: Message):
        if not await _guard(message):
            return
        parts = message.text.split()
        limit = 10
        if len(parts) > 1 and parts[1].isdigit():
            limit = min(int(parts[1]), 50)

        trades = await db_module.fetch_trades(limit)
        if not trades:
            await message.answer("Сделок пока нет")
            return

        lines = []
        for t in trades:
            emoji = "🟢" if t["side"] == "buy" else "🔴"
            pnl = f" P&L {t['profit']:+.4f}" if t.get("profit") is not None else ""
            fee = f" fee {t['fee']:.4f}" if t.get("fee") is not None else ""
            lines.append(
                f"{emoji} #{t['level_index']} {t['side'].upper()} @ {t['price']} × {t['amount']}"
                f"{pnl}{fee}\n   {t['created_at']}"
            )
        await message.answer("\n\n".join(lines))

    @dp.message(Command("pnl", "roi"))
    async def cmd_pnl(message: Message):
        if not await _guard(message):
            return
        stats = await db_module.fetch_trade_stats()
        s = grid_bot.get_stats()
        q = grid_bot.quote_currency
        win_rate = f"{stats['win_rate']}%" if stats["win_rate"] is not None else "—"
        avg = f"{stats['avg_profit_per_cycle']:.4f}" if stats["avg_profit_per_cycle"] is not None else "—"
        roi = f"{s['roi_pct']:+.2f}%" if s["roi_pct"] is not None else "—"

        text = (
            f"💰 Профит (брутто): {s['total_profit']:.4f} {q}\n"
            f"Комиссии: {s['total_fees']:.4f} {q}\n"
            f"Профит (нетто): {s['net_profit']:.4f} {q}\n"
            f"ROI от депозита: {roi}\n\n"
            f"Закрытых циклов: {stats['closed_cycles']}\n"
            f"Винрейт: {win_rate}\n"
            f"Средний профит/цикл: {avg} {q}"
        )
        await message.answer(text)

    @dp.message(Command("fees"))
    async def cmd_fees(message: Message):
        if not await _guard(message):
            return
        s = grid_bot.get_stats()
        await message.answer(f"Комиссии за всё время: ~{s['total_fees']:.4f} {grid_bot.quote_currency}")

    @dp.message(Command("uptime"))
    async def cmd_uptime(message: Message):
        if not await _guard(message):
            return
        s = grid_bot.get_stats()
        await message.answer(f"⏱ Аптайм: {_fmt_uptime(s['uptime_seconds'])}")

    @dp.message(Command("pause"))
    async def cmd_pause(message: Message):
        if not await _guard(message):
            return
        grid_bot.paused = True
        await message.answer("⏸ Новые входы остановлены. Открытые ордера и позиции не трогаю.")

    @dp.message(Command("resume"))
    async def cmd_resume(message: Message):
        if not await _guard(message):
            return
        grid_bot.paused = False
        await message.answer("▶️ Возобновляю открытие новых ордеров по мере филлов.")

    @dp.message(Command("export"))
    async def cmd_export(message: Message):
        if not await _guard(message):
            return
        trades = await db_module.fetch_trades(limit=100000)
        if not trades:
            await message.answer("Сделок пока нет — экспортировать нечего")
            return

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["id", "symbol", "side", "level_index", "price", "amount", "profit", "fee", "dry_run", "created_at"])
        for t in trades:
            writer.writerow([
                t.get("id"), t.get("symbol"), t.get("side"), t.get("level_index"),
                t.get("price"), t.get("amount"), t.get("profit"), t.get("fee"),
                t.get("dry_run"), t.get("created_at"),
            ])

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="", encoding="utf-8") as f:
            f.write(buf.getvalue())
            path = f.name
        try:
            await message.answer_document(FSInputFile(path, filename="trades.csv"))
        finally:
            os.unlink(path)

    logger.info("Telegram-бот управления запущен (long polling)")
    await dp.start_polling(bot)

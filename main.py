"""
main.py — FastAPI-приложение с дашбордом мониторинга и Grid-ботом,
работающим в фоне через asyncio.create_task.
"""

import asyncio
import csv
import io
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.templating import Jinja2Templates

import db
import telegram_bot
from grid_bot import GridBot

logger = logging.getLogger("main")

bot = GridBot()
bot_task: Optional[asyncio.Task] = None
telegram_task: Optional[asyncio.Task] = None

templates = Jinja2Templates(directory="templates")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global bot_task, telegram_task
    bot_task = asyncio.create_task(bot.run())
    logger.info("Grid bot запущен в фоне")

    if telegram_bot.enabled():
        telegram_task = asyncio.create_task(telegram_bot.create_and_run(bot, db))
        logger.info("Telegram-бот управления запущен в фоне")
    else:
        logger.info("Telegram-бот управления отключён (нет TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID)")

    yield

    await bot.stop()
    if bot_task:
        bot_task.cancel()
        try:
            await bot_task
        except asyncio.CancelledError:
            pass

    if telegram_task:
        telegram_task.cancel()
        try:
            await telegram_task
        except asyncio.CancelledError:
            pass

    logger.info("Grid bot остановлен")


app = FastAPI(title="Grid Trading Bot Dashboard", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(request, "dashboard.html", {"stats": bot.get_stats()})


@app.head("/", include_in_schema=False)
async def dashboard_head():
    """Многие аптайм-мониторы (UptimeRobot и др.) по умолчанию шлют HEAD, а не GET.
    Без этого обработчика они получали бы 405 и считали сервис "лежащим", хотя он живой."""
    return Response(status_code=200)


@app.get("/api/stats")
async def api_stats():
    return JSONResponse(bot.get_stats())


@app.get("/api/health")
async def health():
    """Эндпоинт для health-check на Render / аптайм-мониторов."""
    return {"ok": True}


@app.head("/api/health", include_in_schema=False)
async def health_head():
    return Response(status_code=200)


@app.get("/api/trades")
async def api_trades(limit: int = 200):
    """Журнал сделок + агрегированная статистика (винрейт, средний профит на цикл)."""
    trades = await db.fetch_trades(limit)
    stats = await db.fetch_trade_stats()
    return JSONResponse({"enabled": db.enabled(), "trades": trades, "stats": stats})


@app.get("/api/trades.csv")
async def api_trades_csv():
    """Экспорт полного журнала сделок в CSV."""
    trades = await db.fetch_trades(limit=100000)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id", "symbol", "side", "level_index", "price", "amount", "profit", "fee", "dry_run", "created_at"])
    for t in trades:
        writer.writerow([
            t.get("id"), t.get("symbol"), t.get("side"), t.get("level_index"),
            t.get("price"), t.get("amount"), t.get("profit"), t.get("fee"), t.get("dry_run"), t.get("created_at"),
        ])
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=trades.csv"},
    )

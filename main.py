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
from grid_bot import GridBot

logger = logging.getLogger("main")

bot = GridBot()
bot_task: Optional[asyncio.Task] = None

templates = Jinja2Templates(directory="templates")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global bot_task
    bot_task = asyncio.create_task(bot.run())
    logger.info("Grid bot запущен в фоне")
    yield
    await bot.stop()
    if bot_task:
        bot_task.cancel()
        try:
            await bot_task
        except asyncio.CancelledError:
            pass
    logger.info("Grid bot остановлен")


app = FastAPI(title="Grid Trading Bot Dashboard", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request, "stats": bot.get_stats()})


@app.get("/api/stats")
async def api_stats():
    return JSONResponse(bot.get_stats())


@app.get("/api/health")
async def health():
    """Эндпоинт для health-check на Render."""
    return {"ok": True}


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
    writer.writerow(["id", "symbol", "side", "level_index", "price", "amount", "profit", "dry_run", "created_at"])
    for t in trades:
        writer.writerow([
            t.get("id"), t.get("symbol"), t.get("side"), t.get("level_index"),
            t.get("price"), t.get("amount"), t.get("profit"), t.get("dry_run"), t.get("created_at"),
        ])
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=trades.csv"},
    )

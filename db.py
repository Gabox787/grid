"""
db.py — слой персистентности (PostgreSQL через asyncpg).

Хранит:
  - grid_levels — текущее состояние уровней сетки (переживает рестарт/редеплой на Render)
  - bot_meta    — счётчики и конфигурация (trades_completed, total_profit, флаги kill-switch)
  - trades      — журнал каждого филла для честного расчёта ROI/винрейта и экспорта в CSV

Если DATABASE_URL не задан — модуль работает в no-op режиме, бот просто живёт
в памяти процесса как раньше (обратная совместимость, ничего не ломается).
"""

import json
import logging
import os
from typing import Optional

logger = logging.getLogger("db")

DATABASE_URL = os.getenv("DATABASE_URL", "")

_pool = None
_asyncpg = None

if DATABASE_URL:
    try:
        import asyncpg
        _asyncpg = asyncpg
    except ImportError:
        logger.error("DATABASE_URL задан, но asyncpg не установлен — добавьте asyncpg в requirements.txt")


async def init():
    global _pool
    if not DATABASE_URL or not _asyncpg:
        logger.info("DATABASE_URL не задан — персистентность отключена, бот работает только в памяти")
        return
    try:
        _pool = await _asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    except Exception as e:
        logger.error(f"Не удалось подключиться к БД: {e}. Работаю без персистентности.")
        _pool = None
        return

    async with _pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS grid_levels (
                idx INTEGER PRIMARY KEY,
                price DOUBLE PRECISION NOT NULL,
                side TEXT,
                order_id TEXT,
                entry_price DOUBLE PRECISION,
                amount DOUBLE PRECISION,
                updated_at TIMESTAMPTZ DEFAULT now()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id SERIAL PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                level_index INTEGER,
                price DOUBLE PRECISION NOT NULL,
                amount DOUBLE PRECISION NOT NULL,
                profit DOUBLE PRECISION,
                fee DOUBLE PRECISION,
                dry_run BOOLEAN NOT NULL,
                created_at TIMESTAMPTZ DEFAULT now()
            )
        """)
        # На случай апгрейда существующей БД без колонки fee
        await conn.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS fee DOUBLE PRECISION")
    logger.info("Подключение к БД установлено, таблицы готовы")


def enabled() -> bool:
    return _pool is not None


async def close():
    if _pool:
        await _pool.close()


async def clear_levels() -> None:
    """Полностью очищает таблицу grid_levels. Вызывается перед записью свежей сетки,
    чтобы старые уровни от прошлых конфигураций (другой GRID_LEVELS/границы) не оставались
    orphan-строками в БД и не подмешивались при восстановлении после рестарта."""
    if not _pool:
        return
    try:
        async with _pool.acquire() as conn:
            await conn.execute("DELETE FROM grid_levels")
    except Exception as e:
        logger.error(f"Ошибка очистки старых уровней сетки: {e}")


async def save_level(level) -> None:
    if not _pool:
        return
    try:
        async with _pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO grid_levels (idx, price, side, order_id, entry_price, amount, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, now())
                ON CONFLICT (idx) DO UPDATE SET
                    side = EXCLUDED.side,
                    order_id = EXCLUDED.order_id,
                    entry_price = EXCLUDED.entry_price,
                    amount = EXCLUDED.amount,
                    updated_at = now()
                """,
                level.index, level.price, level.side, level.order_id, level.entry_price, level.amount,
            )
    except Exception as e:
        logger.error(f"Ошибка сохранения уровня {level.index} в БД: {e}")


async def load_levels() -> list[dict]:
    if not _pool:
        return []
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT idx, price, side, order_id, entry_price, amount FROM grid_levels ORDER BY idx"
        )
        return [dict(r) for r in rows]


async def save_meta(key: str, value) -> None:
    if not _pool:
        return
    try:
        async with _pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO bot_meta (key, value) VALUES ($1, $2)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """,
                key, json.dumps(value),
            )
    except Exception as e:
        logger.error(f"Ошибка сохранения meta[{key}] в БД: {e}")


async def load_meta(key: str, default=None):
    if not _pool:
        return default
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT value FROM bot_meta WHERE key = $1", key)
        if not row:
            return default
        try:
            return json.loads(row["value"])
        except (TypeError, json.JSONDecodeError):
            return default


async def log_trade(symbol: str, side: str, level_index: int, price: float, amount: float,
                     profit: Optional[float], fee: Optional[float], dry_run: bool) -> None:
    if not _pool:
        return
    try:
        async with _pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO trades (symbol, side, level_index, price, amount, profit, fee, dry_run)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                symbol, side, level_index, price, amount, profit, fee, dry_run,
            )
    except Exception as e:
        logger.error(f"Ошибка записи сделки в БД: {e}")


async def fetch_trades(limit: int = 200) -> list[dict]:
    if not _pool:
        return []
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM trades ORDER BY created_at DESC LIMIT $1", limit
        )
        result = []
        for r in rows:
            d = dict(r)
            if d.get("created_at"):
                d["created_at"] = d["created_at"].isoformat()
            result.append(d)
        return result


async def fetch_trade_stats() -> dict:
    empty = {
        "total_trades": 0, "closed_cycles": 0, "wins": 0, "losses": 0,
        "win_rate": None, "total_profit": 0.0, "avg_profit_per_cycle": None, "total_fees": 0.0,
    }
    if not _pool:
        return empty
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                COUNT(*) AS total_trades,
                COUNT(*) FILTER (WHERE profit IS NOT NULL) AS closed_cycles,
                COUNT(*) FILTER (WHERE profit > 0) AS wins,
                COUNT(*) FILTER (WHERE profit <= 0 AND profit IS NOT NULL) AS losses,
                COALESCE(SUM(profit), 0) AS total_profit,
                AVG(profit) AS avg_profit_per_cycle,
                COALESCE(SUM(fee), 0) AS total_fees
            FROM trades
        """)
        closed = row["closed_cycles"] or 0
        wins = row["wins"] or 0
        return {
            "total_trades": row["total_trades"],
            "closed_cycles": closed,
            "wins": wins,
            "losses": row["losses"] or 0,
            "win_rate": round(wins / closed * 100, 1) if closed else None,
            "total_profit": float(row["total_profit"] or 0),
            "avg_profit_per_cycle": (
                float(row["avg_profit_per_cycle"]) if row["avg_profit_per_cycle"] is not None else None
            ),
            "total_fees": float(row["total_fees"] or 0),
        }

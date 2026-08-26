"""
grid_bot.py — Асинхронный Grid Trading Bot на ccxt.async_support.

Логика:
  - Бюджет депозита делится на две зоны:
      CORE      — основная сетка между LOWER_BOUND и UPPER_BOUND.
      EXTENSION — резерв ниже LOWER_BOUND: если рынок падает и собирает все
                  Buy-ордера основной сетки, бот не останавливается, а докупает
                  на дальнейших откатах уровнями резервной зоны, оставаясь
                  в рамках заранее посчитанного бюджета (никогда не тратит
                  весь депозит, часть всегда остаётся нетронутым резервом).
  - Ни один актив не продаётся по рынку принудительно. Купленная позиция
    всегда ждёт СВОЙ лимитный Sell на нужном уровне — сколько угодно долго.
  - Kill-switch — это не "закрыть всё и остановиться", а alert-only монитор:
    при выходе цены за LOWER_BOUND и за границу резервной зоны бот шлёт
    уведомление (в лог и, если настроено, в Telegram) и просто перестаёт
    открывать НОВЫЕ покупки сверх бюджета — открытые позиции не трогает.
  - Состояние (уровни сетки, счётчики, флаги) персистится в Postgres (db.py),
    поэтому редеплой/рестарт на Render не обнуляет бота: при старте бот
    сначала пытается восстановиться из БД и сверяет восстановленные ордера
    с реальным статусом на бирже (вдруг что-то исполнилось, пока инстанс лежал).
  - Каждый филл пишется в таблицу trades — это основа для честного ROI/винрейта
    и CSV-выгрузки (см. /api/trades и /api/trades.csv в main.py).
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

import ccxt.async_support as ccxt
from ccxt.base.errors import (
    NetworkError,
    RateLimitExceeded,
    ExchangeError,
    InsufficientFunds,
    OrderNotFound,
    InvalidOrder,
)

import db

logger = logging.getLogger("grid_bot")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


class BotStatus(str, Enum):
    STARTING = "starting"
    RUNNING = "running"
    ERROR = "error"
    STOPPED = "stopped"


@dataclass
class GridLevel:
    index: int
    price: float
    side: Optional[str] = None            # "buy" | "sell" | None (уровень свободен)
    order_id: Optional[str] = None
    entry_price: Optional[float] = None   # цена покупки — нужна для расчёта профита при продаже
    amount: Optional[float] = None        # объём в базовой валюте, зафиксированный при открытии ордера


@dataclass
class BotState:
    status: BotStatus = BotStatus.STARTING
    last_error: Optional[str] = None
    current_price: Optional[float] = None
    last_update: Optional[float] = None
    trades_completed: int = 0
    total_profit: float = 0.0
    total_fees: float = 0.0
    cycles: int = 0
    extension_notified: bool = False   # цена сейчас ниже LOWER_BOUND, работаем в резервной зоне
    budget_exhausted: bool = False     # весь бюджет (core+extension) использован


class GridBot:
    def __init__(self):
        # === Базовая конфигурация ===
        self.exchange_id = os.getenv("EXCHANGE_ID", "binance")
        # Пустая строка "" в Python — это НЕ None, а ccxt проверяет именно "is not None"
        # при решении, добавлять ли подписанные (авторизованные) запросы в load_markets().
        # Поэтому пустую строку принудительно превращаем в None, чтобы бот честно работал
        # без реальных ключей биржи в DRY_RUN — иначе ccxt пытается подписать запрос
        # пустыми учётными данными и падает с AuthenticationError даже на публичных данных.
        self.api_key = os.getenv("API_KEY") or None
        self.api_secret = os.getenv("API_SECRET") or None
        self.symbol = os.getenv("SYMBOL", "BTC/USDT")

        self.upper_bound = float(os.getenv("UPPER_BOUND", "66000"))
        self.lower_bound = float(os.getenv("LOWER_BOUND", "60000"))
        self.grid_levels = int(os.getenv("GRID_LEVELS", "10"))

        self.poll_interval = float(os.getenv("POLL_INTERVAL", "10"))
        self.dry_run = os.getenv("DRY_RUN", "true").lower() == "true"
        self.place_initial_sells = os.getenv("PLACE_INITIAL_SELLS", "true").lower() == "true"
        self.fee_rate_pct = float(os.getenv("FEE_RATE_PCT", "0.001"))  # 0.1% — типовая спот-комиссия, если биржа не вернула fee
        # В DRY_RUN нет реального стакана, который исполнял бы ордера пока бот лежит —
        # есть только сравнение "текущая цена vs уровень" на момент опроса. Если простой
        # был долгим, это превращается в мгновенный "марафон" фиктивных фillov против
        # текущей цены вместо честной картины по шагам. Порог ниже — с какого простоя (сек)
        # в DRY_RUN безопаснее честно пересобрать сетку у текущей цены, чем разыгрывать это.
        # В LIVE (dry_run=false) не применяется — там реальные ордера на бирже, реальные фиты.
        self.stale_resume_threshold = float(os.getenv("STALE_RESUME_THRESHOLD_SECONDS", "1800"))

        # === Управление капиталом: CORE + EXTENSION ===
        self.deposit_usdt = float(os.getenv("DEPOSIT_USDT", "2000"))
        self.core_usage_ratio = float(os.getenv("CORE_USAGE_RATIO", "0.6"))
        self.extension_usage_ratio = float(os.getenv("EXTENSION_USAGE_RATIO", "0.2"))
        self.extension_drop_pct = float(os.getenv("EXTENSION_DROP_PCT", "0.15"))
        self.extension_levels_count = int(os.getenv("EXTENSION_LEVELS", "5"))

        if self.core_usage_ratio < 0 or self.extension_usage_ratio < 0:
            raise ValueError("CORE_USAGE_RATIO и EXTENSION_USAGE_RATIO должны быть >= 0")
        if self.core_usage_ratio + self.extension_usage_ratio > 1:
            raise ValueError("CORE_USAGE_RATIO + EXTENSION_USAGE_RATIO не может быть больше 1")
        if self.grid_levels < 2:
            raise ValueError("GRID_LEVELS должен быть >= 2")
        if self.upper_bound <= self.lower_bound:
            raise ValueError("UPPER_BOUND должен быть больше LOWER_BOUND")

        self.core_capital = round(self.deposit_usdt * self.core_usage_ratio, 2)
        self.extension_capital = round(self.deposit_usdt * self.extension_usage_ratio, 2)
        self.reserve_usdt = round(self.deposit_usdt - self.core_capital - self.extension_capital, 2)

        self.base_currency, self.quote_currency = self.symbol.split("/")

        # === Telegram-уведомления (опционально) ===
        self.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")

        exchange_class = getattr(ccxt, self.exchange_id)
        self.exchange = exchange_class({
            "apiKey": self.api_key,
            "secret": self.api_secret,
            "enableRateLimit": True,
        })

        self.levels: list[GridLevel] = []
        self.qty_core: float = 0.0
        self.qty_extension: float = 0.0
        self.extension_lower_bound: Optional[float] = None
        self.state = BotState()
        self.start_time: Optional[float] = None
        self.paused: bool = False  # управляется командой /pause в Telegram — новые входы не открываются
        self.ready = asyncio.Event()  # выставляется после успешной инициализации/восстановления сетки
        self._lock = asyncio.Lock()
        self._stop_event = asyncio.Event()

    # ------------------------------------------------------------------
    # Обёртка над вызовами ccxt: retry + экспоненциальный backoff
    # ------------------------------------------------------------------

    async def _safe_call(self, func, *args, retries: int = 3, base_delay: float = 2.0, **kwargs):
        attempt = 0
        while attempt < retries:
            try:
                return await func(*args, **kwargs)
            except RateLimitExceeded as e:
                attempt += 1
                delay = base_delay * (2 ** (attempt - 1))
                logger.warning(f"Rate limit, попытка {attempt}/{retries}, ждём {delay}s: {e}")
                await asyncio.sleep(delay)
            except NetworkError as e:
                attempt += 1
                delay = base_delay * (2 ** (attempt - 1))
                logger.warning(f"Сетевая ошибка, попытка {attempt}/{retries}, ждём {delay}s: {e}")
                await asyncio.sleep(delay)
            except (InsufficientFunds, InvalidOrder, OrderNotFound):
                raise
            except ExchangeError as e:
                logger.error(f"Ошибка биржи: {e}")
                raise
        raise NetworkError(f"Не удалось выполнить запрос после {retries} попыток")

    # ------------------------------------------------------------------
    # Уведомления (лог + опционально Telegram)
    # ------------------------------------------------------------------

    async def _notify(self, message: str, level: str = "warning"):
        getattr(logger, level, logger.warning)(message)
        if not self.telegram_token or not self.telegram_chat_id:
            return

        import aiohttp
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"

        for attempt in range(3):
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
                    async with session.post(url, json={"chat_id": self.telegram_chat_id, "text": message}) as resp:
                        if resp.status == 200:
                            return
                        if resp.status == 429:
                            # Telegram сам подсказывает сколько ждать (rate limit на чат)
                            retry_after = 2.0
                            try:
                                data = await resp.json()
                                retry_after = float(data.get("parameters", {}).get("retry_after", 2))
                            except Exception:
                                pass
                            logger.warning(
                                f"Telegram rate limit (429), жду {retry_after:.0f}с и повторяю "
                                f"(попытка {attempt + 1}/3)"
                            )
                            await asyncio.sleep(retry_after)
                            continue
                        body = await resp.text()
                        logger.error(f"Telegram sendMessage вернул {resp.status}: {body}")
                        return
            except Exception as e:
                logger.error(f"Не удалось отправить Telegram-уведомление (попытка {attempt + 1}/3): {e}")
                await asyncio.sleep(1.0)

        logger.error(f"Telegram-уведомление НЕ доставлено после 3 попыток: {message[:80]}...")

    # ------------------------------------------------------------------
    # Расчёт уровней сетки
    # ------------------------------------------------------------------

    def _calculate_core_levels(self) -> list[float]:
        step = (self.upper_bound - self.lower_bound) / (self.grid_levels - 1)
        return [round(self.lower_bound + i * step, 8) for i in range(self.grid_levels)]

    def _calculate_extension_levels(self) -> list[float]:
        if self.extension_levels_count <= 0 or self.extension_capital <= 0:
            self.extension_lower_bound = self.lower_bound
            return []
        self.extension_lower_bound = round(self.lower_bound * (1 - self.extension_drop_pct), 8)
        step = (self.lower_bound - self.extension_lower_bound) / self.extension_levels_count
        prices = [round(self.lower_bound - step * i, 8) for i in range(1, self.extension_levels_count + 1)]
        return sorted(prices)  # по возрастанию, строго ниже lower_bound

    # ------------------------------------------------------------------
    # Работа с ордерами
    # ------------------------------------------------------------------

    async def _place_order(self, side: str, price: float, amount: float) -> Optional[str]:
        if self.dry_run:
            fake_id = f"dry-{side}-{price}-{int(time.time() * 1000)}"
            logger.info(f"[DRY_RUN] {side.upper()} {amount} {self.symbol} @ {price}")
            return fake_id
        try:
            precise_amount = float(self.exchange.amount_to_precision(self.symbol, amount))
            order = await self._safe_call(
                self.exchange.create_limit_order,
                self.symbol, side, precise_amount, price,
            )
            logger.info(f"Ордер размещён: {side.upper()} {precise_amount} @ {price} (id={order['id']})")
            return order["id"]
        except InsufficientFunds:
            logger.warning(f"Пропуск уровня {price}: недостаточно средств для {side}")
            return None
        except Exception as e:
            logger.error(f"Не удалось разместить ордер {side} @ {price}: {e}")
            return None

    async def _check_balance_budget(self):
        """
        Preflight: на бирже должно быть не меньше (core_capital + extension_capital)
        свободного quote-актива. Если нет — бот не стартует, чтобы не разместить
        сетку "наполовину" и не остаться в непонятном состоянии.
        """
        if self.dry_run:
            return
        try:
            balance = await self._safe_call(self.exchange.fetch_balance)
            free_quote = balance.get(self.quote_currency, {}).get("free", 0) or 0
        except Exception as e:
            logger.warning(f"Не удалось проверить баланс перед стартом: {e}")
            return

        required = self.core_capital + self.extension_capital
        if free_quote < required:
            raise RuntimeError(
                f"Недостаточно {self.quote_currency}: доступно {free_quote:.2f}, "
                f"требуется {required:.2f} (core {self.core_capital} + extension {self.extension_capital})"
            )

    async def initialize_grid(self):
        logger.info(
            f"Инициализация сетки {self.symbol}: {self.lower_bound}-{self.upper_bound}, "
            f"уровней={self.grid_levels}, dry_run={self.dry_run}"
        )

        await self._check_balance_budget()

        ticker = await self._safe_call(self.exchange.fetch_ticker, self.symbol)
        current_price = ticker["last"]
        self.state.current_price = current_price

        core_prices = self._calculate_core_levels()
        ext_prices = self._calculate_extension_levels()
        n_ext = len(ext_prices)

        all_prices = ext_prices + core_prices  # уже отсортированы по возрастанию, без пересечений
        self.levels = [GridLevel(index=i, price=p) for i, p in enumerate(all_prices)]

        self.qty_core = round(self.core_capital / sum(core_prices), 8) if core_prices else 0.0
        self.qty_extension = round(self.extension_capital / sum(ext_prices), 8) if ext_prices else 0.0

        logger.info(
            f"Бюджет: депозит {self.deposit_usdt} {self.quote_currency} · "
            f"core {self.core_capital} ({self.core_usage_ratio*100:.0f}%) · "
            f"extension {self.extension_capital} ({self.extension_usage_ratio*100:.0f}%) · "
            f"резерв {self.reserve_usdt}. Объём: core={self.qty_core}, extension={self.qty_extension}"
        )

        for i, level in enumerate(self.levels):
            zone_qty = self.qty_extension if i < n_ext else self.qty_core
            if level.price <= current_price:
                order_id = await self._place_order("buy", level.price, zone_qty)
                if order_id:
                    level.side = "buy"
                    level.order_id = order_id
                    level.amount = zone_qty
            elif self.place_initial_sells:
                order_id = await self._place_order("sell", level.price, zone_qty)
                if order_id:
                    level.side = "sell"
                    level.order_id = order_id
                    level.amount = zone_qty
                    level.entry_price = current_price  # приблизительная точка отсчёта для профита

        await self._persist_full_state()
        logger.info(f"Сетка инициализирована. Текущая цена: {current_price}")

    # ------------------------------------------------------------------
    # Персистентность
    # ------------------------------------------------------------------

    def _config_fingerprint(self) -> dict:
        return {
            "symbol": self.symbol,
            "lower_bound": self.lower_bound,
            "upper_bound": self.upper_bound,
            "grid_levels": self.grid_levels,
            "extension_levels": self.extension_levels_count,
            "extension_drop_pct": self.extension_drop_pct,
        }

    async def _persist_full_state(self):
        if not db.enabled():
            return
        # Сначала полностью чистим таблицу — иначе при уменьшении GRID_LEVELS/смене границ
        # старые уровни с "хвостовыми" индексами остаются orphan-строками и могут потом
        # подмешаться при восстановлении, ломая порядок цен по возрастанию.
        await db.clear_levels()
        await db.save_meta("config", self._config_fingerprint())
        await db.save_meta("qty_core", self.qty_core)
        await db.save_meta("qty_extension", self.qty_extension)
        await db.save_meta("extension_lower_bound", self.extension_lower_bound)
        for level in self.levels:
            await db.save_level(level)

    async def _persist_counters(self):
        if not db.enabled():
            return
        await db.save_meta("trades_completed", self.state.trades_completed)
        await db.save_meta("total_profit", self.state.total_profit)
        await db.save_meta("total_fees", self.state.total_fees)
        await db.save_meta("extension_notified", self.state.extension_notified)
        await db.save_meta("budget_exhausted", self.state.budget_exhausted)

    async def reset_stats(self):
        """Сбрасывает счётчики и журнал сделок (для команды /reset_stats), не трогая
        текущую живую сетку/открытые ордера — только историю и статистику."""
        async with self._lock:
            self.state.trades_completed = 0
            self.state.total_profit = 0.0
            self.state.total_fees = 0.0
            await db.reset_trade_history()
            await self._persist_counters()

    async def _try_resume_from_db(self) -> bool:
        if not db.enabled():
            return False

        saved_config = await db.load_meta("config")
        if saved_config != self._config_fingerprint():
            logger.info("Конфигурация сетки отличается от сохранённой (или это первый запуск) — инициализация с нуля")
            return False

        rows = await db.load_levels()
        if not rows:
            return False

        # В DRY_RUN нет реального стакана биржи, который исполнял бы ордера пока бот лежал —
        # только сравнение "текущая цена vs уровень" на момент опроса. Если простой был долгим
        # (упал сервис, не сработал аптайм-монитор и т.п.), это выльется в мгновенный "марафон"
        # фиктивных филлов против текущей цены сразу по всем уровням, которые цена прошла за
        # время простоя — нечестная картина и куча спама в Telegram. Раз реальных денег тут нет,
        # честнее просто пересобрать сетку у текущей цены, чем разыгрывать этот марафон.
        # В LIVE (dry_run=false) это НЕ применяется: там реальные ордера с реальными фитами,
        # _reconcile_with_exchange() ниже и так возьмёт с биржи правду по каждому ордеру.
        if self.dry_run:
            last_update = await db.load_meta("last_update", None)
            if last_update is not None:
                downtime = time.time() - float(last_update)
                if downtime > self.stale_resume_threshold:
                    logger.warning(
                        f"DRY_RUN: обнаружен простой ~{downtime / 60:.0f} мин (порог "
                        f"{self.stale_resume_threshold / 60:.0f} мин) — вместо марафона фиктивных "
                        f"филлов пересобираю сетку заново у текущей цены."
                    )
                    await db.clear_levels()
                    return False

        levels = [
            GridLevel(
                index=r["idx"], price=r["price"], side=r["side"], order_id=r["order_id"],
                entry_price=r["entry_price"], amount=r["amount"],
            )
            for r in rows
        ]

        # Защита от orphan-строк / рассинхрона: массив уровней ДОЛЖЕН быть строго
        # отсортирован по возрастанию цены (на этом держится вся логика "buy fills -> sell
        # выше", "sell fills -> buy ниже"). Если это нарушено — данным из БД не доверяем
        # и переинициализируем сетку с нуля, а не тащим сломанное состояние в торговлю.
        prices = [lvl.price for lvl in levels]
        if prices != sorted(prices) or len(levels) != len(set(p for p in prices)):
            logger.error(
                f"Уровни из БД не отсортированы по возрастанию (обнаружены orphan-строки "
                f"от прошлой конфигурации) — переинициализирую сетку с нуля вместо восстановления."
            )
            await db.clear_levels()
            return False

        logger.info(f"Восстанавливаю {len(rows)} уровней сетки из БД после рестарта")
        self.levels = levels
        self.qty_core = await db.load_meta("qty_core", 0.0) or 0.0
        self.qty_extension = await db.load_meta("qty_extension", 0.0) or 0.0
        self.extension_lower_bound = await db.load_meta("extension_lower_bound", self.lower_bound)
        self.state.trades_completed = await db.load_meta("trades_completed", 0) or 0
        self.state.total_profit = await db.load_meta("total_profit", 0.0) or 0.0
        self.state.total_fees = await db.load_meta("total_fees", 0.0) or 0.0
        self.state.extension_notified = bool(await db.load_meta("extension_notified", False))
        self.state.budget_exhausted = bool(await db.load_meta("budget_exhausted", False))

        await self._reconcile_with_exchange()
        return True

    async def _reconcile_with_exchange(self):
        """После рестарта проверяем: может, пока инстанс был недоступен на Render,
        какие-то ордера успели исполниться на бирже — обрабатываем эти филлы сейчас."""
        if self.dry_run:
            return
        events: list = []
        for level in list(self.levels):
            if not level.order_id or not level.side:
                continue
            try:
                order = await self._safe_call(self.exchange.fetch_order, level.order_id, self.symbol)
            except OrderNotFound:
                logger.warning(f"После рестарта ордер {level.order_id} не найден, сбрасываю уровень {level.index}")
                level.order_id = None
                level.side = None
                await db.save_level(level)
                continue
            except Exception as e:
                logger.error(f"Не удалось сверить ордер {level.order_id} после рестарта: {e}")
                continue

            if order["status"] == "closed":
                fill_price = order.get("average") or order.get("price") or level.price
                logger.info(f"Обнаружен филл во время простоя: уровень {level.index} @ {fill_price}")
                await self._handle_fill(level, fill_price, events=events)
            elif order["status"] == "canceled":
                level.order_id = None
                level.side = None
                await db.save_level(level)
        await self._send_batched_events(events)

    # ------------------------------------------------------------------
    # Обработка исполнения ордера -> перестановка на соседний уровень
    # ------------------------------------------------------------------

    def _format_fill_event(self, ev: dict) -> str:
        q, b = self.quote_currency, self.base_currency
        sum_usd = ev["price"] * ev["amount"]
        time_str = ev["time"].strftime("%Y-%m-%d %H:%M:%S")

        if ev["type"] == "buy":
            return (
                f"📉 ВХОД BUY ур.{ev['index']}/{ev['grid_size'] - 1}\n"
                f"💲 Цена: {ev['price']}\n"
                f"💵 Сумма: {sum_usd:.2f} {q} ({ev['amount']} {b})\n"
                f"💰 Комиссия: ~{ev['fee']:.4f} {q}\n"
                f"🕐 {time_str} UTC"
            )

        profit = ev["profit"]
        buy_price = ev["entry_price"]
        if profit is not None:
            pnl_pct = (profit / (buy_price * ev["amount"]) * 100) if buy_price else 0.0
            pnl_str = f"{profit:+.4f} {q} ({pnl_pct:+.2f}%)"
            emoji = "📈" if profit >= 0 else "🔻"
        else:
            pnl_str = "н/д (начальная продажа)"
            emoji = "📈"

        return (
            f"{emoji} ВЫХОД SELL ур.{ev['index']}/{ev['grid_size'] - 1}\n"
            f"💲 {buy_price if buy_price else '—'} → {ev['price']}\n"
            f"💵 Сумма: {sum_usd:.2f} {q} ({ev['amount']} {b})\n"
            f"🎯 P&L: {pnl_str}\n"
            f"💰 Комиссия: ~{ev['fee']:.4f} {q}\n"
            f"🕐 {time_str} UTC"
        )

    async def _handle_fill(self, level: GridLevel, fill_price: float, fee_cost: Optional[float] = None,
                            events: Optional[list] = None):
        async with self._lock:
            index = level.index
            grid_size = len(self.levels)

            if level.side == "buy":
                bought_price = fill_price
                amount = level.amount or self.qty_core
                fee = fee_cost if fee_cost is not None else round(fill_price * amount * self.fee_rate_pct, 8)
                self.state.total_fees += fee

                level.side = None
                level.order_id = None
                level.entry_price = None
                level.amount = None
                await db.save_level(level)
                await db.log_trade(self.symbol, "buy", index, fill_price, amount, None, fee, self.dry_run)

                next_index = index + 1
                if next_index < len(self.levels) and not self.paused:
                    next_level = self.levels[next_index]
                    order_id = await self._place_order("sell", next_level.price, amount)
                    if order_id:
                        next_level.side = "sell"
                        next_level.order_id = order_id
                        next_level.entry_price = bought_price
                        next_level.amount = amount
                        await db.save_level(next_level)

                self.state.trades_completed += 1
                await self._persist_counters()
                logger.info(f"BUY исполнен, уровень {index} @ {fill_price}. Sell выставлен выше.")

                event = {
                    "type": "buy", "index": index, "grid_size": grid_size,
                    "price": fill_price, "amount": amount, "fee": fee,
                    "time": datetime.now(timezone.utc),
                }
                if events is not None:
                    events.append(event)
                else:
                    await self._notify(self._format_fill_event(event), level="info")

            elif level.side == "sell":
                sell_price = fill_price
                buy_price = level.entry_price
                amount = level.amount or self.qty_core
                fee = fee_cost if fee_cost is not None else round(sell_price * amount * self.fee_rate_pct, 8)
                self.state.total_fees += fee

                profit = None
                if buy_price is not None:
                    profit = round((sell_price - buy_price) * amount, 8)
                    self.state.total_profit += profit
                    logger.info(f"SELL исполнен, уровень {index} @ {sell_price}. Профит цикла: {profit:.6f}")
                else:
                    logger.info(f"SELL исполнен, уровень {index} @ {sell_price} (начальная продажа, профит не считается)")

                level.side = None
                level.order_id = None
                level.entry_price = None
                level.amount = None
                await db.save_level(level)
                await db.log_trade(self.symbol, "sell", index, sell_price, amount, profit, fee, self.dry_run)

                prev_index = index - 1
                if prev_index >= 0 and not self.paused:
                    prev_level = self.levels[prev_index]
                    order_id = await self._place_order("buy", prev_level.price, amount)
                    if order_id:
                        prev_level.side = "buy"
                        prev_level.order_id = order_id
                        prev_level.amount = amount
                        await db.save_level(prev_level)

                self.state.trades_completed += 1
                await self._persist_counters()

                event = {
                    "type": "sell", "index": index, "grid_size": grid_size,
                    "price": sell_price, "amount": amount, "fee": fee,
                    "entry_price": buy_price, "profit": profit,
                    "time": datetime.now(timezone.utc),
                }
                if events is not None:
                    events.append(event)
                else:
                    await self._notify(self._format_fill_event(event), level="info")

    async def _send_batched_events(self, events: list):
        """Объединяет несколько филлов за один цикл в ОДНО Telegram-сообщение —
        снижает риск rate-limit при быстром движении цены (много филлов подряд)
        и не заваливает чат отдельными сообщениями каждую секунду."""
        if not events:
            return
        blocks = [self._format_fill_event(ev) for ev in events]
        if len(blocks) == 1:
            text = blocks[0]
        else:
            text = f"📦 {len(blocks)} событий за один цикл:\n\n" + "\n\n".join(blocks)
        for chunk_start in range(0, len(text), 3800):
            await self._notify(text[chunk_start:chunk_start + 3800], level="info")

    # ------------------------------------------------------------------
    # Kill-switch / alert-монитор (никогда не закрывает позиции сам)
    # ------------------------------------------------------------------

    async def _check_boundary_alerts(self):
        price = self.state.current_price
        if price is None:
            return

        if price < self.lower_bound and not self.state.extension_notified:
            self.state.extension_notified = True
            await self._notify(
                f"⚠️ {self.symbol}: цена {price} ниже LOWER_BOUND ({self.lower_bound}). "
                f"Включается резервная зона — ещё ${self.extension_capital} на догрузку на откатах. "
                f"Открытые позиции никто не продаёт по рынку, ждут своего Sell-уровня.",
                level="warning",
            )
            await self._persist_counters()
        elif price >= self.lower_bound and self.state.extension_notified:
            self.state.extension_notified = False
            await self._notify(f"✅ {self.symbol}: цена вернулась выше LOWER_BOUND ({self.lower_bound}).", level="info")
            await self._persist_counters()

        if self.extension_lower_bound is not None:
            if price < self.extension_lower_bound and not self.state.budget_exhausted:
                self.state.budget_exhausted = True
                await self._notify(
                    f"🛑 {self.symbol}: цена {price} пробила и резервную зону ({self.extension_lower_bound}). "
                    f"Весь заложенный бюджет (core+extension) размещён, новых Buy больше не будет. "
                    f"Бот продолжает мониторить открытые позиции и ждать восстановления цены.",
                    level="error",
                )
                await self._persist_counters()
            elif price >= self.extension_lower_bound and self.state.budget_exhausted:
                self.state.budget_exhausted = False
                await self._persist_counters()

    # ------------------------------------------------------------------
    # Основной цикл
    # ------------------------------------------------------------------

    def _extract_fee(self, order: dict) -> Optional[float]:
        """Пытаемся достать реальную комиссию из ответа биржи (в quote-валюте).
        Если биржа её не вернула — вернём None, и _handle_fill применит оценку по FEE_RATE_PCT."""
        fee_info = order.get("fee")
        if fee_info and fee_info.get("cost") is not None and fee_info.get("currency") == self.quote_currency:
            return float(fee_info["cost"])
        fees_list = order.get("fees") or []
        matching = [f for f in fees_list if f.get("currency") == self.quote_currency and f.get("cost") is not None]
        if matching:
            return sum(float(f["cost"]) for f in matching)
        return None

    async def _check_orders(self):
        events: list = []

        for level in self.levels:
            if not level.order_id or not level.side:
                continue

            if self.dry_run:
                price = self.state.current_price
                if price is None:
                    continue
                filled = (
                    (level.side == "buy" and price <= level.price)
                    or (level.side == "sell" and price >= level.price)
                )
                if filled:
                    await self._handle_fill(level, level.price, fee_cost=None, events=events)
                continue

            try:
                order = await self._safe_call(self.exchange.fetch_order, level.order_id, self.symbol)
            except OrderNotFound:
                logger.warning(f"Ордер {level.order_id} не найден, сбрасываю уровень {level.index}")
                level.order_id = None
                level.side = None
                await db.save_level(level)
                continue
            except Exception as e:
                logger.error(f"Ошибка проверки ордера {level.order_id}: {e}")
                continue

            if order["status"] == "closed":
                fill_price = order.get("average") or order.get("price") or level.price
                fee_cost = self._extract_fee(order)
                await self._handle_fill(level, fill_price, fee_cost=fee_cost, events=events)
            elif order["status"] == "canceled":
                logger.warning(f"Ордер {level.order_id} отменён вручную, уровень {level.index} освобождён")
                level.order_id = None
                level.side = None
                await db.save_level(level)

        await self._send_batched_events(events)

    async def _update_price(self):
        ticker = await self._safe_call(self.exchange.fetch_ticker, self.symbol)
        self.state.current_price = ticker["last"]

    async def run(self):
        self.start_time = time.time()
        await db.init()
        self.state.status = BotStatus.STARTING

        # Retry-цикл на старте: если инициализация упала (например, база ещё просыпается,
        # или временный сетевой сбой у биржи) — пробуем снова с задержкой, а не остаёмся
        # навсегда в ERROR до ручного redeploy. /status в Telegram и логи всё равно покажут
        # last_error на каждой попытке, так что реальная (не транзиентная) проблема видна сразу.
        retry_delay = 10.0
        max_retry_delay = 60.0
        attempt = 0
        while not self._stop_event.is_set():
            attempt += 1
            try:
                resumed = await self._try_resume_from_db()
                if not resumed:
                    await self.initialize_grid()
                self.state.status = BotStatus.RUNNING
                self.ready.set()
                break
            except Exception as e:
                logger.exception(f"Ошибка инициализации сетки (попытка {attempt}), повтор через {retry_delay:.0f}с")
                self.state.status = BotStatus.ERROR
                self.state.last_error = str(e)
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=retry_delay)
                except asyncio.TimeoutError:
                    pass
                retry_delay = min(retry_delay * 1.5, max_retry_delay)

        if self._stop_event.is_set():
            self.state.status = BotStatus.STOPPED
            return

        while not self._stop_event.is_set():
            try:
                await self._update_price()
                await self._check_orders()
                await self._check_boundary_alerts()
                self.state.status = BotStatus.RUNNING
                self.state.last_error = None
            except Exception as e:
                logger.exception("Ошибка в торговом цикле")
                self.state.status = BotStatus.ERROR
                self.state.last_error = str(e)

            self.state.last_update = time.time()
            self.state.cycles += 1
            if db.enabled():
                await db.save_meta("last_update", self.state.last_update)

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.poll_interval)
            except asyncio.TimeoutError:
                pass

        self.state.status = BotStatus.STOPPED

    async def stop(self):
        self._stop_event.set()
        try:
            await self.exchange.close()
        except Exception:
            pass
        await db.close()

    # ------------------------------------------------------------------
    # Данные для дашборда / API
    # ------------------------------------------------------------------

    def get_stats(self) -> dict:
        locked_in_buys = round(
            sum((lvl.amount or 0) * lvl.price for lvl in self.levels if lvl.side == "buy"), 2
        )
        net_profit = round(self.state.total_profit - self.state.total_fees, 6)
        roi_pct = round(self.state.total_profit / self.deposit_usdt * 100, 4) if self.deposit_usdt else None
        uptime_seconds = round(time.time() - self.start_time, 1) if self.start_time else 0

        return {
            "status": self.state.status.value,
            "paused": self.paused,
            "last_error": self.state.last_error,
            "symbol": self.symbol,
            "exchange": self.exchange_id,
            "dry_run": self.dry_run,
            "current_price": self.state.current_price,
            "last_update": self.state.last_update,
            "uptime_seconds": uptime_seconds,
            "trades_completed": self.state.trades_completed,
            "total_profit": round(self.state.total_profit, 6),
            "total_fees": round(self.state.total_fees, 6),
            "net_profit": net_profit,
            "roi_pct": roi_pct,
            "cycles": self.state.cycles,
            "grid": {
                "lower_bound": self.lower_bound,
                "upper_bound": self.upper_bound,
                "levels_count": self.grid_levels,
                "qty_core": self.qty_core,
                "qty_extension": self.qty_extension,
            },
            "capital": {
                "deposit_usdt": self.deposit_usdt,
                "core_capital": self.core_capital,
                "extension_capital": self.extension_capital,
                "reserve_usdt": self.reserve_usdt,
                "locked_in_buys": locked_in_buys,
            },
            "safety": {
                "extension_lower_bound": self.extension_lower_bound,
                "extension_active": self.state.extension_notified,
                "budget_exhausted": self.state.budget_exhausted,
            },
            "persistence_enabled": db.enabled(),
            "open_orders": [
                {"index": lvl.index, "price": lvl.price, "side": lvl.side, "order_id": lvl.order_id}
                for lvl in self.levels
                if lvl.side is not None
            ],
        }

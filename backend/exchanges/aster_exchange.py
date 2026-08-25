"""Aster DEX USDT-M Futures - 完全 WS 化（脫離 ccxt）

Aster API 100% Binance 相容（asterdex.com 是 Binance fork）：
- WS: /market/stream + !markPrice@arr@1s, /stream + !bookTicker
- REST: /fapi/v1/exchangeInfo, /fapi/v1/premiumIndex, /fapi/v1/ticker/bookTicker, /fapi/v1/fundingInfo, /fapi/v1/fundingRate
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone

import aiohttp
import websockets

from exchanges.base import BaseExchange
from exchanges._session import make_session
from models import FundingRecord
from services.normalizer import calc_annual_rate

logger = logging.getLogger(__name__)

FAPI_BASE = "https://fapi.asterdex.com"
WS_MARK_URL = "wss://fstream.asterdex.com/market/stream?streams=!markPrice@arr@1s"
WS_BOOK_URL = "wss://fstream.asterdex.com/stream?streams=!bookTicker"

DEFAULT_FUNDING_INTERVAL_H = 8
METADATA_TTL = 86400  # 24h（PERPETUAL 合約清單變動慢）
FUNDING_INFO_TTL = 3600  # 1h（結算週期會動態變更，需較短 TTL 才能即時反映，與 binance 同款）


def _f(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _ws_to_unified(s: str) -> str | None:
    """ASTERUSDT -> ASTER/USDT:USDT (只取 USDT-margined)"""
    if not s.endswith("USDT"):
        return None
    base = s[:-4]
    return f"{base}/USDT:USDT" if base else None


def _unified_to_ws(symbol: str) -> str:
    return symbol.replace("/", "").split(":", 1)[0]


class _AsterTickerCache:
    def __init__(self):
        self._book: dict[str, dict] = {}
        self._mark: dict[str, dict] = {}
        self._book_task: asyncio.Task | None = None
        self._mark_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._book_ready = asyncio.Event()
        self._mark_ready = asyncio.Event()
        self._bootstrap_done = False

    async def ensure_started(self) -> None:
        async with self._lock:
            if not self._bootstrap_done:
                await asyncio.gather(self._bootstrap_book(), self._bootstrap_mark())
                self._bootstrap_done = True
            if self._mark_task is None or self._mark_task.done():
                self._mark_task = asyncio.create_task(self._run_mark())
            if self._book_task is None or self._book_task.done():
                self._book_task = asyncio.create_task(self._run_book())

    async def wait_warmup(self, timeout: float = 10.0) -> None:
        try:
            await asyncio.wait_for(
                asyncio.gather(self._mark_ready.wait(), self._book_ready.wait()),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            pass

    def get_book(self, symbol: str) -> dict | None:
        return self._book.get(symbol)

    def get_mark(self, symbol: str) -> dict | None:
        return self._mark.get(symbol)

    def stats(self) -> tuple[int, int]:
        return len(self._mark), len(self._book)

    async def _bootstrap_book(self) -> None:
        url = f"{FAPI_BASE}/fapi/v1/ticker/bookTicker"
        try:
            async with make_session(15) as s:
                async with s.get(url) as r:
                    data = await r.json()
            if not isinstance(data, list):
                return
            ts = time.time()
            for item in data:
                sym = item.get("symbol") or ""
                if "_" in sym or not sym:
                    continue
                bid = _f(item.get("bidPrice"))
                ask = _f(item.get("askPrice"))
                if bid is None and ask is None:
                    continue
                self._book[sym] = {"bid": bid, "ask": ask, "ts": ts}
            logger.info(f"[aster-ws] bookTicker REST bootstrap：{len(self._book)} symbols")
            self._book_ready.set()
        except Exception as e:
            logger.warning(f"[aster-ws] bookTicker bootstrap 失敗: {e}")

    async def _bootstrap_mark(self) -> None:
        url = f"{FAPI_BASE}/fapi/v1/premiumIndex"
        try:
            async with make_session(15) as s:
                async with s.get(url) as r:
                    data = await r.json()
            if not isinstance(data, list):
                return
            ts = time.time()
            for item in data:
                sym = item.get("symbol") or ""
                if "_" in sym or not sym:
                    continue
                mark = _f(item.get("markPrice"))
                index = _f(item.get("indexPrice"))
                rate = _f(item.get("lastFundingRate"))
                ft_ms = item.get("nextFundingTime")
                try:
                    ft_ms = int(ft_ms) if ft_ms else None
                except (TypeError, ValueError):
                    ft_ms = None
                self._mark[sym] = {
                    "mark": mark, "index": index, "funding_rate": rate,
                    "funding_time_ms": ft_ms, "ts": ts,
                }
            logger.info(f"[aster-ws] markPrice REST bootstrap：{len(self._mark)} symbols")
            self._mark_ready.set()
        except Exception as e:
            logger.warning(f"[aster-ws] markPrice bootstrap 失敗: {e}")

    async def _run_mark(self) -> None:
        backoff = 1
        while True:
            try:
                logger.info(f"[aster-ws] markPrice 連線 {WS_MARK_URL}")
                async with websockets.connect(WS_MARK_URL, ping_interval=20, ping_timeout=15) as ws:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        data = msg.get("data") if isinstance(msg, dict) else None
                        if not isinstance(data, list):
                            continue
                        ts = time.time()
                        for item in data:
                            if not isinstance(item, dict):
                                continue
                            s = item.get("s") or ""
                            if "_" in s or not s:
                                continue
                            try:
                                mark = float(item.get("p")) if item.get("p") is not None else None
                                index = float(item.get("i")) if item.get("i") is not None else None
                                rate = float(item.get("r")) if item.get("r") is not None else None
                                ft_ms = int(item.get("T")) if item.get("T") else None
                            except (TypeError, ValueError):
                                continue
                            self._mark[s] = {
                                "mark": mark, "index": index, "funding_rate": rate,
                                "funding_time_ms": ft_ms, "ts": ts,
                            }
                        if not self._mark_ready.is_set() and self._mark:
                            self._mark_ready.set()
                            backoff = 1
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[aster-ws] markPrice 中斷 {backoff}s 後重連: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _run_book(self) -> None:
        backoff = 1
        while True:
            try:
                logger.info(f"[aster-ws] bookTicker 連線 {WS_BOOK_URL}")
                async with websockets.connect(WS_BOOK_URL, ping_interval=20, ping_timeout=15) as ws:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        data = msg.get("data") if isinstance(msg, dict) else None
                        if not isinstance(data, dict):
                            continue
                        s = data.get("s") or ""
                        if "_" in s or not s:
                            continue
                        try:
                            bid = float(data["b"]) if data.get("b") is not None else None
                            ask = float(data["a"]) if data.get("a") is not None else None
                        except (TypeError, ValueError, KeyError):
                            continue
                        if bid is None and ask is None:
                            continue
                        self._book[s] = {"bid": bid, "ask": ask, "ts": time.time()}
                        if not self._book_ready.is_set():
                            self._book_ready.set()
                            backoff = 1
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[aster-ws] bookTicker 中斷 {backoff}s 後重連: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)


_cache: _AsterTickerCache | None = None


def _get_cache() -> _AsterTickerCache:
    global _cache
    if _cache is None:
        _cache = _AsterTickerCache()
    return _cache


class AsterExchange(BaseExchange):
    name = "aster"

    def __init__(self, api_key: str = "", secret: str = ""):
        # 兼容舊呼叫；新實作不需要 key（公開資料 only）
        self._session: aiohttp.ClientSession | None = None
        self._perp_symbols: set[str] = set()
        self._funding_intervals: dict[str, float] = {}
        self._metadata_fetched_at: float = 0
        self._funding_info_fetched_at: float = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = make_session(30)
        return self._session

    async def _ensure_metadata(self) -> None:
        """exchangeInfo（PERPETUAL universe）24h refresh；fundingInfo（per-symbol 結算週期）1h refresh。
        結算週期會被交易所動態調整（高波動幣臨時 4h→1h），用較短 TTL 才能及時反映。"""
        now = time.time()
        session = await self._get_session()
        # exchangeInfo: PERPETUAL universe（24h）
        if not self._perp_symbols or now - self._metadata_fetched_at >= METADATA_TTL:
            try:
                async with session.get(f"{FAPI_BASE}/fapi/v1/exchangeInfo") as r:
                    d = await r.json()
                self._perp_symbols = {
                    x["symbol"] for x in d.get("symbols", [])
                    if x.get("contractType") == "PERPETUAL"
                    and x.get("status") == "TRADING"
                    and x.get("quoteAsset") == "USDT"
                }
                self._metadata_fetched_at = now
            except Exception as e:
                logger.warning(f"[aster] exchangeInfo 取得失敗: {e}")
        # fundingInfo: per-symbol fundingIntervalHours（1h）
        if not self._funding_intervals or now - self._funding_info_fetched_at >= FUNDING_INFO_TTL:
            try:
                async with session.get(f"{FAPI_BASE}/fapi/v1/fundingInfo") as r:
                    d = await r.json()
                self._funding_intervals = {
                    item["symbol"]: float(item.get("fundingIntervalHours", DEFAULT_FUNDING_INTERVAL_H))
                    for item in d if isinstance(item, dict) and "symbol" in item
                }
                self._funding_info_fetched_at = now
                logger.info(f"[aster] metadata 更新：{len(self._perp_symbols)} PERP USDT，{len(self._funding_intervals)} 自訂 interval")
            except Exception as e:
                logger.warning(f"[aster] fundingInfo 取得失敗: {e}")

    async def fetch_funding_rates(self) -> list[FundingRecord]:
        await self._ensure_metadata()
        cache = _get_cache()
        await cache.ensure_started()
        await cache.wait_warmup(timeout=10.0)

        records: list[FundingRecord] = []
        bid_ask_hit = 0
        for ws_sym in sorted(self._perp_symbols):
            mark = cache.get_mark(ws_sym)
            if not mark or mark.get("funding_rate") is None:
                continue
            unified = _ws_to_unified(ws_sym)
            if not unified:
                continue
            book = cache.get_book(ws_sym) or {}
            bid = book.get("bid")
            ask = book.get("ask")
            if bid is not None or ask is not None:
                bid_ask_hit += 1
            interval_h = self._funding_intervals.get(ws_sym, DEFAULT_FUNDING_INTERVAL_H)
            ft = None
            if mark.get("funding_time_ms"):
                ft = datetime.fromtimestamp(mark["funding_time_ms"] / 1000, timezone.utc)
            rate = mark["funding_rate"]
            records.append(FundingRecord(
                exchange=self.name,
                symbol=unified,
                funding_rate=rate,
                next_funding_rate=None,
                funding_time=ft,
                mark_price=mark.get("mark"),
                index_price=mark.get("index"),
                bid_price=bid,
                ask_price=ask,
                annual_rate=calc_annual_rate(rate, interval_h),
                funding_interval_h=interval_h,
            ))
        mark_n, book_n = cache.stats()
        logger.info(
            f"[{self.name}] 取得 {len(records)} 筆費率（{len(self._perp_symbols)} PERP），"
            f"bid/ask: {bid_ask_hit} 個（WS mark cache {mark_n}、book cache {book_n}）"
        )
        return records

    async def fetch_funding_history(self, symbol: str, since: int = 0, limit: int = 100) -> list[dict]:
        ws_sym = _unified_to_ws(symbol)
        params: dict = {"symbol": ws_sym, "limit": str(min(limit, 1000))}
        if since:
            params["startTime"] = str(int(since))
        try:
            session = await self._get_session()
            async with session.get(f"{FAPI_BASE}/fapi/v1/fundingRate", params=params) as r:
                data = await r.json()
            if not isinstance(data, list):
                return []
            entries = [
                {"timestamp": int(it["fundingTime"]), "rate": float(it["fundingRate"])}
                for it in data if "fundingTime" in it and "fundingRate" in it
            ]
            entries.sort(key=lambda e: e["timestamp"])
            return entries
        except Exception as e:
            logger.debug(f"[{self.name}] 歷史費率查詢失敗 {symbol}: {e}")
            return []

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

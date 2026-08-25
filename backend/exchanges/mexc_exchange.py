"""MEXC USDT-M Contract - 完全 WS 化（脫離 ccxt）

- WS `sub.ticker` 為 per-symbol（mass subscribe，25ms 節流）
  含 fairPrice/indexPrice/bid1/ask1/fundingRate，但無 nextFundingTime
- REST bulk `/api/v1/contract/ticker` 一次拿 ~880 symbols（所有欄位）→ cold start bootstrap
- REST `/api/v1/contract/funding_rate/{sym}` 才有 nextSettleTime；不適合每筆查
  → 用 contract detail 的 settleCycle 推 next settle time
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

REST_BASE = "https://contract.mexc.com"
WS_URL = "wss://contract.mexc.com/edge"

DEFAULT_FUNDING_INTERVAL_H = 8
METADATA_TTL = 3600  # 合約清單快取（原 24h 太久，新上市合約如 pre-IPO 股票要隔天才進掃描）
INTERVAL_TTL = 240   # 結算週期快取需跟上滿資費縮週期（8h→1h），設短於掃描間隔、每輪刷新（一支 bulk 呼叫）


def _f(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _ws_to_unified(s: str) -> str | None:
    """BTC_USDT -> BTC/USDT:USDT"""
    if not s.endswith("_USDT"):
        return None
    base = s[:-5]
    return f"{base}/USDT:USDT" if base else None


def _unified_to_ws(symbol: str) -> str:
    base, rest = symbol.split("/", 1)
    quote = rest.split(":", 1)[0]
    return f"{base}_{quote}"


def _next_settle_time(interval_h: float) -> datetime:
    """根據 settle cycle，算出下一個結算時間（UTC 整點）"""
    now = datetime.now(timezone.utc)
    interval_s = int(interval_h * 3600)
    # 從 UTC 00:00 起算的 cycle
    midnight_ts = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    elapsed = int(now.timestamp()) - midnight_ts
    next_cycle_offset = ((elapsed // interval_s) + 1) * interval_s
    return datetime.fromtimestamp(midnight_ts + next_cycle_offset, timezone.utc)


class _MexcCache:
    def __init__(self):
        self._state: dict[str, dict] = {}
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._ready = asyncio.Event()
        self._desired: set[str] = set()
        self._bootstrap_done = False

    async def ensure_started(self, symbols: list[str]) -> None:
        async with self._lock:
            self._desired = set(symbols)
            if not self._bootstrap_done:
                await self._bootstrap_rest()
                self._bootstrap_done = True
            if self._task is None or self._task.done():
                self._task = asyncio.create_task(self._run())

    async def wait_warmup(self, timeout: float = 10.0) -> None:
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass

    def get(self, sym: str) -> dict | None:
        return self._state.get(sym)

    def stats(self) -> int:
        return len(self._state)

    async def _bootstrap_rest(self) -> None:
        try:
            async with make_session(15) as s:
                async with s.get(f"{REST_BASE}/api/v1/contract/ticker") as r:
                    d = await r.json()
            for item in d.get("data") or []:
                sym = item.get("symbol")
                if not sym:
                    continue
                self._state[sym] = dict(item)
            logger.info(f"[mexc-ws] REST bootstrap：{len(self._state)} symbols")
            if self._state:
                self._ready.set()
        except Exception as e:
            logger.warning(f"[mexc-ws] bootstrap 失敗: {e}")

    async def _run(self) -> None:
        backoff = 1
        while True:
            try:
                logger.info(f"[mexc-ws] 連線 {WS_URL}，預計訂閱 {len(self._desired)} symbols")
                async with websockets.connect(WS_URL, ping_interval=None, max_size=2 ** 22) as ws:
                    # mass subscribe，25ms 節流（實測 766 個 25s 全收）
                    for sym in sorted(self._desired):
                        try:
                            await ws.send(json.dumps({"method": "sub.ticker", "param": {"symbol": sym}}))
                        except Exception:
                            pass
                        await asyncio.sleep(0.025)
                    logger.info(f"[mexc-ws] 已送出 {len(self._desired)} 個 sub.ticker 訂閱")

                    async def _ping():
                        while True:
                            await asyncio.sleep(20)
                            try:
                                await ws.send(json.dumps({"method": "ping"}))
                            except Exception:
                                break
                    ping_task = asyncio.create_task(_ping())

                    try:
                        async for raw in ws:
                            try:
                                msg = json.loads(raw)
                            except Exception:
                                continue
                            ch = msg.get("channel", "")
                            if ch in ("pong", "rs.sub.ticker", "rs.error"):
                                continue
                            if ch != "push.ticker":
                                continue
                            d = msg.get("data") or {}
                            sym = msg.get("symbol") or d.get("symbol")
                            if not sym:
                                continue
                            cur = self._state.setdefault(sym, {})
                            cur.update(d)
                            if not self._ready.is_set():
                                self._ready.set()
                                backoff = 1
                    finally:
                        ping_task.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[mexc-ws] 連線中斷 {backoff}s 後重連: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)


_cache: _MexcCache | None = None


def _get_cache() -> _MexcCache:
    global _cache
    if _cache is None:
        _cache = _MexcCache()
    return _cache


class MexcExchange(BaseExchange):
    name = "mexc"

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None
        self._perp_symbols: set[str] = set()
        self._funding_intervals: dict[str, float] = {}
        self._metadata_fetched_at: float = 0
        self._intervals_fetched_at: float = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = make_session(30)
        return self._session

    async def _ensure_metadata(self) -> None:
        if self._perp_symbols and time.time() - self._metadata_fetched_at < METADATA_TTL:
            return
        try:
            session = await self._get_session()
            async with session.get(f"{REST_BASE}/api/v1/contract/detail") as r:
                d = await r.json()
            self._perp_symbols.clear()
            for item in d.get("data") or []:
                if item.get("quoteCoin") != "USDT" or item.get("state") != 0:
                    continue
                sym = item.get("symbol")
                if not sym:
                    continue
                self._perp_symbols.add(sym)
            self._metadata_fetched_at = time.time()
            logger.info(f"[mexc] metadata 更新：{len(self._perp_symbols)} USDT 合約")
        except Exception as e:
            logger.warning(f"[mexc] contract detail 取得失敗: {e}")

    async def _ensure_intervals(self) -> None:
        """從批次 funding_rate 端點取每個合約真實結算週期（collectCycle, 小時）。
        detail 端點不含週期欄位；此端點一次回全市場，含 collectCycle（1/4/8/24）。"""
        if self._funding_intervals and time.time() - self._intervals_fetched_at < INTERVAL_TTL:
            return
        try:
            session = await self._get_session()
            async with session.get(f"{REST_BASE}/api/v1/contract/funding_rate") as r:
                d = await r.json()
            intervals = {}
            for item in d.get("data") or []:
                sym = item.get("symbol")
                cycle = item.get("collectCycle")
                if sym and cycle:
                    intervals[sym] = float(cycle)
            if intervals:
                self._funding_intervals = intervals
                self._intervals_fetched_at = time.time()
                logger.info(f"[mexc] 結算週期更新：{len(intervals)} 個合約")
        except Exception as e:
            logger.warning(f"[mexc] funding_rate 週期取得失敗: {e}")

    async def fetch_funding_rates(self) -> list[FundingRecord]:
        await self._ensure_metadata()
        await self._ensure_intervals()
        cache = _get_cache()
        await cache.ensure_started(list(self._perp_symbols))
        await cache.wait_warmup(timeout=10.0)

        records: list[FundingRecord] = []
        bid_ask_hit = 0
        for sym in sorted(self._perp_symbols):
            st = cache.get(sym)
            if not st:
                continue
            fr = _f(st.get("fundingRate"))
            if fr is None:
                continue
            unified = _ws_to_unified(sym)
            if not unified:
                continue
            bid = _f(st.get("bid1"))
            ask = _f(st.get("ask1"))
            if bid is not None or ask is not None:
                bid_ask_hit += 1
            mark = _f(st.get("fairPrice"))
            index = _f(st.get("indexPrice"))
            interval_h = self._funding_intervals.get(sym, DEFAULT_FUNDING_INTERVAL_H)
            # MEXC ticker 不含 nextSettleTime → 推算（每 interval 整點結算）
            ft = _next_settle_time(interval_h)
            records.append(FundingRecord(
                exchange=self.name,
                symbol=unified,
                funding_rate=fr,
                next_funding_rate=None,
                funding_time=ft,
                mark_price=mark,
                index_price=index,
                bid_price=bid,
                ask_price=ask,
                annual_rate=calc_annual_rate(fr, interval_h),
                funding_interval_h=interval_h,
            ))
        logger.info(
            f"[{self.name}] 取得 {len(records)} 筆費率（{len(self._perp_symbols)} USDT 合約），"
            f"bid/ask: {bid_ask_hit} 個（WS cache {cache.stats()}）"
        )
        return records

    async def fetch_funding_history(self, symbol: str, since: int = 0, limit: int = 100) -> list[dict]:
        sym = _unified_to_ws(symbol)
        params = {"symbol": sym, "page_num": "1", "page_size": str(min(limit, 100))}
        try:
            session = await self._get_session()
            async with session.get(f"{REST_BASE}/api/v1/contract/funding_rate/history", params=params) as r:
                d = await r.json()
            items = (d.get("data") or {}).get("resultList") or []
            entries = [
                {"timestamp": int(it["settleTime"]), "rate": float(it["fundingRate"])}
                for it in items
                if "settleTime" in it and "fundingRate" in it
            ]
            entries.sort(key=lambda e: e["timestamp"])
            return entries
        except Exception as e:
            logger.debug(f"[{self.name}] 歷史費率查詢失敗 {symbol}: {e}")
            return []

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

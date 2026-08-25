"""Bybit V5 USDT-M Linear Perpetual - 完全 WS 化（脫離 ccxt）

單一 WS 連線、mass subscribe `tickers.SYMBOL`：
- 一個 frame 含 mark / index / funding / bid1 / ask1 / fundingIntervalHour / nextFundingTime
- snapshot 是全量，delta 只含變動欄位 → 用 cache 做 state merge

REST 用於：cold start bootstrap (tickers 全市場) + 24h refresh instruments universe
            + 歷史費率查詢。
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

REST_BASE = "https://api.bybit.com"
WS_URL = "wss://stream.bybit.com/v5/public/linear"

DEFAULT_FUNDING_INTERVAL_H = 8
METADATA_TTL = 86400  # 24h
SUBSCRIBE_BATCH = 100  # Bybit 接受單 op 至少 100 args


class _BybitTickerCache:
    """單例：1 條 WS 訂閱所有 USDT-M perp 的 tickers。cache 內以 symbol → 完整 state 維護。"""

    def __init__(self):
        self._state: dict[str, dict] = {}  # symbol -> raw fields
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

    def get(self, symbol: str) -> dict | None:
        return self._state.get(symbol)

    def stats(self) -> int:
        return len(self._state)

    async def _bootstrap_rest(self) -> None:
        """REST /v5/market/tickers?category=linear 一次拿全市場 ticker。"""
        url = f"{REST_BASE}/v5/market/tickers?category=linear"
        try:
            async with make_session(15) as s:
                async with s.get(url) as r:
                    d = await r.json()
            items = (d.get("result") or {}).get("list") or []
            for item in items:
                sym = item.get("symbol")
                if not sym:
                    continue
                self._state[sym] = dict(item)
            logger.info(f"[bybit-ws] REST bootstrap：{len(self._state)} symbols")
            if self._state:
                self._ready.set()
        except Exception as e:
            logger.warning(f"[bybit-ws] REST bootstrap 失敗: {e}")

    async def _run(self) -> None:
        backoff = 1
        while True:
            try:
                logger.info(f"[bybit-ws] 連線 {WS_URL}，預計訂閱 {len(self._desired)} symbols")
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=15) as ws:
                    syms = sorted(self._desired)
                    for i in range(0, len(syms), SUBSCRIBE_BATCH):
                        batch = syms[i:i + SUBSCRIBE_BATCH]
                        await ws.send(json.dumps({
                            "op": "subscribe",
                            "args": [f"tickers.{s}" for s in batch],
                        }))
                        await asyncio.sleep(0.1)
                    logger.info(f"[bybit-ws] 已送出 {len(syms)} 個 ticker 訂閱請求")

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        if msg.get("op") == "subscribe":
                            if not msg.get("success"):
                                logger.warning(f"[bybit-ws] subscribe ack failure: {msg}")
                            continue
                        topic = msg.get("topic", "")
                        if not topic.startswith("tickers."):
                            continue
                        sym = topic.split(".", 1)[1]
                        data = msg.get("data")
                        if not isinstance(data, dict):
                            continue
                        # snapshot 全量覆蓋、delta 增量合併
                        kind = msg.get("type")
                        if kind == "snapshot":
                            self._state[sym] = dict(data)
                        else:
                            # delta：merge
                            cur = self._state.setdefault(sym, {})
                            cur.update(data)
                        if not self._ready.is_set():
                            self._ready.set()
                            backoff = 1
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[bybit-ws] 連線中斷 {backoff}s 後重連: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)


_cache: _BybitTickerCache | None = None


def _get_cache() -> _BybitTickerCache:
    global _cache
    if _cache is None:
        _cache = _BybitTickerCache()
    return _cache


def _ws_to_unified(s: str) -> str | None:
    """0GUSDT -> 0G/USDT:USDT"""
    if not s.endswith("USDT"):
        return None
    base = s[:-4]
    return f"{base}/USDT:USDT" if base else None


def _unified_to_ws(symbol: str) -> str:
    return symbol.replace("/", "").split(":", 1)[0]


def _f(x):
    try:
        return float(x) if x not in (None, "", "0") and x != 0 else (float(x) if x not in (None, "") else None)
    except (TypeError, ValueError):
        return None


def _f_strict(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


class BybitExchange(BaseExchange):
    """Bybit V5 USDT-M Linear Perpetual - WS-first"""

    name = "bybit"

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None
        self._perp_symbols: set[str] = set()
        self._funding_intervals: dict[str, float] = {}
        self._metadata_fetched_at: float = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = make_session(30)
        return self._session

    async def _ensure_metadata(self) -> None:
        if self._perp_symbols and time.time() - self._metadata_fetched_at < METADATA_TTL:
            return
        url = f"{REST_BASE}/v5/market/instruments-info?category=linear&limit=1000"
        try:
            session = await self._get_session()
            async with session.get(url) as r:
                d = await r.json()
            items = (d.get("result") or {}).get("list") or []
            self._perp_symbols.clear()
            self._funding_intervals.clear()
            for item in items:
                if (item.get("contractType") != "LinearPerpetual"
                        or item.get("status") != "Trading"
                        or item.get("quoteCoin") != "USDT"):
                    continue
                sym = item.get("symbol")
                if not sym:
                    continue
                self._perp_symbols.add(sym)
                # fundingInterval 單位是分鐘（60, 240, 480 等）
                try:
                    interval_min = float(item.get("fundingInterval", 480))
                    self._funding_intervals[sym] = interval_min / 60 if interval_min >= 60 else interval_min
                except (TypeError, ValueError):
                    self._funding_intervals[sym] = DEFAULT_FUNDING_INTERVAL_H
            self._metadata_fetched_at = time.time()
            logger.info(f"[bybit] metadata 更新：{len(self._perp_symbols)} PERP USDT")
        except Exception as e:
            logger.warning(f"[bybit] instruments-info 取得失敗: {e}")

    async def fetch_funding_rates(self) -> list[FundingRecord]:
        await self._ensure_metadata()
        cache = _get_cache()
        await cache.ensure_started(list(self._perp_symbols))
        await cache.wait_warmup(timeout=10.0)

        records: list[FundingRecord] = []
        bid_ask_hit = 0
        for sym in sorted(self._perp_symbols):
            state = cache.get(sym)
            if not state:
                continue
            fr = _f_strict(state.get("fundingRate"))
            if fr is None:
                continue
            unified = _ws_to_unified(sym)
            if not unified:
                continue
            mark = _f_strict(state.get("markPrice"))
            index = _f_strict(state.get("indexPrice"))
            bid = _f_strict(state.get("bid1Price"))
            ask = _f_strict(state.get("ask1Price"))
            if bid is not None or ask is not None:
                bid_ask_hit += 1
            # interval：優先用 WS 推送的 fundingIntervalHour（含 4h 等非標準）
            interval_h = self._funding_intervals.get(sym, DEFAULT_FUNDING_INTERVAL_H)
            try:
                ws_h = float(state.get("fundingIntervalHour")) if state.get("fundingIntervalHour") else None
                if ws_h:
                    interval_h = ws_h
            except (TypeError, ValueError):
                pass
            ft = None
            nft = state.get("nextFundingTime")
            if nft:
                try:
                    ft = datetime.fromtimestamp(int(nft) / 1000, timezone.utc)
                except (TypeError, ValueError):
                    pass
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
            f"[{self.name}] 取得 {len(records)} 筆費率（{len(self._perp_symbols)} PERP），"
            f"bid/ask: {bid_ask_hit} 個（WS cache {cache.stats()}）"
        )
        return records

    async def fetch_funding_history(self, symbol: str, since: int = 0, limit: int = 100) -> list[dict]:
        sym = _unified_to_ws(symbol)
        params = {"category": "linear", "symbol": sym, "limit": str(min(limit, 200))}
        if since:
            # Bybit V5 只帶 startTime 會回空，必須配對 endTime 才回傳區間資料
            params["startTime"] = str(int(since))
            params["endTime"] = str(int(datetime.now(timezone.utc).timestamp() * 1000))
        try:
            session = await self._get_session()
            async with session.get(f"{REST_BASE}/v5/market/funding/history", params=params) as r:
                d = await r.json()
            items = (d.get("result") or {}).get("list") or []
            entries = [
                {"timestamp": int(it["fundingRateTimestamp"]), "rate": float(it["fundingRate"])}
                for it in items
                if "fundingRateTimestamp" in it and "fundingRate" in it
            ]
            entries.sort(key=lambda e: e["timestamp"])
            return entries
        except Exception as e:
            logger.debug(f"[{self.name}] 歷史費率查詢失敗 {symbol}: {e}")
            return []

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

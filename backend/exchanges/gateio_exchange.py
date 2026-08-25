"""Gate.io USDT-margined Futures - 完全 WS 化（脫離 ccxt）

兩個 channel：
- futures.tickers     → mark_price, funding_rate, index_price
- futures.book_ticker → 最佳 bid/ask (b/a)

payload 接受 symbol list，單一 subscribe 訊息可塞所有 700+ symbols。
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

REST_BASE = "https://api.gateio.ws"
WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"

DEFAULT_FUNDING_INTERVAL_H = 8
# metadata（含 funding_interval / funding_next_apply）需跟上滿資費縮週期切換（如 COTI 8h→1h），
# 故 TTL 設短於掃描間隔（每輪都刷新），只多一支 contracts bulk 呼叫、成本可忽略。
# 1h：funding_interval（結算週期）與合約清單同一個 contracts endpoint，無法單獨拉。
# 結算週期會被動態調整（與 binance H 同款問題），故整包改 1h 重抓，及時反映週期變更（清單一起更新，成本可忽略）。
METADATA_TTL = 240


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


class _GateCache:
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
                async with s.get(f"{REST_BASE}/api/v4/futures/usdt/tickers") as r:
                    d = await r.json()
            for item in d:
                sym = item.get("contract")
                if not sym:
                    continue
                # REST 用 highest_bid/lowest_ask 命名，統一轉成 cache 內 bid/ask
                self._state[sym] = {
                    "bid": _f(item.get("highest_bid")),
                    "ask": _f(item.get("lowest_ask")),
                    "mark": _f(item.get("mark_price")),
                    "index": _f(item.get("index_price")),
                    "funding_rate": _f(item.get("funding_rate")),
                    "ts": time.time(),
                }
            logger.info(f"[gate-ws] REST bootstrap：{len(self._state)} symbols")
            if self._state:
                self._ready.set()
        except Exception as e:
            logger.warning(f"[gate-ws] bootstrap 失敗: {e}")

    async def _run(self) -> None:
        backoff = 1
        while True:
            try:
                logger.info(f"[gate-ws] 連線 {WS_URL}，預計訂閱 {len(self._desired)} symbols × 2 channels")
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=15) as ws:
                    payload = sorted(self._desired)
                    for ch in ("futures.tickers", "futures.book_ticker"):
                        await ws.send(json.dumps({
                            "time": int(time.time()),
                            "channel": ch,
                            "event": "subscribe",
                            "payload": payload,
                        }))
                        await asyncio.sleep(0.1)
                    logger.info(f"[gate-ws] 已送出 2 個 channel 的批次訂閱")

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        if msg.get("event") == "subscribe":
                            r = msg.get("result") or {}
                            if r.get("status") != "success":
                                logger.warning(f"[gate-ws] subscribe failed: {msg}")
                            continue
                        if msg.get("event") != "update":
                            continue
                        ch = msg.get("channel", "")
                        results = msg.get("result")
                        if results is None:
                            continue
                        if ch == "futures.tickers":
                            items = results if isinstance(results, list) else [results]
                            for d in items:
                                sym = d.get("contract")
                                if not sym:
                                    continue
                                cur = self._state.setdefault(sym, {})
                                fr = _f(d.get("funding_rate"))
                                if fr is not None:
                                    cur["funding_rate"] = fr
                                mk = _f(d.get("mark_price"))
                                if mk is not None:
                                    cur["mark"] = mk
                                ix = _f(d.get("index_price"))
                                if ix is not None:
                                    cur["index"] = ix
                                cur["ts"] = time.time()
                        elif ch == "futures.book_ticker":
                            d = results if isinstance(results, dict) else (results[0] if results else {})
                            sym = d.get("s")
                            if not sym:
                                continue
                            cur = self._state.setdefault(sym, {})
                            b = _f(d.get("b"))
                            a = _f(d.get("a"))
                            if b is not None:
                                cur["bid"] = b
                            if a is not None:
                                cur["ask"] = a
                            cur["ts"] = time.time()
                        if not self._ready.is_set():
                            self._ready.set()
                            backoff = 1
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[gate-ws] 連線中斷 {backoff}s 後重連: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)


_cache: _GateCache | None = None


def _get_cache() -> _GateCache:
    global _cache
    if _cache is None:
        _cache = _GateCache()
    return _cache


class GateioExchange(BaseExchange):
    name = "gateio"

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None
        self._perp_symbols: set[str] = set()
        self._funding_intervals: dict[str, float] = {}
        self._funding_next_apply: dict[str, int] = {}  # seconds
        self._delisting_syms: set[str] = set()  # 即將下架合約（保留顯示但標記 ⚠下架）
        self._metadata_fetched_at: float = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = make_session(30)
        return self._session

    async def _ensure_metadata(self) -> None:
        if self._perp_symbols and time.time() - self._metadata_fetched_at < METADATA_TTL:
            return
        try:
            session = await self._get_session()
            async with session.get(f"{REST_BASE}/api/v4/futures/usdt/contracts") as r:
                d = await r.json()
            self._perp_symbols.clear()
            self._funding_intervals.clear()
            self._funding_next_apply.clear()
            self._delisting_syms.clear()
            for item in d:
                sym = item.get("name")
                if not sym:
                    continue
                # 即將下架的合約不跳過：保留並標記，與其他所一致（前端顯示 ⚠下架），
                # 避免像 AERGO 這種有資費但被 Gate 標下架的幣整個消失。
                if item.get("in_delisting"):
                    self._delisting_syms.add(sym)
                self._perp_symbols.add(sym)
                fi_s = item.get("funding_interval", 28800)
                try:
                    self._funding_intervals[sym] = float(fi_s) / 3600
                except (TypeError, ValueError):
                    self._funding_intervals[sym] = DEFAULT_FUNDING_INTERVAL_H
                try:
                    self._funding_next_apply[sym] = int(item.get("funding_next_apply", 0))
                except (TypeError, ValueError):
                    pass
            self._metadata_fetched_at = time.time()
            logger.info(f"[gateio] metadata 更新：{len(self._perp_symbols)} USDT 合約")
        except Exception as e:
            logger.warning(f"[gateio] contracts 取得失敗: {e}")

    async def fetch_funding_rates(self) -> list[FundingRecord]:
        await self._ensure_metadata()
        cache = _get_cache()
        await cache.ensure_started(list(self._perp_symbols))
        await cache.wait_warmup(timeout=10.0)

        records: list[FundingRecord] = []
        bid_ask_hit = 0
        for sym in sorted(self._perp_symbols):
            st = cache.get(sym)
            if not st:
                continue
            fr = st.get("funding_rate")
            if fr is None:
                continue
            unified = _ws_to_unified(sym)
            if not unified:
                continue
            bid = st.get("bid")
            ask = st.get("ask")
            if bid is not None or ask is not None:
                bid_ask_hit += 1
            interval_h = self._funding_intervals.get(sym, DEFAULT_FUNDING_INTERVAL_H)
            ft = None
            apply_s = self._funding_next_apply.get(sym)
            if apply_s:
                # 若已過期，往下個 cycle 推
                now_s = time.time()
                while apply_s <= now_s:
                    apply_s += int(interval_h * 3600)
                ft = datetime.fromtimestamp(apply_s, timezone.utc)
            records.append(FundingRecord(
                exchange=self.name,
                symbol=unified,
                funding_rate=fr,
                next_funding_rate=None,
                funding_time=ft,
                mark_price=st.get("mark"),
                index_price=st.get("index"),
                bid_price=bid,
                ask_price=ask,
                annual_rate=calc_annual_rate(fr, interval_h),
                funding_interval_h=interval_h,
                is_delisting=sym in self._delisting_syms,
            ))
        logger.info(
            f"[{self.name}] 取得 {len(records)} 筆費率（{len(self._perp_symbols)} USDT 合約），"
            f"bid/ask: {bid_ask_hit} 個（WS cache {cache.stats()}）"
        )
        return records

    async def fetch_funding_history(self, symbol: str, since: int = 0, limit: int = 100) -> list[dict]:
        sym = _unified_to_ws(symbol)
        params = {"contract": sym, "limit": str(min(limit, 1000))}
        if since:
            params["from"] = str(int(since / 1000))
        try:
            session = await self._get_session()
            async with session.get(f"{REST_BASE}/api/v4/futures/usdt/funding_rate", params=params) as r:
                d = await r.json()
            entries = [
                {"timestamp": int(it["t"]) * 1000, "rate": float(it["r"])}
                for it in d
                if "t" in it and "r" in it
            ]
            entries.sort(key=lambda e: e["timestamp"])
            return entries
        except Exception as e:
            logger.debug(f"[{self.name}] 歷史費率查詢失敗 {symbol}: {e}")
            return []

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

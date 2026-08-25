"""CoinW USDT Perpetual - 完全 WS 化（funding_rate + depth）

WS singleton 訂閱所有 active USDT contracts 的 funding_rate + depth：
- funding_rate channel: 即時預測費率
- depth channel: 完整訂單簿（取 bids[0]/asks[0] 當 bid/ask）

REST：
- /v1/perpum/instruments：合約清單（含 settledPeriod 結算週期、preOffline 下架預告）
- /v1/perpumPublic/tickers：mark / index（fair_price / last_price）
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone, timedelta

import aiohttp
import websockets

from exchanges.base import BaseExchange
from exchanges._session import make_session
from models import FundingRecord
from services.normalizer import calc_annual_rate, normalize_rate_to_8h

logger = logging.getLogger(__name__)

REST_BASE = "https://api.coinw.com"
WS_URL = "wss://ws.futurescw.com/perpum"
DEFAULT_FUNDING_INTERVAL_H = 8


def _f(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _parse_settled_period(s: str) -> float:
    """settledPeriod 可能是 '8h' / '4h' / 8 / 4"""
    if isinstance(s, (int, float)):
        return float(s)
    if isinstance(s, str):
        st = s.strip().lower()
        if st.endswith("h"):
            try:
                return float(st[:-1])
            except ValueError:
                return DEFAULT_FUNDING_INTERVAL_H
        try:
            return float(st)
        except ValueError:
            return DEFAULT_FUNDING_INTERVAL_H
    return DEFAULT_FUNDING_INTERVAL_H


class _CoinwCache:
    """單例：1 條 WS 訂閱所有 active USDT contracts 的 funding_rate + depth。"""

    def __init__(self):
        self._fr: dict[str, float] = {}     # BASE -> rate
        self._book: dict[str, dict] = {}    # BASE -> {bid, ask, ts}
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._fr_ready = asyncio.Event()
        self._book_ready = asyncio.Event()
        self._desired: set[str] = set()

    async def ensure_started(self, bases: list[str]) -> None:
        async with self._lock:
            self._desired = set(b.lower() for b in bases)
            if self._task is None or self._task.done():
                self._task = asyncio.create_task(self._run())

    async def wait_warmup(self, timeout: float = 12.0) -> None:
        try:
            await asyncio.wait_for(
                asyncio.gather(self._fr_ready.wait(), self._book_ready.wait()),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            pass

    def get_rate(self, base_upper: str) -> float | None:
        return self._fr.get(base_upper)

    def get_book(self, base_upper: str) -> dict | None:
        return self._book.get(base_upper)

    def stats(self) -> tuple[int, int]:
        return len(self._fr), len(self._book)

    async def _run(self) -> None:
        backoff = 1
        while True:
            try:
                logger.info(f"[coinw-ws] 連線 {WS_URL}，預計訂閱 {len(self._desired)} bases × 2 channels")
                async with websockets.connect(WS_URL, ping_interval=None, close_timeout=5) as ws:
                    for base in sorted(self._desired):
                        for ch_type in ("funding_rate", "depth"):
                            await ws.send(json.dumps({
                                "event": "sub",
                                "params": {"biz": "futures", "type": ch_type, "pairCode": base},
                            }))
                            await asyncio.sleep(0.015)
                    logger.info(f"[coinw-ws] 已送出 {len(self._desired)*2} 訂閱請求")

                    async def _ping():
                        while True:
                            await asyncio.sleep(20)
                            try:
                                await ws.send(json.dumps({"event": "ping"}))
                            except Exception:
                                break
                    ping_task = asyncio.create_task(_ping())

                    try:
                        async for raw in ws:
                            if raw == "pong":
                                continue
                            try:
                                msg = json.loads(raw)
                            except Exception:
                                continue
                            if msg.get("channel") == "subscribe":
                                continue
                            data = msg.get("data")
                            if not data:
                                continue
                            msg_type = msg.get("type", "")
                            if msg_type == "funding_rate":
                                items = data if isinstance(data, list) else [data]
                                for item in items:
                                    n = (item.get("n") or "").upper()
                                    r = _f(item.get("r"))
                                    if n and r is not None:
                                        self._fr[n] = r
                                if not self._fr_ready.is_set() and len(self._fr) >= len(self._desired) * 0.7:
                                    self._fr_ready.set()
                                    backoff = 1
                            elif msg_type == "depth" or "asks" in (data if isinstance(data, dict) else {}):
                                if isinstance(data, dict):
                                    n = (data.get("n") or "").upper()
                                    bids = data.get("bids") or []
                                    asks = data.get("asks") or []
                                    if not n:
                                        continue
                                    bid = _f(bids[0]["p"]) if bids else None
                                    ask = _f(asks[0]["p"]) if asks else None
                                    if bid is None and ask is None:
                                        continue
                                    self._book[n] = {"bid": bid, "ask": ask, "ts": time.time()}
                                    if not self._book_ready.is_set() and len(self._book) >= len(self._desired) * 0.7:
                                        self._book_ready.set()
                                        backoff = 1
                    finally:
                        ping_task.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[coinw-ws] 連線中斷 {backoff}s 後重連: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)


_cache: _CoinwCache | None = None


def _get_cache() -> _CoinwCache:
    global _cache
    if _cache is None:
        _cache = _CoinwCache()
    return _cache


class CoinwExchange(BaseExchange):
    name = "coinw"

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = make_session(60)
        return self._session

    async def fetch_funding_rates(self) -> list[FundingRecord]:
        session = await self._get_session()
        # 1. instruments（含 settledPeriod / preOffline）
        async with session.get(f"{REST_BASE}/v1/perpum/instruments") as resp:
            data = await resp.json()
        contracts = data.get("data") or []
        active_contracts = [
            c for c in contracts
            if c.get("quote", "").upper() == "USDT"
            and c.get("status") in ("online", "preOffline")
        ]
        active_bases = [c["base"] for c in active_contracts]
        interval_map: dict[str, float] = {}
        delisting_map: dict[str, datetime | None] = {}
        for c in active_contracts:
            base = c["base"].upper()
            interval_map[base] = _parse_settled_period(c.get("settledPeriod", "8h"))
            if c.get("status") == "preOffline":
                ct = c.get("closeTime")
                delisting_map[base] = (
                    datetime.fromtimestamp(int(ct) / 1000, tz=timezone.utc) if ct else None
                )

        # 2. tickers (mark / index)
        ticker_map: dict[str, dict] = {}
        try:
            async with session.get(f"{REST_BASE}/v1/perpumPublic/tickers") as resp:
                if resp.status == 200:
                    tdata = await resp.json()
                    for t in tdata.get("data") or []:
                        base_coin = (t.get("base_coin") or "").upper()
                        if base_coin:
                            ticker_map[base_coin] = t
        except Exception as e:
            logger.warning(f"[coinw] tickers 查詢失敗: {e}")

        # 3. WS cache（funding_rate + depth 都從這拿）
        cache = _get_cache()
        await cache.ensure_started(active_bases)
        await cache.wait_warmup(timeout=12.0)

        # 4. 組裝
        records: list[FundingRecord] = []
        bid_ask_hit = 0
        for base in active_bases:
            base_upper = base.upper()
            rate = cache.get_rate(base_upper)
            if rate is None:
                continue
            rate_8h = normalize_rate_to_8h(rate, "coinw")
            symbol = f"{base_upper}/USDT:USDT"
            interval_h = interval_map.get(base_upper, DEFAULT_FUNDING_INTERVAL_H)
            ticker = ticker_map.get(base_upper) or {}
            mark = _f(ticker.get("fair_price"))
            index = _f(ticker.get("last_price"))
            # next settle time
            now = datetime.now(timezone.utc)
            interval_secs = int(interval_h * 3600)
            midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
            elapsed = int((now - midnight).total_seconds())
            next_secs = ((elapsed // interval_secs) + 1) * interval_secs
            funding_time = midnight + timedelta(seconds=next_secs)
            book = cache.get_book(base_upper) or {}
            bid = book.get("bid")
            ask = book.get("ask")
            if bid is not None or ask is not None:
                bid_ask_hit += 1
            records.append(FundingRecord(
                exchange=self.name,
                symbol=symbol,
                funding_rate=rate_8h,
                next_funding_rate=None,
                funding_time=funding_time,
                mark_price=mark,
                index_price=index,
                annual_rate=calc_annual_rate(rate_8h, interval_h),
                funding_interval_h=interval_h,
                bid_price=bid,
                ask_price=ask,
                is_delisting=base_upper in delisting_map,
                delisting_time=delisting_map.get(base_upper),
            ))
        fr_n, book_n = cache.stats()
        logger.info(
            f"[{self.name}] 取得 {len(records)} 筆費率（{len(active_bases)} active USDT），"
            f"bid/ask: {bid_ask_hit} 個（WS fr cache {fr_n}, book cache {book_n}）"
        )
        return records

    async def fetch_funding_history(self, symbol: str, since: int = 0, limit: int = 100) -> list[dict]:
        """CoinW 隱藏 API（futuresapi domain，需特定 headers 偽裝瀏覽器）"""
        instrument = symbol.split("/")[0].upper()
        try:
            async with make_session() as hist:
                async with hist.post(
                    "https://futuresapi.coinw.com/v1/futuresc/public/selectFundingRateHistory",
                    json={"instrument": instrument, "day": 7},
                    headers={
                        "x-requested-with": "XMLHttpRequest",
                        "Origin": "https://www.coinw.com",
                        "Referer": "https://www.coinw.com/",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    },
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as r:
                    rdata = await r.json(content_type=None)
            if rdata.get("code") != 0 or not rdata.get("data"):
                return []
            entries = []
            for item in rdata["data"]:
                # createdDate 格式："2026-03-01 08:00"（UTC+8）
                dt = datetime.strptime(item["createdDate"], "%Y-%m-%d %H:%M")
                dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
                ts = int(dt.timestamp() * 1000)
                if since and ts < since:
                    continue
                entries.append({"timestamp": ts, "rate": float(item["fundingRate"])})
            entries.sort(key=lambda e: e["timestamp"])
            return entries[-limit:]
        except Exception as e:
            logger.debug(f"[{self.name}] 歷史費率查詢失敗 {symbol}: {e}")
            return []

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

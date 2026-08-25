"""BitMart V2 Futures - 完全 WS 化（脫離 ccxt）

- REST /contract/public/details：bulk 一次拿合約 universe + 初始 funding/index/interval
- WS futures/ticker + futures/fundingRate（mass subscribe）：持續更新 bid/ask/mark/index/fundingRate
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

REST_BASE = "https://api-cloud-v2.bitmart.com"
WS_URL = "wss://openapi-ws-v2.bitmart.com/api?protocol=1.1"

DEFAULT_FUNDING_INTERVAL_H = 8
METADATA_TTL = 86400
BATCH = 100  # 單 subscribe 訊息塞多少 args


def _f(x):
    if x in (None, ""):
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _ws_to_unified(s: str) -> str | None:
    if not s.endswith("USDT"):
        return None
    base = s[:-4]
    return f"{base}/USDT:USDT" if base else None


def _unified_to_ws(symbol: str) -> str:
    return symbol.replace("/", "").split(":", 1)[0]


class _BitmartCache:
    def __init__(self):
        self._state: dict[str, dict] = {}  # sym -> {bid, ask, mark, index, funding_rate, ft_ms, ts}
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._ready = asyncio.Event()
        self._desired: set[str] = set()

    async def ensure_started(self, symbols: list[str]) -> None:
        async with self._lock:
            self._desired = set(symbols)
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

    async def _run(self) -> None:
        backoff = 1
        while True:
            try:
                logger.info(f"[bitmart-ws] 連線，預計訂閱 {len(self._desired)} symbols × 2 channels")
                async with websockets.connect(WS_URL, ping_interval=None) as ws:
                    syms = sorted(self._desired)
                    # 分批送 subscribe，每批 BATCH 個 args
                    for i in range(0, len(syms), BATCH):
                        chunk = syms[i:i + BATCH]
                        args = []
                        for s in chunk:
                            args.append(f"futures/ticker:{s}")
                            args.append(f"futures/fundingRate:{s}")
                        await ws.send(json.dumps({"action": "subscribe", "args": args}))
                        await asyncio.sleep(0.2)
                    logger.info(f"[bitmart-ws] 已送出 {(len(syms)+BATCH-1)//BATCH} 批訂閱")

                    async def _ping():
                        while True:
                            await asyncio.sleep(15)
                            try:
                                await ws.send(json.dumps({"action": "ping"}))
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
                            if "event" in msg or "errorCode" in msg:
                                if "errorCode" in msg:
                                    logger.warning(f"[bitmart-ws] error: {msg}")
                                continue
                            group = msg.get("group", "")
                            data = msg.get("data")
                            if not isinstance(data, dict):
                                continue
                            sym = data.get("symbol")
                            if not sym:
                                continue
                            cur = self._state.setdefault(sym, {})
                            if group.startswith("futures/ticker"):
                                b = _f(data.get("bid_price"))
                                a = _f(data.get("ask_price"))
                                mk = _f(data.get("mark_price"))
                                ix = _f(data.get("index_price"))
                                if b is not None: cur["bid"] = b
                                if a is not None: cur["ask"] = a
                                if mk is not None: cur["mark"] = mk
                                if ix is not None: cur["index"] = ix
                            elif group.startswith("futures/fundingRate"):
                                # nextFundingRate = 即將結算那一期; fundingRate = 已結算歷史
                                fr = _f(data.get("nextFundingRate"))
                                if fr is None:
                                    fr = _f(data.get("fundingRate"))
                                if fr is not None:
                                    cur["funding_rate"] = fr
                                nft = data.get("nextFundingTime")
                                if nft:
                                    try:
                                        cur["ft_ms"] = int(nft)
                                    except (TypeError, ValueError):
                                        pass
                            cur["ts"] = time.time()
                            if not self._ready.is_set():
                                self._ready.set()
                                backoff = 1
                    finally:
                        ping_task.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[bitmart-ws] 連線中斷 {backoff}s 後重連: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)


_cache: _BitmartCache | None = None


def _get_cache() -> _BitmartCache:
    global _cache
    if _cache is None:
        _cache = _BitmartCache()
    return _cache


class BitmartExchange(BaseExchange):
    name = "bitmart"

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None
        self._contracts: dict[str, dict] = {}  # symbol -> REST contract record
        self._metadata_fetched_at: float = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = make_session(30)
        return self._session

    async def _refresh_contracts(self) -> None:
        """details 每次 fetch 都重撈：funding/interval/index 都在這（WS 補新鮮度）"""
        try:
            session = await self._get_session()
            async with session.get(f"{REST_BASE}/contract/public/details") as r:
                d = await r.json()
            if d.get("code") != 1000:
                logger.warning(f"[bitmart] details code={d.get('code')} msg={d.get('message')}")
                return
            items = (d.get("data") or {}).get("symbols") or []
            self._contracts = {}
            for it in items:
                if (it.get("product_type") != 1
                        or it.get("quote_currency") != "USDT"
                        or it.get("status") != "Trading"):
                    continue
                sym = it.get("symbol")
                if sym:
                    self._contracts[sym] = it
            self._metadata_fetched_at = time.time()
        except Exception as e:
            logger.warning(f"[bitmart] details 取得失敗: {e}")

    async def fetch_funding_rates(self) -> list[FundingRecord]:
        await self._refresh_contracts()
        cache = _get_cache()
        await cache.ensure_started(list(self._contracts.keys()))
        await cache.wait_warmup(timeout=10.0)

        records: list[FundingRecord] = []
        bid_ask_hit = 0
        skipped_no_rate = 0
        for sym, it in self._contracts.items():
            ws_state = cache.get(sym) or {}
            # funding rate：WS 優先，REST fallback
            rate = ws_state.get("funding_rate")
            if rate is None:
                rate = _f(it.get("expected_funding_rate")) or _f(it.get("funding_rate"))
            if rate is None:
                skipped_no_rate += 1
                continue
            base = it.get("base_currency") or sym.replace("USDT", "")
            unified = f"{base.upper()}/USDT:USDT"
            interval_h = float(it.get("funding_interval_hours") or DEFAULT_FUNDING_INTERVAL_H)
            # funding time：WS ft_ms 更新鮮
            ft = None
            if ws_state.get("ft_ms"):
                ft = datetime.fromtimestamp(ws_state["ft_ms"] / 1000, tz=timezone.utc)
            elif it.get("funding_time"):
                try:
                    ft = datetime.fromtimestamp(int(it["funding_time"]) / 1000, tz=timezone.utc)
                except (TypeError, ValueError):
                    pass
            # 其他欄位 WS 優先，REST fallback
            mark = ws_state.get("mark") if ws_state.get("mark") is not None else _f(it.get("last_price"))
            index = ws_state.get("index") if ws_state.get("index") is not None else _f(it.get("index_price"))
            bid = ws_state.get("bid")
            ask = ws_state.get("ask")
            if bid is not None or ask is not None:
                bid_ask_hit += 1
            # delist_time
            delist_raw = it.get("delist_time")
            is_delisting = bool(delist_raw)
            delisting_time = None
            if delist_raw:
                try:
                    ts = int(delist_raw)
                    delisting_time = datetime.fromtimestamp(
                        ts / 1000 if ts > 1e12 else ts, tz=timezone.utc
                    )
                except (TypeError, ValueError):
                    pass
            records.append(FundingRecord(
                exchange=self.name,
                symbol=unified,
                funding_rate=rate,
                next_funding_rate=None,
                funding_time=ft,
                mark_price=mark,
                index_price=index,
                bid_price=bid,
                ask_price=ask,
                annual_rate=calc_annual_rate(rate, interval_h),
                funding_interval_h=interval_h,
                is_delisting=is_delisting,
                delisting_time=delisting_time,
            ))
        logger.info(
            f"[{self.name}] 取得 {len(records)} 筆費率（{len(self._contracts)} active，略過 {skipped_no_rate}），"
            f"bid/ask: {bid_ask_hit}（WS cache {cache.stats()}）"
        )
        return records

    async def fetch_funding_history(self, symbol: str, since: int = 0, limit: int = 100) -> list[dict]:
        bm_sym = _unified_to_ws(symbol)
        session = await self._get_session()
        try:
            async with session.get(
                f"{REST_BASE}/contract/public/funding-rate-history",
                params={"symbol": bm_sym, "limit": min(limit, 100)},
            ) as r:
                d = await r.json()
        except Exception as e:
            logger.debug(f"[{self.name}] 歷史費率 {symbol} 失敗: {e}")
            return []
        items = (d.get("data") or {}).get("list") or d.get("data") or []
        entries = []
        for it in items:
            ts = it.get("funding_time") or it.get("timestamp")
            r = it.get("funding_rate") or it.get("rate")
            if ts is None or r is None:
                continue
            try:
                entries.append({"timestamp": int(ts), "rate": float(r)})
            except (TypeError, ValueError):
                continue
        entries.sort(key=lambda e: e["timestamp"])
        return entries[-limit:]

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

"""OKX V5 USDT-margined SWAP - 完全 WS 化（脫離 ccxt）

單一 WS 連線，每 symbol 訂閱 4 channels：
- tickers      → bid/ask (askPx, bidPx)
- mark-price   → mark (markPx)
- index-tickers → index (idxPx) — instId 為 BTC-USDT（無 -SWAP 後綴）
- funding-rate → fundingRate, nextFundingTime, prevFundingTime（推算 interval）

REST bootstrap：tickers / mark-price / index-tickers 都有 bulk endpoint，
funding-rate 沒 bulk，靠 WS 訂閱時的初次推送（OKX 訂閱即推一筆）。
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

REST_BASE = "https://www.okx.com"
WS_URL = "wss://ws.okx.com:8443/ws/v5/public"

DEFAULT_FUNDING_INTERVAL_H = 8
METADATA_TTL = 86400
SUBSCRIBE_BATCH = 200  # OKX 容許單 op 多 args


def _swap_to_index(inst_id: str) -> str:
    """BTC-USDT-SWAP -> BTC-USDT"""
    if inst_id.endswith("-SWAP"):
        return inst_id[:-5]
    return inst_id


def _swap_to_unified(inst_id: str) -> str | None:
    """BTC-USDT-SWAP -> BTC/USDT:USDT"""
    parts = inst_id.split("-")
    if len(parts) != 3 or parts[2] != "SWAP":
        return None
    return f"{parts[0]}/{parts[1]}:{parts[1]}"


def _unified_to_swap(symbol: str) -> str:
    base, rest = symbol.split("/", 1)
    quote = rest.split(":", 1)[0]
    return f"{base}-{quote}-SWAP"


def _f(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


class _OKXCache:
    """單例：1 條 WS 訂閱所有 SWAP USDT 的 4 channels。"""

    def __init__(self):
        self._state: dict[str, dict] = {}  # SWAP instId -> {bid, ask, mark, index, funding_rate, ft_ms, prev_ft_ms, ts}
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._ready = asyncio.Event()
        self._funding_ready = asyncio.Event()
        self._desired: set[str] = set()
        self._bootstrap_done = False

    async def ensure_started(self, swap_inst_ids: list[str]) -> None:
        async with self._lock:
            self._desired = set(swap_inst_ids)
            if not self._bootstrap_done:
                await self._bootstrap_rest()
                self._bootstrap_done = True
            if self._task is None or self._task.done():
                self._task = asyncio.create_task(self._run())

    async def wait_warmup(self, timeout: float = 12.0) -> None:
        """等到 funding-rate 推送涵蓋 desired 70% 以上才放行（OKX 訂閱即推但需 ~5-8 秒）。"""
        try:
            await asyncio.wait_for(self._funding_ready.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass

    def get(self, swap_inst_id: str) -> dict | None:
        return self._state.get(swap_inst_id)

    def stats(self) -> int:
        return len(self._state)

    def _upd(self, sid: str, **kwargs) -> None:
        cur = self._state.setdefault(sid, {})
        for k, v in kwargs.items():
            if v is not None:
                cur[k] = v
        cur["ts"] = time.time()

    async def _bootstrap_rest(self) -> None:
        """三條 bulk REST 一次填滿 cache。"""
        async with make_session(15) as s:
            # tickers bulk
            try:
                async with s.get(f"{REST_BASE}/api/v5/market/tickers?instType=SWAP") as r:
                    d = await r.json()
                for item in d.get("data") or []:
                    sid = item.get("instId")
                    if not sid or not sid.endswith("-SWAP"):
                        continue
                    self._upd(sid, bid=_f(item.get("bidPx")), ask=_f(item.get("askPx")))
            except Exception as e:
                logger.warning(f"[okx-ws] tickers bootstrap 失敗: {e}")
            # mark-price bulk
            try:
                async with s.get(f"{REST_BASE}/api/v5/public/mark-price?instType=SWAP") as r:
                    d = await r.json()
                for item in d.get("data") or []:
                    sid = item.get("instId")
                    if not sid or not sid.endswith("-SWAP"):
                        continue
                    self._upd(sid, mark=_f(item.get("markPx")))
            except Exception as e:
                logger.warning(f"[okx-ws] mark-price bootstrap 失敗: {e}")
            # index-tickers bulk (BTC-USDT format) → map 回 SWAP
            try:
                async with s.get(f"{REST_BASE}/api/v5/market/index-tickers?quoteCcy=USDT") as r:
                    d = await r.json()
                idx_map = {item["instId"]: _f(item.get("idxPx")) for item in d.get("data") or [] if item.get("instId")}
                for sid in list(self._state.keys()):
                    base_quote = _swap_to_index(sid)
                    if base_quote in idx_map:
                        self._upd(sid, index=idx_map[base_quote])
            except Exception as e:
                logger.warning(f"[okx-ws] index-tickers bootstrap 失敗: {e}")
        logger.info(f"[okx-ws] REST bootstrap：{len(self._state)} symbols")
        if self._state:
            self._ready.set()

    async def _run(self) -> None:
        backoff = 1
        while True:
            try:
                logger.info(f"[okx-ws] 連線 {WS_URL}，預計訂閱 {len(self._desired)} SWAPs × 4 channels")
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=15) as ws:
                    args: list[dict] = []
                    for sid in sorted(self._desired):
                        args.append({"channel": "tickers", "instId": sid})
                        args.append({"channel": "mark-price", "instId": sid})
                        args.append({"channel": "funding-rate", "instId": sid})
                        args.append({"channel": "index-tickers", "instId": _swap_to_index(sid)})
                    for i in range(0, len(args), SUBSCRIBE_BATCH):
                        batch = args[i:i + SUBSCRIBE_BATCH]
                        await ws.send(json.dumps({"op": "subscribe", "args": batch}))
                        await asyncio.sleep(0.1)
                    logger.info(f"[okx-ws] 已送出 {len(args)} 個 channel 訂閱請求")

                    # OKX 用 raw ping/pong（text）
                    async def _ping():
                        while True:
                            await asyncio.sleep(25)
                            try:
                                await ws.send("ping")
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
                            if msg.get("event"):
                                if msg.get("event") == "error":
                                    logger.warning(f"[okx-ws] error: {msg.get('msg')} code={msg.get('code')}")
                                continue
                            arg = msg.get("arg") or {}
                            ch = arg.get("channel")
                            inst = arg.get("instId")
                            data = msg.get("data")
                            if not data or not isinstance(data, list):
                                continue
                            d = data[0]
                            if ch == "tickers":
                                self._upd(inst, bid=_f(d.get("bidPx")), ask=_f(d.get("askPx")))
                            elif ch == "mark-price":
                                self._upd(inst, mark=_f(d.get("markPx")))
                            elif ch == "index-tickers":
                                # inst 是 BTC-USDT，要映射回所有以它為 underlying 的 SWAP
                                sid = f"{inst}-SWAP"
                                if sid in self._state or sid in self._desired:
                                    self._upd(sid, index=_f(d.get("idxPx")))
                            elif ch == "funding-rate":
                                # OKX 欄位定義：
                                #   fundingTime: 即將結算的時間（我們要的「下次結算」）
                                #   nextFundingTime: 再下一次結算（fundingTime + interval）
                                #   prevFundingTime: 上次已結算的時間
                                ft = _f(d.get("fundingTime"))
                                next_ft = _f(d.get("nextFundingTime"))
                                pft = _f(d.get("prevFundingTime"))
                                self._upd(
                                    inst,
                                    funding_rate=_f(d.get("fundingRate")),
                                    ft_ms=int(ft) if ft else None,
                                    next_ft_ms=int(next_ft) if next_ft else None,
                                    prev_ft_ms=int(pft) if pft else None,
                                )
                                # funding-rate 涵蓋 70% 才視為熱身完成
                                if not self._funding_ready.is_set():
                                    fr_n = sum(1 for v in self._state.values() if v.get("funding_rate") is not None)
                                    if self._desired and fr_n >= len(self._desired) * 0.7:
                                        self._funding_ready.set()
                            if not self._ready.is_set():
                                self._ready.set()
                                backoff = 1
                    finally:
                        ping_task.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[okx-ws] 連線中斷 {backoff}s 後重連: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)


_cache: _OKXCache | None = None


def _get_cache() -> _OKXCache:
    global _cache
    if _cache is None:
        _cache = _OKXCache()
    return _cache


class OkxExchange(BaseExchange):
    name = "okx"

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None
        self._swap_ids: set[str] = set()
        self._metadata_fetched_at: float = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = make_session(30)
        return self._session

    async def _ensure_metadata(self) -> None:
        if self._swap_ids and time.time() - self._metadata_fetched_at < METADATA_TTL:
            return
        try:
            session = await self._get_session()
            async with session.get(f"{REST_BASE}/api/v5/public/instruments?instType=SWAP") as r:
                d = await r.json()
            self._swap_ids = {
                x["instId"] for x in d.get("data") or []
                if x.get("settleCcy") == "USDT" and x.get("state") == "live" and x.get("instId", "").endswith("-SWAP")
            }
            self._metadata_fetched_at = time.time()
            logger.info(f"[okx] metadata 更新：{len(self._swap_ids)} USDT SWAP")
        except Exception as e:
            logger.warning(f"[okx] instruments 取得失敗: {e}")

    async def fetch_funding_rates(self) -> list[FundingRecord]:
        await self._ensure_metadata()
        cache = _get_cache()
        await cache.ensure_started(list(self._swap_ids))
        await cache.wait_warmup(timeout=12.0)

        records: list[FundingRecord] = []
        bid_ask_hit = 0
        for sid in sorted(self._swap_ids):
            st = cache.get(sid)
            if not st or st.get("funding_rate") is None:
                continue
            unified = _swap_to_unified(sid)
            if not unified:
                continue
            bid = st.get("bid")
            ask = st.get("ask")
            if bid is not None or ask is not None:
                bid_ask_hit += 1
            # interval：fundingTime - prevFundingTime（即將結算 - 上次已結算 = 一個 cycle 長度）
            interval_h = DEFAULT_FUNDING_INTERVAL_H
            if st.get("ft_ms") and st.get("prev_ft_ms"):
                diff_h = (st["ft_ms"] - st["prev_ft_ms"]) / 3600000
                if 0.5 <= diff_h <= 24:
                    interval_h = diff_h
            ft = None
            if st.get("ft_ms"):
                ft = datetime.fromtimestamp(st["ft_ms"] / 1000, timezone.utc)
            rate = st["funding_rate"]
            records.append(FundingRecord(
                exchange=self.name,
                symbol=unified,
                funding_rate=rate,
                next_funding_rate=None,
                funding_time=ft,
                mark_price=st.get("mark"),
                index_price=st.get("index"),
                bid_price=bid,
                ask_price=ask,
                annual_rate=calc_annual_rate(rate, interval_h),
                funding_interval_h=interval_h,
            ))
        logger.info(
            f"[{self.name}] 取得 {len(records)} 筆費率（{len(self._swap_ids)} SWAP），"
            f"bid/ask: {bid_ask_hit} 個（WS cache {cache.stats()}）"
        )
        return records

    async def fetch_funding_history(self, symbol: str, since: int = 0, limit: int = 100) -> list[dict]:
        sid = _unified_to_swap(symbol)
        params = {"instId": sid, "limit": str(min(limit, 100))}
        if since:
            # ⚠ 必須用 before，不是 after。OKX 這兩個參數是【翻頁】語意不是區間端點：
            #   after  = 回傳「比這個 fundingTime 更早」的紀錄（往過去翻）
            #   before = 回傳「比這個 fundingTime 更新」的紀錄（往現在翻）← 我們要的 since
            # 原本傳 after=since，等於叫 OKX 回傳【視窗之前】的資料，
            # 呼叫端要的最近 9h / 24h / 3 天結算永遠抓不到。
            # 實證（2026-08-08）：KAITO 在 binance/bybit/bitget/gateio/mexc 都有 18 個
            # 結算點，OKX 只有 2 個（還是即時掃描寫進去的，不是這支 API 來的）；
            # 平均每幣結算點數 OKX 6.6 vs 其他家 12~15。改 before 後同視窗回 18 筆、
            # 範圍 08-05 20:00 ~ 08-08 16:00，與其他所完全一致。
            # 另實測 limit 不足時 before 是從【視窗起點往新的方向】截斷，正合所需。
            params["before"] = str(int(since))
        try:
            session = await self._get_session()
            async with session.get(f"{REST_BASE}/api/v5/public/funding-rate-history", params=params) as r:
                d = await r.json()
            items = d.get("data") or []
            entries = [
                {"timestamp": int(it["fundingTime"]), "rate": float(it["realizedRate"])}
                for it in items
                if "fundingTime" in it and "realizedRate" in it
            ]
            entries.sort(key=lambda e: e["timestamp"])
            return entries
        except Exception as e:
            logger.debug(f"[{self.name}] 歷史費率查詢失敗 {symbol}: {e}")
            return []

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

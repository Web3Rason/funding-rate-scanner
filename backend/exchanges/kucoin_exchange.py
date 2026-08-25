"""KuCoin Futures USDT-margined Perpetual - 完全 WS 化（脫離 ccxt）

即時資費/標記/指數/買一賣一全部走 WS，掃描時不再每輪打 REST：
- WS `/contractMarket/tickerV2`  → bid/ask（mass subscribe，每 topic 100 symbols）
- WS `/contract/instrument`      → mark.index.price（每秒）+ funding.rate（每分鐘）
- REST `/api/v1/contracts/active` 只在 metadata TTL 內撈一次（symbol 清單 + 結算週期）
  與 cache 冷啟動 bootstrap（種一次 funding/mark/index，補 WS 暖機前的空窗）

KuCoin 限制 400 subs/session，且每個 symbol 現在要 2 個訂閱（tickerV2 + instrument），
故 PER_CONN_LIMIT 砍半到 180（2×180=360 < 400），symbols 多時自動拆多條連線。
nextFundingTime WS 不推 → 用結算週期推算（實測 KuCoin 對齊 UTC 00:00，與 mexc 同手法）。

WS 連線需先 POST `bullet-public` 取 token。
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

REST_BASE = "https://api-futures.kucoin.com"
DEFAULT_FUNDING_INTERVAL_H = 8
# contracts/active 同時提供合約清單與每合約結算週期。KuCoin 的收費時間是用結算週期
# 反推（WS 不推 nextFundingTime），無獨立信號可校驗，故結算週期一旦改變（例滿資費 8h→1h）
# 只能靠重抓 metadata 更新。1h 仍會卡住舊週期最多 1 小時 → 縮到短於掃描間隔、每輪刷新。
METADATA_TTL = 240
BATCH = 100        # KuCoin 單 topic 最多 100 symbols
PER_CONN_LIMIT = 180  # 每 symbol 2 個訂閱（tickerV2+instrument），2×180=360 < 400 上限


def _f(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _ws_to_unified(s: str) -> str | None:
    """XBTUSDTM -> BTC/USDT:USDT  (KuCoin XBT = BTC)"""
    if not s.endswith("USDTM"):
        return None
    base = s[:-5]
    if not base:
        return None
    # XBT -> BTC (KuCoin 沿用 BitMEX 命名)
    if base == "XBT":
        base = "BTC"
    return f"{base}/USDT:USDT"


def _unified_to_ws(symbol: str) -> str:
    base, rest = symbol.split("/", 1)
    if base == "BTC":
        base = "XBT"
    return f"{base}USDTM"


def _next_settle_time(interval_h: float) -> datetime:
    """根據結算週期算下一個結算時間（實測 KuCoin 對齊 UTC 00:00）。"""
    now = datetime.now(timezone.utc)
    interval_s = int(interval_h * 3600)
    midnight_ts = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    elapsed = int(now.timestamp()) - midnight_ts
    next_cycle_offset = ((elapsed // interval_s) + 1) * interval_s
    return datetime.fromtimestamp(midnight_ts + next_cycle_offset, timezone.utc)


def _sym_from_topic(topic: str) -> str | None:
    """/contract/instrument:XBTUSDTM -> XBTUSDTM"""
    if not topic or ":" not in topic:
        return None
    return topic.rsplit(":", 1)[-1] or None


class _KucoinCache:
    """WS 全狀態 cache：bid/ask（tickerV2）+ mark/index（mark.index.price）+ funding（funding.rate）。

    KuCoin 限制 400 subs/session，超過要開多條連線分擔。
    """

    def __init__(self):
        self._state: dict[str, dict] = {}
        self._tasks: list[asyncio.Task] = []
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
            self._tasks = [t for t in self._tasks if not t.done()]
            if not self._tasks:
                # 拆成多條連線
                syms = sorted(self._desired)
                shards = [syms[i:i + PER_CONN_LIMIT] for i in range(0, len(syms), PER_CONN_LIMIT)]
                for i, shard in enumerate(shards):
                    self._tasks.append(asyncio.create_task(self._run(shard, i)))

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
        """冷啟動：用 contracts/active 種一次 funding/mark/index，補 WS 暖機前空窗
        （funding.rate WS 每分鐘才推一次，bootstrap 讓首輪掃描就有資費）。"""
        try:
            async with make_session(15) as s:
                async with s.get(f"{REST_BASE}/api/v1/contracts/active") as r:
                    d = await r.json()
            n = 0
            for item in d.get("data") or []:
                sym = item.get("symbol")
                if not sym:
                    continue
                cur = self._state.setdefault(sym, {})
                fr = _f(item.get("fundingFeeRate"))
                mk = _f(item.get("markPrice"))
                ix = _f(item.get("indexPrice"))
                if fr is not None:
                    cur["funding"] = fr
                if mk is not None:
                    cur["mark"] = mk
                if ix is not None:
                    cur["index"] = ix
                n += 1
            logger.info(f"[kucoin-ws] REST bootstrap：{n} symbols")
            if self._state:
                self._ready.set()
        except Exception as e:
            logger.warning(f"[kucoin-ws] bootstrap 失敗: {e}")

    async def _get_ws_endpoint(self) -> tuple[str, int]:
        """POST bullet-public → 拿 token + endpoint + pingInterval(ms)"""
        async with make_session(15) as s:
            async with s.post(f"{REST_BASE}/api/v1/bullet-public") as r:
                d = await r.json()
        token = d["data"]["token"]
        srv = d["data"]["instanceServers"][0]
        url = f"{srv['endpoint']}?token={token}&connectId={int(time.time() * 1000)}"
        return url, int(srv.get("pingInterval", 18000)) // 1000

    async def _subscribe(self, ws, topic_prefix: str, syms: list[str]) -> None:
        for i in range(0, len(syms), BATCH):
            chunk = syms[i:i + BATCH]
            await ws.send(json.dumps({
                "id": str(int(time.time() * 1000)),
                "type": "subscribe",
                "topic": topic_prefix + ",".join(chunk),
                "response": False,
            }))
            await asyncio.sleep(0.2)

    async def _run(self, syms: list[str], conn_id: int = 0) -> None:
        backoff = 1
        # 分片啟動錯開：多條分片同時開時，避免 bullet-public 取 token 請求同一瞬間打爆
        # KuCoin 被限流（回應無 data → KeyError 'data' → 全部重連）。
        if conn_id:
            await asyncio.sleep(conn_id * 0.6)
        while True:
            try:
                ws_url, ping_s = await self._get_ws_endpoint()
                logger.info(f"[kucoin-ws] 連線 #{conn_id}（ping {ping_s}s），預計訂閱 {len(syms)} symbols × 2 topic")
                async with websockets.connect(ws_url, ping_interval=None, ping_timeout=None) as ws:
                    # KuCoin 連上會先送 welcome
                    await asyncio.wait_for(ws.recv(), timeout=5)

                    async def _ping():
                        while True:
                            await asyncio.sleep(ping_s)
                            try:
                                await ws.send(json.dumps({"id": str(int(time.time() * 1000)), "type": "ping"}))
                            except Exception:
                                break
                    ping_task = asyncio.create_task(_ping())

                    await self._subscribe(ws, "/contractMarket/tickerV2:", syms)
                    await self._subscribe(ws, "/contract/instrument:", syms)
                    logger.info(f"[kucoin-ws] #{conn_id} 已送出 tickerV2 + instrument 訂閱")

                    try:
                        async for raw in ws:
                            try:
                                msg = json.loads(raw)
                            except Exception:
                                continue
                            t = msg.get("type")
                            if t in ("welcome", "ack", "pong"):
                                continue
                            if t == "error":
                                logger.warning(f"[kucoin-ws] error: {msg}")
                                continue
                            subject = msg.get("subject")
                            d = msg.get("data") or {}
                            if subject == "tickerV2":
                                sym = d.get("symbol")
                                if not sym:
                                    continue
                                cur = self._state.setdefault(sym, {})
                                b = _f(d.get("bestBidPrice"))
                                a = _f(d.get("bestAskPrice"))
                                if b is not None:
                                    cur["bid"] = b
                                if a is not None:
                                    cur["ask"] = a
                            elif subject == "mark.index.price":
                                sym = _sym_from_topic(msg.get("topic"))
                                if not sym:
                                    continue
                                cur = self._state.setdefault(sym, {})
                                mk = _f(d.get("markPrice"))
                                ix = _f(d.get("indexPrice"))
                                if mk is not None:
                                    cur["mark"] = mk
                                if ix is not None:
                                    cur["index"] = ix
                            elif subject == "funding.rate":
                                sym = _sym_from_topic(msg.get("topic"))
                                if not sym:
                                    continue
                                fr = _f(d.get("fundingRate"))
                                if fr is None:
                                    continue
                                self._state.setdefault(sym, {})["funding"] = fr
                                if not self._ready.is_set():
                                    self._ready.set()
                                    backoff = 1
                            else:
                                continue
                    finally:
                        ping_task.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[kucoin-ws] 連線 #{conn_id} 中斷 {backoff}s 後重連: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)


_cache: _KucoinCache | None = None


def _get_cache() -> _KucoinCache:
    global _cache
    if _cache is None:
        _cache = _KucoinCache()
    return _cache


class KucoinExchange(BaseExchange):
    name = "kucoinfutures"

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None
        self._perp_symbols: set[str] = set()
        self._intervals: dict[str, float] = {}  # sym -> 結算週期(小時)
        self._metadata_fetched_at: float = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = make_session(30)
        return self._session

    async def _ensure_metadata(self) -> None:
        """contracts/active 只在 TTL 內撈一次：取 USDT 永續清單 + 每合約結算週期。
        funding/mark/index 改由 WS 提供，不再每輪 REST。"""
        if self._perp_symbols and time.time() - self._metadata_fetched_at < METADATA_TTL:
            return
        try:
            session = await self._get_session()
            async with session.get(f"{REST_BASE}/api/v1/contracts/active") as r:
                d = await r.json()
            perp: set[str] = set()
            intervals: dict[str, float] = {}
            for item in d.get("data") or []:
                if (item.get("quoteCurrency") != "USDT"
                        or item.get("status") != "Open"
                        or item.get("type") != "FFWCSX"  # 線性永續
                        or item.get("isInverse")):
                    continue
                sym = item.get("symbol")
                if not sym:
                    continue
                perp.add(sym)
                interval_h = DEFAULT_FUNDING_INTERVAL_H
                gran_ms = item.get("fundingRateGranularity") or item.get("currentFundingRateGranularity")
                if gran_ms:
                    try:
                        h = float(gran_ms) / 3600000
                        if 0.5 <= h <= 24:
                            interval_h = h
                    except (TypeError, ValueError):
                        pass
                intervals[sym] = interval_h
            if perp:
                self._perp_symbols = perp
                self._intervals = intervals
                self._metadata_fetched_at = time.time()
                logger.info(f"[kucoin] metadata 更新：{len(perp)} USDT 合約")
        except Exception as e:
            logger.warning(f"[kucoin] contracts/active 取得失敗: {e}")

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
            fr = st.get("funding")
            if fr is None:
                continue
            unified = _ws_to_unified(sym)
            if not unified:
                continue
            mark = st.get("mark")
            index = st.get("index")
            bid = st.get("bid")
            ask = st.get("ask")
            if bid is not None or ask is not None:
                bid_ask_hit += 1
            interval_h = self._intervals.get(sym, DEFAULT_FUNDING_INTERVAL_H)
            # WS 不推 nextFundingTime → 用結算週期推算（KuCoin 對齊 UTC 00:00）
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
        now_ms = int(time.time() * 1000)
        # KuCoin 必須給 from / to 範圍且 to <= 現在；不指定就抓近 30 天
        from_ms = int(since) if since else now_ms - 30 * 86400000
        params = {"symbol": sym, "from": str(from_ms), "to": str(now_ms)}
        try:
            session = await self._get_session()
            async with session.get(f"{REST_BASE}/api/v1/contract/funding-rates", params=params) as r:
                d = await r.json()
            items = d.get("data") or []
            entries = [
                {"timestamp": int(it["timepoint"]), "rate": float(it["fundingRate"])}
                for it in items
                if "timepoint" in it and "fundingRate" in it
            ]
            entries.sort(key=lambda e: e["timestamp"])
            return entries[-limit:]
        except Exception as e:
            logger.debug(f"[{self.name}] 歷史費率查詢失敗 {symbol}: {e}")
            return []

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

"""Hyperliquid DEX - REST API（每小時結算）

bid/ask 走 WS bbo 訂閱（單一連線多 coin），不再對每個合約打訂單簿 REST，
避免在共用 IP weight pool 上跟其他打 HL 的程式互相波及限速。
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

import aiohttp
import websockets

from exchanges.base import BaseExchange
from exchanges._session import make_session
from models import FundingRecord
from services.normalizer import calc_annual_rate

logger = logging.getLogger(__name__)

API_URL = "https://api.hyperliquid.xyz/info"
WS_URL = "wss://api.hyperliquid.xyz/ws"
FUNDING_INTERVAL_H = 1  # Hyperliquid 每小時結算


class _HyperliquidL2Cache:
    """單例：1 條 WS 連線訂閱所有 coin 的 bbo（best bid/offer），把 top-of-book 快取在記憶體。

    - HL WS 限制：10 連線/IP、1000 訂閱、2000 outgoing msg/min；183 coin 完全在規格內
    - scanner 重啟時不重連 WS（singleton 跨實例存活），避免每次重啟都重訂閱 183 次
    """

    def __init__(self):
        self._cache: dict[str, dict] = {}      # coin → {"bid": float, "ask": float, "ts": float}
        self._subscribed: set[str] = set()      # 已送出 subscribe 的 coin
        self._owners: dict[str, set[str]] = {}  # owner_name → 該 owner 要訂閱的 coin set
        self._ws = None
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._subscribed_event = asyncio.Event()  # 初始 subscribe 全部送出後 set
        self._ready = asyncio.Event()           # 第一次拿到任何 bbo 推送就 set

    @property
    def _desired(self) -> set[str]:
        """所有 owner 想訂閱的 coin 聯集（同一條 WS 連線共用）"""
        result: set[str] = set()
        for s in self._owners.values():
            result |= s
        return result

    def get(self, coin: str) -> dict | None:
        return self._cache.get(coin)

    def stats(self) -> tuple[int, int]:
        """回 (已訂閱數, 已有資料數)"""
        return len(self._subscribed), len(self._cache)

    async def ensure_subscribed(self, coins: list[str], owner: str = "default",
                                wait_subscribe_timeout: float = 20.0) -> None:
        """更新 owner 的訂閱集合，啟動 WS 任務並等到初始 subscribe 全部送出再返回。

        多 owner 共用同一條 WS：HL 主 universe (owner='hl') 與 tradexyz (owner='tradexyz')
        可同時 ensure_subscribed 各自的 coin set，cache 用 coin 全名（'BTC' vs 'xyz:TSLA'）
        當 key，互不衝突。
        """
        async with self._lock:
            self._owners[owner] = set(coins)
            if self._task is None or self._task.done():
                self._subscribed_event.clear()
                self._ready.clear()
                self._task = asyncio.create_task(self._run())
        try:
            await asyncio.wait_for(self._subscribed_event.wait(), timeout=wait_subscribe_timeout)
        except asyncio.TimeoutError:
            pass

    async def wait_warmup(self, timeout: float = 5.0) -> None:
        """等到至少有一個 bbo 推送進來（subscribe 送完後通常 <1 秒）。逾時也回，不拋例外。"""
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass

    @staticmethod
    def _sub_msg(coin: str) -> str:
        return json.dumps({
            "method": "subscribe",
            "subscription": {"type": "bbo", "coin": coin},
        })

    @staticmethod
    def _unsub_msg(coin: str) -> str:
        return json.dumps({
            "method": "unsubscribe",
            "subscription": {"type": "bbo", "coin": coin},
        })

    async def _run(self) -> None:
        backoff = 1
        while True:
            try:
                if not self._desired:
                    await asyncio.sleep(1)
                    continue
                logger.info(f"[hyperliquid-ws] 連線 {WS_URL}，預計訂閱 {len(self._desired)} coin")
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=15) as ws:
                    self._ws = ws
                    self._subscribed.clear()
                    # 初始批量訂閱：HL 限 2000 msg/min（33/s）；每條間隔 40ms = 25/s，留 buffer
                    # event loop 沒爭用時若不 throttle 會一口氣送出被 HL 直接踢連線
                    for c in list(self._desired):
                        try:
                            await ws.send(self._sub_msg(c))
                            self._subscribed.add(c)
                            await asyncio.sleep(0.04)
                        except Exception as e:
                            logger.warning(f"[hyperliquid-ws] subscribe {c} 失敗: {e}")
                    logger.info(f"[hyperliquid-ws] 已送出 {len(self._subscribed)} 個 bbo 訂閱")
                    self._subscribed_event.set()  # 通知 ensure_subscribed 可以放行
                    # backoff 在收到第一筆 bbo 推送後才重置（見下方 _ready.set()），
                    # 避免訂閱送出後 server 立即 close 卻誤判為連線正常

                    while True:
                        # 動態同步 desired 變動
                        new = self._desired - self._subscribed
                        gone = self._subscribed - self._desired
                        for c in new:
                            try:
                                await ws.send(self._sub_msg(c))
                                self._subscribed.add(c)
                            except Exception:
                                pass
                        for c in gone:
                            try:
                                await ws.send(self._unsub_msg(c))
                            except Exception:
                                pass
                            self._subscribed.discard(c)
                            self._cache.pop(c, None)

                        raw = await ws.recv()
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(msg, dict):
                            continue
                        ch = msg.get("channel")
                        if ch != "bbo":
                            continue
                        data = msg.get("data") or {}
                        coin = data.get("coin")
                        # bbo.data = {coin, time, bbo:[bestBid|null, bestAsk|null]}，每檔 {px,sz,n}
                        bbo = data.get("bbo") or []
                        if not coin or len(bbo) < 2:
                            continue
                        best_bid = bbo[0]
                        best_ask = bbo[1]
                        bid_px = None
                        ask_px = None
                        if best_bid:
                            try:
                                bid_px = float(best_bid.get("px"))
                            except (TypeError, ValueError):
                                bid_px = None
                        if best_ask:
                            try:
                                ask_px = float(best_ask.get("px"))
                            except (TypeError, ValueError):
                                ask_px = None
                        if bid_px is None and ask_px is None:
                            continue
                        ts_ms = data.get("time") or 0
                        try:
                            ts_sec = float(ts_ms) / 1000.0
                        except (TypeError, ValueError):
                            ts_sec = 0.0
                        self._cache[coin] = {"bid": bid_px, "ask": ask_px, "ts": ts_sec}
                        if not self._ready.is_set():
                            self._ready.set()
                            backoff = 1  # 確實收到資料才視為連線健康，重設退避
            except asyncio.CancelledError:
                self._ws = None
                raise
            except Exception as e:
                logger.warning(f"[hyperliquid-ws] 連線中斷 {backoff}s 後重連: {e}")
                self._ws = None
                self._subscribed.clear()
                self._subscribed_event.clear()
                # bbo 無重連快照：清掉舊 cache，避免斷線期間漏掉的 BBO 變動殘留成過期價餵給掃描器；
                # 重連後由首筆 bbo 推送重新填回（冷門幣暫回 None，也比餵過期價安全）。
                self._cache.clear()
                # 重設 _ready：讓重連後首筆推送重新標記健康並把 backoff 歸 1（見上方 _ready.set()）
                self._ready.clear()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)


# 模組層單例：跨 HyperliquidExchange 實例（含 scanner 重啟）共用同一條 WS
_l2_cache: _HyperliquidL2Cache | None = None


def _get_l2_cache() -> _HyperliquidL2Cache:
    global _l2_cache
    if _l2_cache is None:
        _l2_cache = _HyperliquidL2Cache()
    return _l2_cache


class HyperliquidExchange(BaseExchange):
    """Hyperliquid DEX - 公開 Info API"""

    name = "hyperliquid"

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = make_session(30)
        return self._session

    async def _post(self, payload: dict) -> dict | list:
        session = await self._get_session()
        async with session.post(API_URL, json=payload) as resp:
            if resp.status != 200:
                raise Exception(f"Hyperliquid HTTP {resp.status}")
            return await resp.json()

    async def fetch_funding_rates(self) -> list[FundingRecord]:
        records = []

        # 1. 取得所有合約 metadata + 當前 funding/price（仍走 REST，整支 1 個請求）
        data = await self._post({"type": "metaAndAssetCtxs"})
        universe = data[0]["universe"]
        ctxs = data[1]

        # 過濾已下架合約
        active_pairs = [(u, ctx) for u, ctx in zip(universe, ctxs) if not u.get("isDelisted", False)]
        universe = [p[0] for p in active_pairs]
        ctxs = [p[1] for p in active_pairs]
        coins = [u["name"] for u in universe]

        # 2. 確保 WS 訂閱集合涵蓋所有 coin；cold start 等 5s 讓推送先進來
        cache = _get_l2_cache()
        await cache.ensure_subscribed(coins, owner="hl")
        await cache.wait_warmup(timeout=5.0)

        # 3. 組裝 records，bid/ask 直接讀 WS cache（沒拿到就 None）
        bid_ask_hit = 0
        for u, ctx in zip(universe, ctxs):
            coin = u["name"]
            funding_str = ctx.get("funding")
            if funding_str is None:
                continue

            # Hyperliquid funding rate 是每小時費率，直接使用原始值
            rate_1h = float(funding_str)

            mark_px = float(ctx.get("markPx", 0)) or None
            oracle_px = float(ctx.get("oraclePx", 0)) or None

            symbol = f"{coin}/USDT:USDT"

            # 計算下次結算時間（每小時整點）
            now = datetime.now(timezone.utc)
            next_hour = now.replace(minute=0, second=0, microsecond=0)
            if next_hour <= now:
                from datetime import timedelta
                next_hour += timedelta(hours=1)

            book = cache.get(coin) or {}
            bid = book.get("bid")
            ask = book.get("ask")
            if bid is not None or ask is not None:
                bid_ask_hit += 1

            records.append(FundingRecord(
                exchange=self.name,
                symbol=symbol,
                funding_rate=rate_1h,
                next_funding_rate=None,
                funding_time=next_hour,
                mark_price=mark_px,
                index_price=oracle_px,
                bid_price=bid,
                ask_price=ask,
                annual_rate=calc_annual_rate(rate_1h, FUNDING_INTERVAL_H),
                funding_interval_h=FUNDING_INTERVAL_H,
            ))

        subs, hits = cache.stats()
        logger.info(
            f"[{self.name}] 取得 {len(records)} 筆費率（{len(universe)} 合約），"
            f"bid/ask: {bid_ask_hit} 個（WS 已訂閱 {subs}、cache 有資料 {hits}）"
        )
        return records

    async def fetch_funding_history(self, symbol: str, since: int = 0, limit: int = 100) -> list[dict]:
        """取得歷史費率（已下架幣種跳過）"""
        coin = symbol.split("/")[0].upper()

        try:
            # 檢查是否已下架
            meta = await self._post({"type": "meta"})
            for asset in meta.get("universe", []):
                if asset.get("name", "").upper() == coin and asset.get("isDelisted", False):
                    return []

            payload = {"type": "fundingHistory", "coin": coin, "startTime": since or 0}
            data = await self._post(payload)

            entries = []
            for item in data:
                rate_1h = float(item.get("fundingRate", 0))
                entries.append({
                    "timestamp": item.get("time"),
                    "rate": rate_1h,
                })

            entries.sort(key=lambda e: e["timestamp"])
            return entries[-limit:]
        except Exception as e:
            logger.debug(f"[{self.name}] 歷史費率查詢失敗 {symbol}: {e}")
            return []

    async def close(self):
        # WS singleton 不在 close 時關掉，讓 cache 跨 scanner 重啟保留
        if self._session and not self._session.closed:
            await self._session.close()

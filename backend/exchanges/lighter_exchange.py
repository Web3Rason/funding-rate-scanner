"""Lighter DEX 永續 - WS 全市場快照（主站 + Robinhood Chain 兩個獨立部署）

這支檔案同時服務兩間所，它們是**各自獨立的交易所**（不同鏈、不同帳戶、不同市場清單），
只是 API 規格一模一樣，所以共用同一份實作：

    lighter      主站      mainnet.zklighter.elliot.ai   227 市場，幣為主
    lighter_rh   RH Chain  api.rh.lighter.xyz             40 市場，幾乎全是股票 perp
                                                          （SPY/QQQ/NVDA/ANTHROPIC…），手續費 0

實測依據（2026-08-15，兩個 host 都逐項驗過）：

- WS `market_stats/all` **一條訂閱**就給全部市場的 index/mark/best_bid/best_ask/
  current_funding_rate，所以不需要對每個市場各開一條（對照 hyperliquid 要逐 coin 訂 bbo）。
- REST 沒有任何「一次拿全市場買賣一價」的端點：orderBookDetails / exchangeStats / orderBooks
  都不含 bid/ask，只有 orderBookOrders?market_id=N 是單一市場。故 bid/ask 只能走 WS。

**資費單位（主站 210/210、RH 40/40 交叉驗證皆 0 誤差，勿改）**
    WS `current_funding_rate` = 每小時費率，單位是【百分比】
        BTC "0.0010" → 0.001%/h → 小數 1e-5
    REST /funding-rates 的 rate = 【8 小時等價】的【小數】
        BTC 8e-05 = 0.008%
    關係：current_funding_rate / 100 * 8 == /funding-rates 的 rate
    本專案 FundingRecord.funding_rate 要的是「每個結算週期的小數」，
    Lighter 每小時結算 → funding_rate = current_funding_rate / 100，funding_interval_h = 1。

    ⚠ WS 另有一個叫 `funding_rate` 的欄位，那是【上一小時已結算】的值，不是當前費率，別誤用。

**K 線拿不到（已知限制，兩個部署都一樣）**
    /api/v1/candlesticks 從本機網路一律回 CloudFront 403（mainnet 與 api.rh 兩個 host、
    urllib 與 aiohttp、補 Origin/Referer 全試過），且 ccxt 4.5.32 沒有 lighter。
    → 價差圖／溢價圖對這兩間所會沒有資料。已列入 _CCXT_OHLCV_BLOCKLIST，
      並在 _fetch_ohlcv_direct 留下正式端點的實作，哪天不擋了就會自動生效。
"""

import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta

import aiohttp
import websockets

from exchanges.base import BaseExchange
from exchanges._session import make_session
from models import FundingRecord
from services.normalizer import calc_annual_rate

logger = logging.getLogger(__name__)

FUNDING_INTERVAL_H = 1  # Lighter 每小時結算（兩個部署皆同）

# 兩個部署的網域。新增部署只要在這裡加一組 + 在檔尾多一個子類別。
MAINNET_REST = "https://mainnet.zklighter.elliot.ai/api/v1"
MAINNET_WS = "wss://mainnet.zklighter.elliot.ai/stream"
RH_REST = "https://api.rh.lighter.xyz/api/v1"
RH_WS = "wss://api.rh.lighter.xyz/stream"


def _f(x):
    if x is None or x == "":
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v


class _LighterStatsCache:
    """單例（每個部署一份）：1 條 WS 連線訂閱 `market_stats/all`，把全市場快照留在記憶體。

    Lighter 這條頻道會先推一份全量（dict：market_id → stats），之後只推有變動的市場，
    所以 cache 用 market_id 當 key 持續累積更新即可。

    scanner 重啟時不重連（singleton 跨實例存活），避免每次重啟都重連一次。
    """

    CHANNEL = "market_stats/all"

    def __init__(self, ws_url: str, name: str):
        self._ws_url = ws_url
        self._name = name
        self._cache: dict[str, dict] = {}   # market_id(str) → market_stats dict
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._ready = asyncio.Event()       # 收到第一批推送就 set

    def get_by_id(self, market_id) -> dict | None:
        return self._cache.get(str(market_id))

    def snapshot(self) -> dict[str, dict]:
        return dict(self._cache)

    def size(self) -> int:
        return len(self._cache)

    async def ensure_started(self) -> None:
        async with self._lock:
            if self._task is None or self._task.done():
                self._ready.clear()
                self._task = asyncio.create_task(self._run())

    async def wait_warmup(self, timeout: float = 8.0) -> None:
        """等第一批推送進來；逾時也回，不拋例外（讓呼叫端走 REST 退路）。"""
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass

    def _absorb(self, payload) -> int:
        """吃下 market_stats 訊息。兩種形狀都要接：
        - 單一市場：{"symbol": "BTC", "market_id": 1, ...}
        - 全量/增量：{"212": {...}, "3": {...}}
        """
        if not isinstance(payload, dict):
            return 0
        if "market_id" in payload and "symbol" in payload:
            self._cache[str(payload["market_id"])] = payload
            return 1
        n = 0
        for mid, v in payload.items():
            if isinstance(v, dict) and "symbol" in v:
                self._cache[str(mid)] = v
                n += 1
        return n

    async def _run(self) -> None:
        backoff = 1
        while True:
            try:
                logger.info(f"[{self._name}-ws] 連線 {self._ws_url}，訂閱 {self.CHANNEL}")
                # WS 一律用 websockets，不可套 make_session（見專案 CLAUDE.md）
                async with websockets.connect(
                    self._ws_url, ping_interval=20, ping_timeout=15, max_size=16 * 1024 * 1024
                ) as ws:
                    await ws.send(json.dumps({"type": "subscribe", "channel": self.CHANNEL}))
                    backoff = 1
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        if not str(msg.get("type", "")).endswith("market_stats"):
                            continue
                        if self._absorb(msg.get("market_stats")) and not self._ready.is_set():
                            logger.info(f"[{self._name}-ws] 首批快照到齊，{len(self._cache)} 個市場")
                            self._ready.set()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[{self._name}-ws] 斷線: {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# 每個部署一份 cache（key = ws_url），singleton 跨 scanner 重啟存活
_stats_caches: dict[str, _LighterStatsCache] = {}


def _get_stats_cache(ws_url: str, name: str) -> _LighterStatsCache:
    if ws_url not in _stats_caches:
        _stats_caches[ws_url] = _LighterStatsCache(ws_url, name)
    return _stats_caches[ws_url]


class _BaseLighterExchange(BaseExchange):
    """主站與 RH Chain 共用的實作；子類別只換 name / REST_BASE / WS_URL。"""

    name = "lighter"
    REST_BASE = MAINNET_REST
    WS_URL = MAINNET_WS

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None
        self._symbol_to_id: dict[str, int] = {}   # "BTC" → market_id（給歷史費率用）

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = make_session(30)
        return self._session

    async def _get(self, path: str, params: dict | None = None):
        session = await self._get_session()
        async with session.get(f"{self.REST_BASE}/{path}", params=params) as r:
            if r.status != 200:
                raise Exception(f"Lighter HTTP {r.status} on /{path}")
            return await r.json()

    @staticmethod
    def _next_settle() -> datetime:
        """Lighter 每小時整點結算"""
        now = datetime.now(timezone.utc)
        return now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

    async def _rest_fallback(self) -> dict[str, dict]:
        """WS 還沒暖機時的退路：用 REST 拼出等價快照（但沒有 bid/ask）。

        /funding-rates 的 rate 是 8 小時等價小數 → ×100/8 換回「每小時百分比」，
        好讓下游跟 WS 的 current_funding_rate 走同一條計算路徑。
        """
        out: dict[str, dict] = {}
        try:
            details = (await self._get("orderBookDetails")).get("order_book_details") or []
        except Exception as e:
            logger.warning(f"[{self.name}] REST 退路 orderBookDetails 失敗: {e}")
            return out
        by_id = {str(d.get("market_id")): d for d in details}

        try:
            frs = (await self._get("funding-rates")).get("funding_rates") or []
        except Exception as e:
            logger.warning(f"[{self.name}] REST 退路 funding-rates 失敗: {e}")
            return out

        for fr in frs:
            if fr.get("exchange") != "lighter":
                continue
            mid = str(fr.get("market_id"))
            d = by_id.get(mid)
            if not d:
                continue
            rate_8h = _f(fr.get("rate"))
            if rate_8h is None:
                continue
            out[mid] = {
                "symbol": d.get("symbol"),
                "market_id": fr.get("market_id"),
                "status": d.get("status"),
                "index_price": d.get("index_price"),
                "mark_price": d.get("mark_price"),
                "current_funding_rate": rate_8h * 100.0 / 8.0,  # → 每小時百分比
            }
        return out

    async def fetch_funding_rates(self) -> list[FundingRecord]:
        # 1. WS 是主要來源（唯一拿得到 bid/ask 的路）
        cache = _get_stats_cache(self.WS_URL, self.name)
        await cache.ensure_started()
        await cache.wait_warmup(timeout=8.0)
        stats = cache.snapshot()

        used_ws = bool(stats)
        if not used_ws:
            logger.warning(f"[{self.name}] WS 尚未暖機，本輪改用 REST 退路（無 bid/ask）")
            stats = await self._rest_fallback()

        # 2. 市場清單（順便記下 symbol → market_id，歷史費率要用）
        try:
            details = (await self._get("orderBookDetails")).get("order_book_details") or []
            self._symbol_to_id = {
                str(d["symbol"]).upper(): d["market_id"]
                for d in details if d.get("symbol") and d.get("market_id") is not None
            }
            status_by_id = {str(d.get("market_id")): d.get("status") for d in details}
        except Exception as e:
            logger.warning(f"[{self.name}] orderBookDetails 取得失敗，改用 WS 內的狀態: {e}")
            status_by_id = {}

        next_settle = self._next_settle()
        records: list[FundingRecord] = []
        bid_ask_hit = 0
        skipped_inactive = 0

        for mid, s in stats.items():
            symbol = str(s.get("symbol") or "").upper()
            if not symbol:
                continue

            status = status_by_id.get(mid) or s.get("status")
            if status is not None and status != "active":
                skipped_inactive += 1
                continue

            # current_funding_rate 是「每小時百分比」→ 除以 100 換成小數（見檔頭實測說明）
            cur_pct = _f(s.get("current_funding_rate"))
            if cur_pct is None:
                continue
            rate_1h = cur_pct / 100.0

            bid = _f(s.get("best_bid_price"))
            ask = _f(s.get("best_ask_price"))
            if bid is not None or ask is not None:
                bid_ask_hit += 1

            records.append(FundingRecord(
                exchange=self.name,
                symbol=f"{symbol}/USDT:USDT",
                funding_rate=rate_1h,
                next_funding_rate=None,
                funding_time=next_settle,
                mark_price=_f(s.get("mark_price")),
                index_price=_f(s.get("index_price")),
                bid_price=bid,
                ask_price=ask,
                annual_rate=calc_annual_rate(rate_1h, FUNDING_INTERVAL_H),
                funding_interval_h=FUNDING_INTERVAL_H,
            ))

        logger.info(
            f"[{self.name}] 取得 {len(records)} 筆費率"
            f"（來源 {'WS' if used_ws else 'REST 退路'}，快照 {len(stats)} 個市場，"
            f"跳過非 active {skipped_inactive}），bid/ask: {bid_ask_hit} 個"
        )
        return records

    async def fetch_funding_history(self, symbol: str, since: int = 0, limit: int = 100) -> list[dict]:
        """歷史費率：/fundings?resolution=1h 的 rate 是【每小時百分比】→ /100 換成小數"""
        base = symbol.split("/")[0].upper()
        market_id = self._symbol_to_id.get(base)
        if market_id is None:
            try:
                details = (await self._get("orderBookDetails")).get("order_book_details") or []
                self._symbol_to_id = {
                    str(d["symbol"]).upper(): d["market_id"]
                    for d in details if d.get("symbol") and d.get("market_id") is not None
                }
                market_id = self._symbol_to_id.get(base)
            except Exception as e:
                logger.debug(f"[{self.name}] 歷史費率取 market_id 失敗 {symbol}: {e}")
                return []
        if market_id is None:
            return []

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        start_ms = since or (now_ms - limit * 3600 * 1000)
        try:
            data = await self._get("fundings", {
                "market_id": market_id,
                "resolution": "1h",
                "start_timestamp": start_ms,
                "end_timestamp": now_ms,
                "count_back": limit,
            })
        except Exception as e:
            logger.debug(f"[{self.name}] 歷史費率查詢失敗 {symbol}: {e}")
            return []

        entries = []
        for item in data.get("fundings") or []:
            rate_pct = _f(item.get("rate"))
            ts = item.get("timestamp")
            if rate_pct is None or ts is None:
                continue
            entries.append({
                "timestamp": int(ts) * 1000,   # 這個端點回的是秒，本專案統一用毫秒
                "rate": rate_pct / 100.0,
            })
        entries.sort(key=lambda e: e["timestamp"])
        return entries[-limit:]

    async def close(self):
        # WS singleton 不在 close 時關掉，讓快照跨 scanner 重啟保留（與 hyperliquid 一致）
        if self._session and not self._session.closed:
            await self._session.close()


class LighterExchange(_BaseLighterExchange):
    """Lighter 主站（zkLighter mainnet）：227 個市場，以幣為主"""

    name = "lighter"
    REST_BASE = MAINNET_REST
    WS_URL = MAINNET_WS


class LighterRhExchange(_BaseLighterExchange):
    """Lighter on Robinhood Chain：與主站是**完全獨立**的部署（不同鏈、不同帳戶、不同市場）。

    40 個市場，幾乎全是股票 perp（SPY/QQQ/NVDA/COIN/ANTHROPIC…），maker/taker 手續費皆 0。
    與主站同名的幣（SOL、ZEC、HYPE…）在這裡是**另一個場子的報價**，兩者的價差／資費差是
    真實可套的，不要當成重複資料去重。
    """

    name = "lighter_rh"
    REST_BASE = RH_REST
    WS_URL = RH_WS

"""Bitget V2 USDT-FUTURES - REST 批量輪詢（脫離 ccxt）

【為什麼不是 WS】原本訂 ticker channel × 745 symbols，一條連線搞定所有欄位，看似漂亮，
但 py-spy 對正式環境取樣 320 秒（25,301 個 MainThread 樣本）發現它是全專案最大的 CPU 大戶：
    實測 6,608 msg/s、3,748 KB/s，而主掃描每 300 秒才讀一次 self._state
    → 超過 99.98% 的 TLS 解密 + deflate 解壓 + json.loads + dict 更新都是白做的
    → 單這一條連線約吃掉 25~30% 的一顆核心
    （bitget_exchange.py 舊第 129 行的 json.loads 是全專案最熱的單行，4.71%）
改成 REST /api/v2/mix/market/tickers 批量輪詢：一次拿全部 745 檔，
每 POLL_INTERVAL 秒 1 個請求就夠餵飽 300 秒的掃描與 60 秒的現貨搬磚。

【nextFundingTime 怎麼辦】REST tickers 唯獨沒有這個欄位（實測欄位表已確認），
而它原本有兩個用途：(1) 顯示下次收費時間 (2) 反推結算週期、偵測滿資費臨時縮週期。
改為由 contracts 端點的 fundInterval 對齊 UTC 邊界推算：
    下次結算 = 當前小時對齊 interval 的下界 + interval
實測 2026-08-11 14:22 UTC（1h/4h/8h 三種週期同時驗證，該時點具鑑別力）8/8 完全命中。
代價：週期變更的偵測從「即時」變成「最長 METADATA_TTL 秒」，故把 TTL 從 3600 降到 300，
讓它跟掃描同頻——contracts 也是一個批量請求，成本可忽略。
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone

import aiohttp

from exchanges.base import BaseExchange
from exchanges._session import make_session
from models import FundingRecord
from services.normalizer import calc_annual_rate, guess_interval_from_next_funding

logger = logging.getLogger(__name__)

REST_BASE = "https://api.bitget.com"

DEFAULT_FUNDING_INTERVAL_H = 8
# fundInterval（結算週期）與合約清單同一個 contracts endpoint，無法單獨拉。
# 結算週期會被交易所動態調整（滿資費時臨時縮成 1h），而改用 REST 輪詢後
# nextFundingTime 是由 fundInterval 推算的、無法再反過來當偵測訊號，
# 所以這個 TTL 就是週期變更的偵測延遲上限 → 壓到 300 秒與掃描同頻。
# 成本只是每輪多一個批量請求（745 筆一次拿完），可忽略。
# ⚠ 必須【小於】掃描週期(300s)，不能等於。_metadata_fetched_at 是在請求【完成後】才寫，
# 所以下一輪進來時 elapsed = 300 - 請求耗時 ≈ 299.x < 300 → 判定未過期 → 整整跳過一輪，
# 實際刷新變成 600 秒。正式環境 log 佐證：舊版 TTL=3600 時
# 「[bitget] metadata 更新」的實際間隔是穩定的 65 分鐘（15:35→16:40→17:45→18:50→19:55→21:00），
# 正好是 13×300s，而不是 60 分鐘。設 240 讓它每輪都真的刷新。
METADATA_TTL = 240
# REST 全量 tickers 輪詢間隔。主掃描 300 秒讀一次、現貨搬磚 60 秒讀一次，
# 10 秒代表報價最舊只會差 10 秒（舊版 WS 是即時，這是唯一的功能性代價）。
# 流量對比：一次約 200KB / 10 秒 = 20 KB/s，舊版 WS 是 3,748 KB/s → 1/187；
# 解析成本從「每秒 6,608 次小 json.loads」變成「每 10 秒 1 次大 json.loads」。
# Bitget 公開端點限流是每秒數十次，0.1 req/s 完全無壓力。
POLL_INTERVAL = 10
# 陳舊熔斷：輪詢連續失敗超過這麼久，就當作「這一輪沒有 bitget」而不是餵舊報價。
#
# ⚠ 這道防護在 WS 版是【意外白送】的：報價凍結時 nextFundingTime 也跟著凍結，
# 下游 _append_scan_to_history 算出的 last_settle 停在舊整點、被去重擋掉，不會產生假結算。
# 改成由 now() 推算 funding_time 之後這層保險就沒了 —— 舊報價會看起來永遠新鮮，
# 於是 (a) 每個結算週期都用凍結的舊費率寫一筆假資料進 _rate_history
#     (b) detect_arbitrage 拿 N 分鐘前的 bid/ask 去比其他所的即時報價 → 幻影價差
# 所以必須顯式熔斷。60 秒 = 6 次輪詢機會，短暫抖動不會誤觸。
STALE_AFTER = 60

# 週期校驗：只對「真的可能被臨時縮週期」的少數幣，額外打單筆 funding-time 拿交易所給的
# 真 nextFundingTime，用原本那套 guess_interval_from_next_funding 反推校驗。
#
# 為什麼需要：改用 fundInterval 推算 nextFundingTime 之後，兩者變成循環論證，
# 整條鏈只剩 fundInterval 一個推論起點，而它多久才反映排程變更沒人驗證過。
# 若落後超過 45 分鐘（_MEAT_SETTLE_LOOKBACK_MINUTES），碎肉流會直接【跳過整段結算】而非只是晚報。
# 交易所縮週期的觸發條件就是資費打到上下限，所以只查「資費極端」的那幾檔即可，
# 用不著為了 745 檔全查而把省下來的成本又吐回去（每輪最多 12 個單筆請求）。
INTERVAL_VERIFY_TOP_N = 12
INTERVAL_VERIFY_MIN_RATE = 0.002        # |資費| >= 0.2% 才值得查


def _f(x):
    if x is None or x == "":
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


def _next_funding_utc(interval_h: float, now: datetime) -> datetime | None:
    """依結算週期推算下次結算時間（對齊 UTC 整點邊界）。

    Bitget 的結算時刻固定落在 UTC 邊界：8h → 00/08/16、4h → 00/04/08/12/16/20、1h → 每小時。
    實測 2026-08-11 14:22 UTC 對 1h/4h/8h 三種週期各取樣，與官方 funding-time 端點 8/8 一致
    （該時點 1h 推得 15:00、4h 與 8h 推得 16:00，能區分不同週期，非巧合）。
    """
    try:
        h = int(interval_h)
    except (TypeError, ValueError):
        return None
    if h <= 0 or 24 % h != 0:
        return None                       # 非整除的週期無法對齊邊界，寧可不猜
    floor_h = now.hour - (now.hour % h)
    return now.replace(hour=floor_h, minute=0, second=0, microsecond=0) + timedelta(hours=h)


class _BitgetCache:
    """全市場 ticker 快取：單一背景任務每 POLL_INTERVAL 秒打一次 REST 批量端點。

    取代原本的 WS ticker 訂閱（見檔頭說明）。因為是輪詢而非推送，
    連線數固定 1、CPU 只在每次輪詢時付一次解析成本。
    """

    def __init__(self):
        self._state: dict[str, dict] = {}
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._ready = asyncio.Event()
        self._session: aiohttp.ClientSession | None = None
        self._last_ok: float = 0.0

    async def ensure_started(self, symbols: list[str]) -> None:
        # symbols 參數保留給呼叫端相容；REST 是全市場一次拿，不需要逐檔訂閱
        async with self._lock:
            if self._task is None or self._task.done():
                await self._poll_once()                       # 先同步抓一次，首輪掃描就有資料
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

    def is_stale(self) -> tuple[bool, float]:
        """(是否過期, 距上次成功幾秒)。從未成功過也算過期。"""
        if not self._last_ok:
            return True, float("inf")
        age = time.time() - self._last_ok
        return age > STALE_AFTER, age

    async def _get_session(self) -> aiohttp.ClientSession:
        # 專案鐵則：REST 一律走 make_session（共用 connector、per-host 上限）
        if self._session is None or self._session.closed:
            self._session = make_session(15)
        return self._session

    async def _poll_once(self) -> bool:
        try:
            session = await self._get_session()
            async with session.get(
                f"{REST_BASE}/api/v2/mix/market/tickers?productType=USDT-FUTURES"
            ) as r:
                if r.status != 200:
                    logger.warning(f"[bitget] tickers HTTP {r.status}")
                    return False
                d = await r.json()
        except Exception as e:
            logger.warning(f"[bitget] tickers 輪詢失敗: {e}")
            return False
        rows = d.get("data") or []
        if not rows:
            return False
        # 整批替換而非逐筆 update：下架的合約會自然消失，不會留下永遠不更新的殘影
        self._state = {item["symbol"]: item for item in rows if item.get("symbol")}
        self._last_ok = time.time()
        if self._state and not self._ready.is_set():
            self._ready.set()
            logger.info(f"[bitget] REST tickers 就緒：{len(self._state)} symbols")
        return True

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(POLL_INTERVAL)
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[bitget] 輪詢迴圈例外: {e}")


_cache: _BitgetCache | None = None


def _get_cache() -> _BitgetCache:
    global _cache
    if _cache is None:
        _cache = _BitgetCache()
    return _cache


class BitgetExchange(BaseExchange):
    name = "bitget"

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
        # ⚠ 先組進暫存、確認成功才整批換掉，絕不可以先 clear() 再填。
        # Bitget 的錯誤回應是 HTTP 4xx + 合法 JSON {"code":...,"data":null}，
        # r.json() 解得開、不會拋例外 → 舊寫法會把 _perp_symbols 清成 0 又補不回來，
        # 而且照樣更新 _metadata_fetched_at，該輪 bitget 直接回 0 筆（ticker 快取明明是滿的）。
        # TTL 從 3600 降到 240 後這個請求變成每輪都打，踩雷機率×12，非補不可。
        try:
            session = await self._get_session()
            async with session.get(f"{REST_BASE}/api/v2/mix/market/contracts?productType=USDT-FUTURES") as r:
                if r.status != 200:
                    logger.warning(f"[bitget] contracts HTTP {r.status}，保留上一版 metadata")
                    return
                d = await r.json()
            rows = d.get("data")
            if not rows:
                logger.warning(f"[bitget] contracts 回空（code={d.get('code')}），保留上一版 metadata")
                return
            syms: set[str] = set()
            intervals: dict[str, float] = {}
            for item in rows:
                if (item.get("symbolStatus") != "normal"
                        or item.get("symbolType") != "perpetual"
                        or "USDT" not in (item.get("supportMarginCoins") or [])):
                    continue
                sym = item.get("symbol")
                if not sym:
                    continue
                syms.add(sym)
                try:
                    intervals[sym] = float(item.get("fundInterval", DEFAULT_FUNDING_INTERVAL_H))
                except (TypeError, ValueError):
                    intervals[sym] = DEFAULT_FUNDING_INTERVAL_H
            if not syms:
                logger.warning("[bitget] contracts 解析後 0 檔，保留上一版 metadata")
                return
            self._perp_symbols = syms
            self._funding_intervals = intervals
            self._metadata_fetched_at = time.time()      # 只有成功才更新
            logger.info(f"[bitget] metadata 更新：{len(self._perp_symbols)} PERP USDT")
        except Exception as e:
            logger.warning(f"[bitget] contracts 取得失敗: {e}")

    async def _verify_intervals(self, cache) -> dict[str, float]:
        """對資費極端的少數幣打 funding-time，用交易所給的真 nextFundingTime 反推週期。

        回傳 {symbol: 修正後週期}，只包含「推得比 fundInterval 更短」的（＝正在縮週期）。
        任何一支失敗都只是少一筆校驗，不影響主流程。
        """
        cands = []
        for sym in self._perp_symbols:
            iv = self._funding_intervals.get(sym, DEFAULT_FUNDING_INTERVAL_H)
            if iv <= 1:
                continue                      # 已經是最短週期，沒得再縮
            st = cache.get(sym)
            fr = _f((st or {}).get("fundingRate"))
            if fr is None or abs(fr) < INTERVAL_VERIFY_MIN_RATE:
                continue
            cands.append((abs(fr), sym, iv))
        if not cands:
            return {}
        cands.sort(reverse=True)
        cands = cands[:INTERVAL_VERIFY_TOP_N]

        session = await self._get_session()

        async def _one(sym: str, iv: float):
            try:
                async with session.get(
                    f"{REST_BASE}/api/v2/mix/market/funding-time",
                    params={"productType": "USDT-FUTURES", "symbol": sym},
                ) as r:
                    if r.status != 200:
                        return None
                    d = await r.json()
                row = (d.get("data") or [{}])[0]
                nft = row.get("nextFundingTime")
                if not nft:
                    return None
                guessed = guess_interval_from_next_funding(nft)
                return (sym, guessed) if guessed < iv else None
            except Exception:
                return None

        res = await asyncio.gather(*[_one(s, iv) for _, s, iv in cands], return_exceptions=True)
        out = {s: g for x in res if isinstance(x, tuple) for s, g in [x]}
        if out:
            logger.info(f"[bitget] 週期校驗：{len(out)} 檔實際已縮週期 {out}")
        return out

    async def fetch_funding_rates(self) -> list[FundingRecord]:
        await self._ensure_metadata()
        cache = _get_cache()
        await cache.ensure_started(list(self._perp_symbols))
        await cache.wait_warmup(timeout=10.0)

        stale, age = cache.is_stale()
        if stale:
            # 寧可這一輪沒有 bitget，也不要餵舊報價（見 STALE_AFTER 說明）。
            # 回空陣列時 _purge_delisted 會因 bitget 不在 live_exchanges 而保留它的 72h 歷史。
            logger.warning(f"[{self.name}] ticker 快取已陳舊 {age:.0f}s（>{STALE_AFTER}s），本輪跳過")
            return []

        verified = await self._verify_intervals(cache)

        records: list[FundingRecord] = []
        bid_ask_hit = 0
        now = datetime.now(timezone.utc)
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
            bid = _f(st.get("bidPr"))
            ask = _f(st.get("askPr"))
            if bid is not None or ask is not None:
                bid_ask_hit += 1
            mark = _f(st.get("markPrice"))
            index = _f(st.get("indexPrice"))
            # 校驗結果優先：fundInterval 可能落後於實際排程變更（見 INTERVAL_VERIFY_* 說明）
            interval_h = verified.get(sym) or self._funding_intervals.get(sym, DEFAULT_FUNDING_INTERVAL_H)
            # REST tickers 沒有 nextFundingTime，改由 fundInterval 對齊 UTC 邊界推算
            # （見檔頭；實測 1h/4h/8h 三種週期 8/8 命中）。
            ft = _next_funding_utc(interval_h, now)
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
        params = {"symbol": sym, "productType": "USDT-FUTURES", "pageSize": str(min(limit, 100))}
        try:
            session = await self._get_session()
            async with session.get(f"{REST_BASE}/api/v2/mix/market/history-fund-rate", params=params) as r:
                d = await r.json()
            items = d.get("data") or []
            entries = [
                {"timestamp": int(it["fundingTime"]), "rate": float(it["fundingRate"])}
                for it in items
                if "fundingTime" in it and "fundingRate" in it
            ]
            entries.sort(key=lambda e: e["timestamp"])
            return entries
        except Exception as e:
            logger.debug(f"[{self.name}] 歷史費率查詢失敗 {symbol}: {e}")
            return []

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

"""Binance USDT-M Futures - 完全 WS 化（脫離 ccxt）

資料來源：
- markPrice/index/funding：!markPrice@arr@1s WS（1 秒 1 筆，給即時告警 + 掃描共用）
- bid/ask：/fapi/v1/ticker/bookTicker bulk REST，每輪掃描刷新（S1：取代每秒數千筆的
  !bookTicker WS 以降 CPU；bulk 快照非逐 symbol 輪詢，符合精確化後的 WS 優先規則）

REST 也用於 24h refresh 的 metadata（exchangeInfo / fundingInfo）和歷史費率查詢。

注意：markPrice WS 走 /market/stream；bid/ask 走 /fapi/v1/ticker/bookTicker REST。
端點若未來下線，請參考 5005 backend/market_data.py 取代。
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
from services.normalizer import calc_annual_rate, guess_interval_from_next_funding

logger = logging.getLogger(__name__)

FAPI_BASE = "https://fapi.binance.com"
WS_MARK_URL = "wss://fstream.binance.com/market/stream?streams=!markPrice@arr@1s"

DEFAULT_FUNDING_INTERVAL_H = 8
METADATA_TTL = 86400  # 24h（PERPETUAL 合約清單變動慢）
FUNDING_INFO_TTL = 3600  # 1h（結算週期會動態變更，例如高波動幣臨時 4h→1h，需較短 TTL 才能即時反映）


class _BinanceTickerCache:
    """模組層單例：markPrice WS + bid/ask bulk REST 快照，跨 BinanceExchange 實例（含 scanner 重啟）共用。

    cache 結構：
      _book[SYMBOL] = {"bid": float|None, "ask": float|None, "ts": float}
      _mark[SYMBOL] = {"mark": ..., "index": ..., "funding_rate": ...,
                       "funding_time_ms": int|None, "ts": float}
    """

    def __init__(self):
        self._book: dict[str, dict] = {}
        self._mark: dict[str, dict] = {}
        self._mark_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._book_ready = asyncio.Event()
        self._mark_ready = asyncio.Event()
        self._bootstrap_done = False

    async def ensure_started(self) -> None:
        async with self._lock:
            # cold start：REST 一次全量填滿 book（bid/ask）與 mark 兩個 cache。
            #   mark 之後由 !markPrice@arr@1s WS 接手更新；book 則由每輪掃描 bulk REST 刷新（S1）。
            if not self._bootstrap_done:
                await asyncio.gather(self._bootstrap_book(), self._bootstrap_mark())
                self._bootstrap_done = True
            if self._mark_task is None or self._mark_task.done():
                self._mark_task = asyncio.create_task(self._run_mark())
            # bid/ask 改每輪掃描 bulk REST 刷新（fetch_funding_rates 內呼叫 _bootstrap_book），
            # 不再開全市場 !bookTicker WS（每秒數千筆、佔 WS 解析大半）→ S1 降 CPU

    async def _bootstrap_book(self) -> None:
        """REST /fapi/v1/ticker/bookTicker 一次拿全市場 bid/ask 填 cache。
        S1：cold start + 每輪掃描刷新（bid/ask 已無 WS 接手，改由 bulk 快照）。"""
        url = f"{FAPI_BASE}/fapi/v1/ticker/bookTicker"
        try:
            async with make_session(15) as s:
                async with s.get(url) as r:
                    data = await r.json()
            if not isinstance(data, list):
                # Binance 限流/錯誤時回 dict（非 list）：S1 後 book 已無 WS 兜底，靜默沿用舊快照
                # 會讓 degraded 狀態不可見，故明確告警（bid/ask 是即時交易價格來源）。
                logger.warning(f"[binance] bookTicker bulk REST 回應非 list（可能限流），沿用上輪快照: {str(data)[:120]}")
                return
            ts = time.time()
            for item in data:
                if not isinstance(item, dict):
                    continue
                sym = item.get("symbol") or ""
                if "_" in sym or not sym:
                    continue
                try:
                    bid = float(item["bidPrice"]) if item.get("bidPrice") is not None else None
                    ask = float(item["askPrice"]) if item.get("askPrice") is not None else None
                except (TypeError, ValueError, KeyError):
                    continue
                if bid is None and ask is None:
                    continue
                self._book[sym] = {"bid": bid, "ask": ask, "ts": ts}
            self._book_ready.set()
        except Exception as e:
            logger.warning(f"[binance] bookTicker bulk REST 刷新失敗（沿用上輪快照）: {e}")

    async def _bootstrap_mark(self) -> None:
        """REST /fapi/v1/premiumIndex 一次拿全市場 mark/index/fundingRate/nextFundingTime。"""
        url = f"{FAPI_BASE}/fapi/v1/premiumIndex"
        try:
            async with make_session(15) as s:
                async with s.get(url) as r:
                    data = await r.json()
            if not isinstance(data, list):
                return
            ts = time.time()
            for item in data:
                if not isinstance(item, dict):
                    continue
                sym = item.get("symbol") or ""
                if "_" in sym or not sym:
                    continue
                try:
                    mark = float(item["markPrice"]) if item.get("markPrice") is not None else None
                    index = float(item["indexPrice"]) if item.get("indexPrice") is not None else None
                    rate = float(item["lastFundingRate"]) if item.get("lastFundingRate") is not None else None
                    ft_ms = int(item["nextFundingTime"]) if item.get("nextFundingTime") else None
                except (TypeError, ValueError, KeyError):
                    continue
                self._mark[sym] = {
                    "mark": mark,
                    "index": index,
                    "funding_rate": rate,
                    "funding_time_ms": ft_ms,
                    "ts": ts,
                }
            logger.info(f"[binance-ws] markPrice REST bootstrap：{len(self._mark)} symbols")
            self._mark_ready.set()
        except Exception as e:
            logger.warning(f"[binance-ws] markPrice bootstrap 失敗（將靠 WS 慢慢補）: {e}")

    async def wait_warmup(self, timeout: float = 10.0) -> None:
        """等到 mark 與 book cache 都至少收到一次。逾時也回，不拋例外。"""
        try:
            await asyncio.wait_for(
                asyncio.gather(self._mark_ready.wait(), self._book_ready.wait()),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            pass

    def get_book(self, symbol: str) -> dict | None:
        return self._book.get(symbol)

    def get_mark(self, symbol: str) -> dict | None:
        return self._mark.get(symbol)

    def stats(self) -> tuple[int, int]:
        return len(self._mark), len(self._book)

    async def _run_mark(self) -> None:
        backoff = 1
        while True:
            try:
                logger.info(f"[binance-ws] markPrice 連線 {WS_MARK_URL}")
                async with websockets.connect(WS_MARK_URL, ping_interval=20, ping_timeout=15) as ws:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        data = msg.get("data") if isinstance(msg, dict) else None
                        if not isinstance(data, list):
                            continue
                        ts = time.time()
                        for item in data:
                            if not isinstance(item, dict):
                                continue
                            s = item.get("s") or ""
                            # 過濾 delivery 合約（symbol 帶下劃線如 BTCUSDT_260626）
                            if "_" in s or not s:
                                continue
                            try:
                                mark = float(item.get("p")) if item.get("p") is not None else None
                                index = float(item.get("i")) if item.get("i") is not None else None
                                rate = float(item.get("r")) if item.get("r") is not None else None
                                ft_ms = int(item.get("T")) if item.get("T") else None
                            except (TypeError, ValueError):
                                continue
                            self._mark[s] = {
                                "mark": mark,
                                "index": index,
                                "funding_rate": rate,
                                "funding_time_ms": ft_ms,
                                "ts": ts,
                            }
                        if not self._mark_ready.is_set() and self._mark:
                            self._mark_ready.set()
                            backoff = 1
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[binance-ws] markPrice 中斷 {backoff}s 後重連: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

_cache: _BinanceTickerCache | None = None


def _get_cache() -> _BinanceTickerCache:
    global _cache
    if _cache is None:
        _cache = _BinanceTickerCache()
    return _cache


def _ws_symbol_to_unified(s: str) -> str | None:
    """BTCUSDT -> BTC/USDT:USDT；BTCUSDC -> BTC/USDC:USDC。其他 quote 回 None 跳過。"""
    for q in ("USDT", "USDC"):
        if s.endswith(q):
            base = s[: -len(q)]
            if not base:
                return None
            return f"{base}/{q}:{q}"
    return None


def _unified_to_ws_symbol(symbol: str) -> str:
    """BTC/USDT:USDT -> BTCUSDT（給歷史費率查詢用）"""
    return symbol.replace("/", "").split(":", 1)[0]


class BinanceExchange(BaseExchange):
    """Binance USDT/USDC-M Futures - WS-first 實作"""

    name = "binance"

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None
        self._perp_symbols: set[str] = set()        # WS symbol (BTCUSDT) 形式
        self._funding_intervals: dict[str, float] = {}  # symbol -> hours
        self._metadata_fetched_at: float = 0
        self._funding_info_fetched_at: float = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = make_session(30)
        return self._session

    async def _ensure_metadata(self) -> None:
        """exchangeInfo（PERPETUAL universe）24h refresh；fundingInfo（per-symbol 結算週期）1h refresh。
        結算週期會被交易所動態調整（高波動幣臨時 4h→1h），用較短 TTL 才能及時反映，
        否則最久要等 24h 才更新，導致週期欄位顯示舊值。"""
        now = time.time()
        session = await self._get_session()
        # exchangeInfo: 取得 PERPETUAL+TRADING 合約清單（24h）
        if not self._perp_symbols or now - self._metadata_fetched_at >= METADATA_TTL:
            try:
                async with session.get(f"{FAPI_BASE}/fapi/v1/exchangeInfo") as r:
                    d = await r.json()
                    self._perp_symbols = {
                        x["symbol"] for x in d.get("symbols", [])
                        if x.get("contractType") in ("PERPETUAL", "TRADIFI_PERPETUAL")
                        and x.get("status") == "TRADING"
                    }
                self._metadata_fetched_at = now
            except Exception as e:
                logger.warning(f"[binance] exchangeInfo 取得失敗: {e}")
        # fundingInfo: 取得每幣種 fundingIntervalHours（多數 8h，部分 4h/1h）（1h）
        if not self._funding_intervals or now - self._funding_info_fetched_at >= FUNDING_INFO_TTL:
            try:
                async with session.get(f"{FAPI_BASE}/fapi/v1/fundingInfo") as r:
                    d = await r.json()
                    self._funding_intervals = {
                        item["symbol"]: float(item.get("fundingIntervalHours", DEFAULT_FUNDING_INTERVAL_H))
                        for item in d if isinstance(item, dict) and "symbol" in item
                    }
                self._funding_info_fetched_at = now
                logger.info(f"[binance] metadata 更新：{len(self._perp_symbols)} PERP，{len(self._funding_intervals)} 自訂 interval")
            except Exception as e:
                logger.warning(f"[binance] fundingInfo 取得失敗: {e}")

    async def fetch_funding_rates(self) -> list[FundingRecord]:
        await self._ensure_metadata()
        cache = _get_cache()
        await cache.ensure_started()
        await cache.wait_warmup(timeout=10.0)
        # bid/ask 用 bulk REST 快照（一次拿全市場）取代常駐 !bookTicker WS：掃描時點刷新、
        # 新鮮度零損失，省掉每秒數千筆 WS 解析（S1）。失敗沿用上輪快照。
        await cache._bootstrap_book()

        records: list[FundingRecord] = []
        bid_ask_hit = 0

        for ws_sym in sorted(self._perp_symbols):
            mark = cache.get_mark(ws_sym)
            if not mark or mark.get("funding_rate") is None:
                continue
            unified = _ws_symbol_to_unified(ws_sym)
            if not unified:
                continue

            book = cache.get_book(ws_sym) or {}
            bid = book.get("bid")
            ask = book.get("ask")
            if bid is not None or ask is not None:
                bid_ask_hit += 1

            interval_h = self._funding_intervals.get(ws_sym, DEFAULT_FUNDING_INTERVAL_H)
            ft = None
            ft_ms = mark.get("funding_time_ms")
            if ft_ms:
                ft = datetime.fromtimestamp(ft_ms / 1000, timezone.utc)
                # 交叉校驗：fundingInfo 快取最長有 1h 延遲，高波動幣臨時改週期（如 4h→1h）期間
                # 週期快取仍是舊值，但 nextFundingTime 已反映新節奏。用收費時間反推，
                # 推算值更短則覆寫，避免結算週期與下次收費時間不對齊。
                if interval_h > 1:
                    guessed_h = guess_interval_from_next_funding(ft_ms)
                    if guessed_h < interval_h:
                        interval_h = guessed_h

            rate = mark["funding_rate"]
            records.append(FundingRecord(
                exchange=self.name,
                symbol=unified,
                funding_rate=rate,
                next_funding_rate=None,
                funding_time=ft,
                mark_price=mark.get("mark"),
                index_price=mark.get("index"),
                bid_price=bid,
                ask_price=ask,
                annual_rate=calc_annual_rate(rate, interval_h),
                funding_interval_h=interval_h,
            ))

        mark_n, book_n = cache.stats()
        logger.info(
            f"[{self.name}] 取得 {len(records)} 筆費率（{len(self._perp_symbols)} PERP），"
            f"bid/ask: {bid_ask_hit} 個（WS mark cache {mark_n}、book cache {book_n}）"
        )
        return records

    async def fetch_funding_history(self, symbol: str, since: int = 0, limit: int = 100) -> list[dict]:
        """REST /fapi/v1/fundingRate（歷史費率不適合走 WS，每筆只在結算時產生一次）"""
        ws_sym = _unified_to_ws_symbol(symbol)
        params: dict = {"symbol": ws_sym, "limit": str(limit)}
        if since:
            params["startTime"] = str(int(since))
        try:
            session = await self._get_session()
            async with session.get(f"{FAPI_BASE}/fapi/v1/fundingRate", params=params) as r:
                data = await r.json()
            if not isinstance(data, list):
                return []
            entries = [
                {"timestamp": int(item["fundingTime"]), "rate": float(item["fundingRate"])}
                for item in data if "fundingTime" in item and "fundingRate" in item
            ]
            entries.sort(key=lambda e: e["timestamp"])
            return entries
        except Exception as e:
            logger.debug(f"[{self.name}] 歷史費率查詢失敗 {symbol}: {e}")
            return []

    async def close(self):
        # WS singleton 不在 close 時關掉，跨 scanner 重啟保留
        if self._session and not self._session.closed:
            await self._session.close()

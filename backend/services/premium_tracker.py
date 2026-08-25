"""Bybit 全市場溢價指數追蹤器

數據層設計（兩層）：
- 即時層：WS 訂閱全部 linear USDT 永續的 tickers.{symbol}（delta 合併），
  即時溢價 proxy = (mark - index) / index
- 焦點層：|proxy| Top N ∪ 資費使用率 >= 90%（貼上下限）的合約，
  每 5 秒並發抓官方溢價指數 1m K 線（含 60 分鐘 sparkline）

官方文檔依據（bybit-exchange.github.io/docs/v5）：
- GET /v5/market/tickers?category=linear：markPrice/indexPrice/lastPrice/
  fundingRate/nextFundingTime 一次回全市場
- GET /v5/market/instruments-info?category=linear：fundingInterval（分鐘）、
  upperFundingRate/lowerFundingRate（字串、可能不對稱）、limit<=1000 + cursor 分頁
- GET /v5/market/premium-index-price-kline：單一 symbol，list 為
  [startTime, open, high, low, close]，新→舊排序
- WS wss://stream.bybit.com/v5/public/linear：單連線 args 總長 <= 21000 字元，
  官方建議每 20 秒送 app-level {"op":"ping"}
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone

import aiohttp
import websockets

from services.arbitrage_detector import _normalize_symbol
from exchanges._session import make_session

logger = logging.getLogger(__name__)

REST_BASE = "https://api.bybit.com"
WS_URL = "wss://stream.bybit.com/v5/public/linear"

FOCUS_TOP_N = 20           # |proxy| 排名前 N 進焦點
FOCUS_CAP_USAGE = 0.9      # 資費使用率 >= 90% 也進焦點
FOCUS_MAX = 40             # 焦點集合上限（REST 量防呆）
FOCUS_INTERVAL_S = 5       # 焦點層輪詢間隔
REFRESH_INTERVAL_S = 1800  # 合約清單重整間隔（新上市/下市）
OFFICIAL_STALE_S = 120     # 官方溢價超過此秒數未更新即剔除
SPARK_MINUTES = 60         # sparkline 長度（1m K 線根數）
WS_ARGS_CHAR_BUDGET = 20000  # 單連線 args 總長預算（官方上限 21000）


class PremiumTracker:
    """Bybit 溢價指數追蹤（WS 即時 proxy + 官方溢價焦點輪詢）"""

    def __init__(self):
        self.is_running = False
        self.data: dict[str, dict] = {}       # symbol -> 即時 ticker 狀態
        self.limits: dict[str, dict] = {}     # symbol -> 資費上下限/週期
        self.official: dict[str, dict] = {}   # symbol -> 官方溢價 K 線快取
        self.ws_connected = 0                 # 已連線的 WS 條數
        self.last_bootstrap: datetime | None = None
        self._session: aiohttp.ClientSession | None = None
        self._tasks: list[asyncio.Task] = []
        self._ws_tasks: list[asyncio.Task] = []
        self._chunks: list[list[str]] = []

    # ─── 生命週期 ─────────────────────────────────────────

    async def start(self):
        self.is_running = True
        self._session = make_session(15)
        try:
            await self._bootstrap()
            self._spawn_ws_tasks()
        except Exception as e:
            logger.error(f"[溢價] bootstrap 失敗: {e}，每 30 秒重試")
            self._tasks.append(asyncio.create_task(self._bootstrap_retry_loop()))
        self._tasks.append(asyncio.create_task(self._focus_loop()))
        self._tasks.append(asyncio.create_task(self._refresh_loop()))
        logger.info(f"[溢價] 追蹤器啟動：{len(self.data)} 個合約")

    async def _bootstrap_retry_loop(self):
        """啟動時 bootstrap 失敗的快速重試（成功即收工）"""
        while self.is_running:
            await asyncio.sleep(30)
            try:
                await self._bootstrap()
                self._spawn_ws_tasks()
                return
            except Exception as e:
                logger.warning(f"[溢價] bootstrap 重試失敗: {e}")

    async def stop(self):
        self.is_running = False
        for t in self._tasks + self._ws_tasks:
            t.cancel()
        for t in self._tasks + self._ws_tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        if self._session and not self._session.closed:
            await self._session.close()

    # ─── REST bootstrap ──────────────────────────────────

    async def _get(self, path: str, params: dict) -> dict:
        async with self._session.get(f"{REST_BASE}{path}", params=params) as r:
            r.raise_for_status()
            d = await r.json()
        # Bybit 錯誤常是 HTTP 200 + retCode != 0（如限流 10006）
        if d.get("retCode") not in (0, None):
            raise RuntimeError(f"Bybit retCode={d.get('retCode')} {d.get('retMsg')}")
        return d

    async def _fetch_instruments(self) -> dict[str, dict]:
        """instruments-info：資費上下限（可能不對稱）+ 結算週期，cursor 分頁"""
        out = {}
        cursor = ""
        while True:
            params = {"category": "linear", "limit": "1000"}
            if cursor:
                params["cursor"] = cursor
            d = await self._get("/v5/market/instruments-info", params)
            result = d.get("result") or {}
            for it in result.get("list") or []:
                sym = it.get("symbol", "")
                if not sym.endswith("USDT") or it.get("status") != "Trading":
                    continue
                out[sym] = {
                    "interval_h": (int(it.get("fundingInterval") or 480)) / 60,
                    "upper": float(it.get("upperFundingRate") or 0) or None,
                    "lower": float(it.get("lowerFundingRate") or 0) or None,
                }
            cursor = result.get("nextPageCursor") or ""
            if not cursor:
                break
        return out

    async def _bootstrap(self):
        """抓合約清單 + 全市場 tickers 初始快照，計算 WS 訂閱分塊"""
        limits = await self._fetch_instruments()
        d = await self._get("/v5/market/tickers", {"category": "linear"})
        now = time.time()
        fresh: dict[str, dict] = {}
        for it in (d.get("result") or {}).get("list") or []:
            sym = it.get("symbol", "")
            if sym not in limits:
                continue
            prev = self.data.get(sym, {})
            fresh[sym] = {
                "last": _f(it.get("lastPrice")) or prev.get("last"),
                "mark": _f(it.get("markPrice")) or prev.get("mark"),
                "index": _f(it.get("indexPrice")) or prev.get("index"),
                "funding_rate": _f(it.get("fundingRate")),
                "next_funding_time": _i(it.get("nextFundingTime")),
                "ts": now,
            }
        if not fresh:
            # 空結果不覆蓋既有快取（限流/暫時故障時保住資料）
            raise RuntimeError("bootstrap 取得 0 個合約，保留舊快取")
        self.limits = limits
        self.data = fresh
        self.official = {s: v for s, v in self.official.items() if s in fresh}
        self._chunks = self._split_chunks(sorted(fresh.keys()))
        self.last_bootstrap = datetime.now(timezone.utc)
        logger.info(
            f"[溢價] bootstrap：{len(fresh)} 個 USDT 永續，"
            f"WS 分 {len(self._chunks)} 條連線"
        )

    @staticmethod
    def _split_chunks(symbols: list[str]) -> list[list[str]]:
        """依官方 21000 字元上限把 topics 分塊（一塊一條 WS 連線，通常 1 條）"""
        chunks, cur, cur_len = [], [], 0
        for sym in symbols:
            topic_len = len(f'"tickers.{sym}",')
            if cur and cur_len + topic_len > WS_ARGS_CHAR_BUDGET:
                chunks.append(cur)
                cur, cur_len = [], 0
            cur.append(sym)
            cur_len += topic_len
        if cur:
            chunks.append(cur)
        return chunks

    # ─── WS 即時層 ────────────────────────────────────────

    def _spawn_ws_tasks(self):
        for t in self._ws_tasks:
            t.cancel()
        self._ws_tasks = [
            asyncio.create_task(self._ws_loop(i, chunk))
            for i, chunk in enumerate(self._chunks)
        ]
        self.ws_connected = 0

    async def _ws_loop(self, chunk_idx: int, symbols: list[str]):
        """單條 WS 連線：訂閱本塊全部 tickers，斷線 3 秒重連"""
        while self.is_running:
            try:
                async with websockets.connect(
                    WS_URL, ping_interval=None, close_timeout=5, max_queue=4096
                ) as ws:
                    # 分批送訂閱（單則訊息別太大），args 總長已由分塊控制
                    for i in range(0, len(symbols), 100):
                        await ws.send(json.dumps({
                            "op": "subscribe",
                            "args": [f"tickers.{s}" for s in symbols[i:i + 100]],
                        }))
                    self.ws_connected += 1
                    logger.info(f"[溢價] WS#{chunk_idx} 已連線（{len(symbols)} topics）")
                    ping_task = asyncio.create_task(self._ws_ping(ws))
                    try:
                        async for raw in ws:
                            msg = json.loads(raw)
                            topic = msg.get("topic", "")
                            if topic.startswith("tickers."):
                                self._on_ticker(topic[8:], msg.get("data") or {})
                    finally:
                        ping_task.cancel()
                        self.ws_connected = max(0, self.ws_connected - 1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[溢價] WS#{chunk_idx} 斷線（{e}），3 秒後重連")
                await asyncio.sleep(3)

    @staticmethod
    async def _ws_ping(ws):
        """官方建議每 20 秒 app-level ping"""
        while True:
            await asyncio.sleep(20)
            await ws.send(json.dumps({"op": "ping"}))

    def _on_ticker(self, sym: str, d: dict):
        """tickers 是 delta 推送：缺欄位 = 未變，保留舊值"""
        cur = self.data.get(sym)
        if cur is None:
            return
        for src, dst in (("lastPrice", "last"), ("markPrice", "mark"),
                         ("indexPrice", "index")):
            v = _f(d.get(src))
            if v:
                cur[dst] = v
        v = _f(d.get("fundingRate"))
        if v is not None and d.get("fundingRate") != "":
            cur["funding_rate"] = v
        nft = _i(d.get("nextFundingTime"))
        if nft:
            cur["next_funding_time"] = nft
        cur["ts"] = time.time()

    # ─── 焦點層：官方溢價指數 ─────────────────────────────

    def _cap_usage(self, sym: str, rate) -> float | None:
        """資費使用率：當期資費距上/下限的比例（1.0 = 貼死）"""
        if rate is None:
            return None
        lim = self.limits.get(sym) or {}
        if rate < 0 and lim.get("lower"):
            return rate / lim["lower"]
        if rate > 0 and lim.get("upper"):
            return rate / lim["upper"]
        return 0.0

    def _proxy(self, st: dict) -> float | None:
        mark, index = st.get("mark"), st.get("index")
        if not mark or not index:
            return None
        return (mark - index) / index

    def _pick_focus(self) -> list[str]:
        """焦點 = |proxy| Top N ∪ 資費貼上下限者，上限 FOCUS_MAX"""
        scored = []
        pinned = []
        for sym, st in self.data.items():
            p = self._proxy(st)
            if p is not None:
                scored.append((abs(p), sym))
            u = self._cap_usage(sym, st.get("funding_rate"))
            if u is not None and u >= FOCUS_CAP_USAGE:
                pinned.append(sym)
        scored.sort(reverse=True)
        focus = list(dict.fromkeys(
            pinned + [sym for _, sym in scored[:FOCUS_TOP_N]]
        ))
        return focus[:FOCUS_MAX]

    async def _focus_loop(self):
        while self.is_running:
            try:
                focus = self._pick_focus()
                if focus:
                    sem = asyncio.Semaphore(10)
                    await asyncio.gather(
                        *[self._fetch_official(s, sem) for s in focus],
                        return_exceptions=True,
                    )
                # 剔除過期的官方溢價（已掉出焦點的）
                cutoff = time.time() - OFFICIAL_STALE_S
                self.official = {
                    s: v for s, v in self.official.items() if v["ts"] > cutoff
                }
            except Exception as e:
                logger.warning(f"[溢價] 焦點輪詢失敗: {e}")
            await asyncio.sleep(FOCUS_INTERVAL_S)

    async def _fetch_official(self, sym: str, sem: asyncio.Semaphore):
        """官方溢價指數 1m K 線：list 為新→舊，最後一根（最新分鐘）未收盤"""
        async with sem:
            try:
                d = await self._get("/v5/market/premium-index-price-kline", {
                    "category": "linear", "symbol": sym,
                    "interval": "1", "limit": str(SPARK_MINUTES),
                })
                rows = (d.get("result") or {}).get("list") or []
                if not rows:
                    return
                rows = rows[::-1]  # 轉為舊→新
                spark = [round(float(r[4]) * 100, 4) for r in rows]
                self.official[sym] = {
                    "premium_pct": spark[-1],
                    "spark": spark,
                    "kline_start": int(rows[-1][0]),  # 最新一根起始 ms（未收盤）
                    "ts": time.time(),
                }
            except Exception as e:
                logger.debug(f"[溢價] 官方K線失敗 {sym}: {e}")

    # ─── 合約清單重整（新上市/下市） ──────────────────────

    async def _refresh_loop(self):
        while self.is_running:
            await asyncio.sleep(REFRESH_INTERVAL_S)
            try:
                old = set(self.data.keys())
                await self._bootstrap()
                if set(self.data.keys()) != old:
                    logger.info("[溢價] 合約清單變動，重建 WS 訂閱")
                    self._spawn_ws_tasks()
            except Exception as e:
                logger.warning(f"[溢價] 清單重整失敗: {e}")

    # ─── 對外快照 ─────────────────────────────────────────

    def snapshot(self) -> list[dict]:
        now = time.time()
        rows = []
        for sym, st in self.data.items():
            p = self._proxy(st)
            rate = st.get("funding_rate")
            usage = self._cap_usage(sym, rate)
            lim = self.limits.get(sym) or {}
            off = self.official.get(sym)
            base = sym[:-4]  # 去掉 USDT 後綴
            rows.append({
                "symbol": sym,
                "base": base,
                # 正規化名稱（PLAYSOUT→PLAY 等），詳細面板要用它才查得到所有交易所
                "detail_symbol": _normalize_symbol(f"{base}/USDT:USDT", "bybit"),
                "last": st.get("last"),
                "premium_pct": round(p * 100, 4) if p is not None else None,
                "official_pct": off["premium_pct"] if off else None,
                "spark": off["spark"] if off else None,
                "funding_rate_pct": round(rate * 100, 4) if rate is not None else None,
                "upper_pct": round(lim["upper"] * 100, 3) if lim.get("upper") else None,
                "lower_pct": round(lim["lower"] * 100, 3) if lim.get("lower") else None,
                "cap_usage_pct": round(usage * 100, 1) if usage is not None else None,
                "interval_h": lim.get("interval_h"),
                "next_funding_time": st.get("next_funding_time"),
                "stale_s": round(now - st["ts"], 1) if st.get("ts") else None,
            })
        rows.sort(key=lambda r: abs(r["premium_pct"] or 0), reverse=True)
        return rows


def _f(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _i(v):
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None

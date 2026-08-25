# -*- coding: utf-8 -*-
"""Binance 全市場即時溢價追蹤（一條 WS !markPrice@arr 推全市場）。

溢價 = (markPrice − indexPrice) / indexPrice。對照 premium_tracker（Bybit）介面，
提供 snapshot()。Binance 的 markPrice 串流已含 index 與 funding，無需像 Bybit
另外輪詢官方溢價 K 線，故一條 WS 即可涵蓋全市場。

endpoint（實測 2026-06-17）：
  wss://fstream.binance.com/market/stream?streams=!markPrice@arr
  （Binance 2026-04-23 起把 markPrice 串流搬到 /market/stream，舊 /stream 已失效）
  每筆元素：{s:symbol, p:markPrice, i:indexPrice, r:fundingRate, T:nextFundingTime}
"""
import asyncio
import json
import logging
import time

import websockets

logger = logging.getLogger(__name__)

WS_URL = "wss://fstream.binance.com/market/stream?streams=!markPrice@arr"


class BinancePremiumTracker:
    """Binance 全市場即時溢價（單一 WS，自動重連）。"""

    def __init__(self):
        self.is_running = False
        self.data: dict[str, dict] = {}   # symbol -> {mark,index,premium,funding_rate,next_funding_time,ts}
        self.ws_connected = 0
        self._task: asyncio.Task | None = None

    async def start(self):
        self.is_running = True
        self._task = asyncio.create_task(self._ws_loop())
        logger.info("[BN溢價] Binance 即時溢價追蹤器啟動")

    async def stop(self):
        self.is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    async def _ws_loop(self):
        while self.is_running:
            try:
                async with websockets.connect(
                    WS_URL, ping_interval=20, ping_timeout=10, close_timeout=5, max_queue=4096
                ) as ws:
                    self.ws_connected = 1
                    logger.info("[BN溢價] WS 已連線（全市場 !markPrice@arr）")
                    async for raw in ws:
                        msg = json.loads(raw)
                        data = msg.get("data", msg)
                        if isinstance(data, list):
                            self._on_arr(data)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.ws_connected = 0
                logger.warning(f"[BN溢價] WS 斷線（{e}），3 秒後重連")
                await asyncio.sleep(3)
        self.ws_connected = 0

    def _on_arr(self, arr: list):
        now = time.time()
        for d in arr:
            sym = d.get("s", "")
            if not sym.endswith("USDT"):
                continue
            try:
                mark = float(d.get("p", 0) or 0)
                index = float(d.get("i", 0) or 0)
            except (ValueError, TypeError):
                continue
            if mark <= 0 or index <= 0:
                continue
            self.data[sym] = {
                "mark": mark,
                "index": index,
                "premium": (mark - index) / index,
                "funding_rate": float(d.get("r", 0) or 0),
                "next_funding_time": int(d.get("T", 0) or 0),
                "ts": now,
            }

    def snapshot(self) -> list[dict]:
        """對外快照，欄位對齊 premium_tracker.snapshot()（premium_pct / stale_s）。"""
        now = time.time()
        rows = []
        for sym, st in self.data.items():
            rows.append({
                "symbol": sym,
                "base": sym[:-4],
                "mark": st["mark"],
                "index": st["index"],
                "premium_pct": round(st["premium"] * 100, 4),
                "funding_rate_pct": round(st["funding_rate"] * 100, 4),
                "next_funding_time": st["next_funding_time"],
                "stale_s": round(now - st["ts"], 1),
            })
        rows.sort(key=lambda r: abs(r["premium_pct"]), reverse=True)
        return rows

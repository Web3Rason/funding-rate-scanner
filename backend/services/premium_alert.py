# -*- coding: utf-8 -*-
"""極端溢價即時告警：任一家（Bybit / Binance）|premium| ≥ 閾值 → TG 通知。

- 溢價來源：直接讀兩個即時溢價追蹤器的 snapshot()（皆 WS 即時，無 REST 輪詢）。
- 去重節流：同一 (交易所, 幣) 冷卻時間內不重發，避免持續極端時洗頻。
- 通知管道：復用 tools/notify.py -s 5011（與 funding_scanner 同一套 TG 通道）。
"""
import asyncio
import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)

NOTIFY_PATH = str(Path(__file__).resolve().parent.parent.parent.parent / "tools" / "notify.py")


class PremiumAlerter:
    def __init__(self, bybit_tracker, binance_tracker,
                 threshold_pct: float = 2.0, cooldown_min: int = 30, interval_s: int = 10):
        self.bybit = bybit_tracker
        self.binance = binance_tracker
        self.threshold = threshold_pct        # |premium%| ≥ 此值觸發
        self.cooldown = cooldown_min * 60      # 同 (ex,symbol) 冷卻秒數
        self.interval = interval_s             # 掃描間隔
        self.is_running = False
        self._task: asyncio.Task | None = None
        self._notified: dict[tuple, float] = {}  # (ex, symbol) -> 上次通知 monotonic

    async def start(self):
        self.is_running = True
        self._task = asyncio.create_task(self._loop())
        logger.info(f"[溢價告警] 啟動：|premium|≥{self.threshold}% 通知，"
                    f"同幣冷卻 {self.cooldown // 60} 分")

    async def stop(self):
        self.is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    async def _loop(self):
        await asyncio.sleep(15)  # 等兩家 WS 暖機有資料
        while self.is_running:
            try:
                self._scan()
            except Exception as e:
                logger.warning(f"[溢價告警] 掃描失敗: {e}")
            await asyncio.sleep(self.interval)

    def _scan(self):
        for ex, tr in (("Bybit", self.bybit), ("Binance", self.binance)):
            if tr is None:
                continue
            for r in tr.snapshot():
                p = r.get("premium_pct")
                if p is None:
                    continue
                # 只看新鮮資料（stale < 60s），避免斷線殘值誤觸
                if abs(p) >= self.threshold and (r.get("stale_s") or 0) < 60:
                    self._maybe_alert(ex, r)

    def _maybe_alert(self, ex: str, r: dict):
        key = (ex, r["symbol"])
        now = time.monotonic()
        if now - self._notified.get(key, 0) < self.cooldown:
            return
        self._notified[key] = now
        p = r["premium_pct"]
        msg = (f"⚠ 極端溢價 {r['symbol']} @{ex} "
               f"premium={p:+.2f}% (mark={r['mark']:.6g} index={r['index']:.6g} "
               f"FR={r.get('funding_rate_pct', 0):+.4f}%)")
        asyncio.create_task(self._notify(msg))

    async def _notify(self, msg: str):
        try:
            proc = await asyncio.create_subprocess_exec("python", NOTIFY_PATH, "-s", "5011", msg)
            await proc.wait()
            logger.info(f"[溢價告警] 已發送: {msg}")
        except Exception as e:
            logger.warning(f"[溢價告警] 通知失敗: {e}")

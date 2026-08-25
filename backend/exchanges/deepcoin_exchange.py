"""DeepCoin USDT SWAP - 純 REST（脫離 ccxt）

DeepCoin WS market topic 實測：訂閱後僅在資料變動時推送，多數 symbol 數十秒無 push，
不適合用作 cold start 來源。改用 4 條並行 REST bulk endpoint，cold start <500ms 完整覆蓋。

REST endpoints（並行）：
- /deepcoin/market/instruments：合約清單
- /deepcoin/market/tickers：bid/ask/last (mark 用 last 替代，DeepCoin REST 無 markPrice 欄位)
- /deepcoin/trade/fund-rate/current-funding-rate：當前費率
- /deepcoin/trade/funding-rate：結算週期 + 下次結算時間

funding interval 校驗：用 /fund-rate/history 取最近 2 筆，cache 4 小時。
幣種詳情頁的 LIVE 由 services/ws_manager.py _ws_deepcoin (per-symbol) 處理。
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

from exchanges.base import BaseExchange
from exchanges._session import make_session
from models import FundingRecord
from services.normalizer import calc_annual_rate

logger = logging.getLogger(__name__)

REST_BASE = "https://api.deepcoin.com"
INTERVAL_CACHE_PATH = Path(__file__).parent.parent / "data" / "deepcoin_intervals.json"
INTERVAL_CACHE_TTL = 3600            # 週期快取 1h 刷新（背景重驗）
INTERVAL_VERIFY_SPACING = 1.1        # DeepCoin 歷史端點限速 1/s，序列驗證每筆間隔（略大於 1s）


def _next_settle_time(interval_h: float) -> datetime:
    """依結算週期推算下一個結算時間（對齊 UTC 00:00 的整數倍）。
    DeepCoin 宣告端點的 nextSettleTime 不可靠（會謊報，如 ESPORTS 真實 4h 卻回 1h 邊界），
    故改由「驗證後的真實週期」推算，與週期顯示一致。DeepCoin 結算對齊 UTC 午夜（歷史實證）。"""
    now = datetime.now(timezone.utc)
    interval_s = max(int(interval_h * 3600), 3600)
    midnight_ts = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    elapsed = int(now.timestamp()) - midnight_ts
    next_offset = ((elapsed // interval_s) + 1) * interval_s
    return datetime.fromtimestamp(midnight_ts + next_offset, timezone.utc)


class DeepCoinExchange(BaseExchange):
    name = "deepcoin"

    _interval_cache: dict[str, float] = {}
    _interval_cache_ts: float = 0
    _verify_task = None  # 背景週期驗證 task（class 層級，避免重複啟動）

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = make_session(30)
        return self._session

    async def _get_json(self, session, path: str, params: dict) -> dict:
        async with session.get(REST_BASE + path, params=params) as r:
            if r.status != 200:
                raise Exception(f"DeepCoin HTTP {r.status}: {path}")
            return await r.json()

    async def fetch_funding_rates(self) -> list[FundingRecord]:
        session = await self._get_session()
        try:
            instruments_d, tickers_d, fr_curr_d, fr_intv_d = await asyncio.gather(
                self._get_json(session, "/deepcoin/market/instruments", {"instType": "SWAP"}),
                self._get_json(session, "/deepcoin/market/tickers", {"instType": "SWAP"}),
                self._get_json(session, "/deepcoin/trade/fund-rate/current-funding-rate", {"instType": "SwapU"}),
                self._get_json(session, "/deepcoin/trade/funding-rate", {"instType": "SwapU"}),
            )
        except Exception as e:
            logger.error(f"[{self.name}] API 請求失敗: {e}")
            raise

        instruments = instruments_d.get("data") or []
        tickers = tickers_d.get("data") or []
        fr_curr = (fr_curr_d.get("data") or {}).get("current_fund_rates") or []
        fr_intv = fr_intv_d.get("data") or []

        instr_map: dict[str, dict] = {
            ins["instId"]: ins for ins in instruments
            if ins.get("state") == "live" and ins.get("quoteCcy") == "USDT" and ins.get("instType") == "SWAP"
        }
        ticker_map = {t["instId"]: t for t in tickers}
        fr_curr_map = {x["instrumentId"]: x.get("fundingRate") for x in fr_curr}
        fr_intv_map = {x["instrumentID"]: x for x in fr_intv}

        inst_ids = [iid.replace("-USDT-SWAP", "") + "USDT" for iid in instr_map]
        verified_intervals = await self._get_intervals(session, inst_ids)

        records: list[FundingRecord] = []
        skipped = 0
        for inst_id, instr in instr_map.items():
            base = instr.get("baseCcy") or inst_id.replace("-USDT-SWAP", "")
            fr_key = inst_id.replace("-USDT-SWAP", "") + "USDT"
            t = ticker_map.get(inst_id)
            rate_raw = fr_curr_map.get(fr_key)
            intv = fr_intv_map.get(fr_key)
            if t is None or rate_raw is None or intv is None:
                skipped += 1
                continue
            try:
                rate = float(rate_raw)
                interval_s = int(intv.get("settleInterval") or 0)
            except (TypeError, ValueError):
                skipped += 1
                continue
            if interval_s <= 0:
                skipped += 1
                continue
            interval_h = verified_intervals.get(fr_key, interval_s / 3600.0)
            # 下次收費由「驗證後的真實週期」推算（對齊 UTC 邊界），不用不可靠的宣告 nextSettleTime，
            # 避免週期(4h) 與下次收費(1h 邊界) 矛盾。
            funding_time = _next_settle_time(interval_h)
            last = t.get("last")
            bid = t.get("bidPx")
            ask = t.get("askPx")
            records.append(FundingRecord(
                exchange=self.name,
                symbol=f"{base}/USDT:USDT",
                funding_rate=rate,
                next_funding_rate=None,
                funding_time=funding_time,
                mark_price=float(last) if last else None,
                index_price=None,
                bid_price=float(bid) if bid else None,
                ask_price=float(ask) if ask else None,
                annual_rate=calc_annual_rate(rate, interval_h),
                funding_interval_h=interval_h,
            ))
        logger.info(
            f"[{self.name}] 取得 {len(records)} 筆費率（活躍 USDT 永續 {len(instr_map)}，略過 {skipped}）"
        )
        return records

    async def _get_intervals(self, session, inst_ids: list[str]) -> dict[str, float]:
        """回傳「當前已知」的週期快取（不阻塞掃描）。過期時在背景以 1/s 節流重新驗證。
        DeepCoin 歷史端點限速 1/s 且無批量，無法即時查全部 → 用背景 last-known-good 策略。"""
        now = time.time()
        # 首次：把檔案快取載入記憶體（跨重啟即有 last-known-good，避免冷啟退回壞宣告值）
        if not DeepCoinExchange._interval_cache and INTERVAL_CACHE_PATH.exists():
            try:
                with open(INTERVAL_CACHE_PATH, "r") as f:
                    file_data = json.load(f)
                DeepCoinExchange._interval_cache = {k: v for k, v in file_data.items() if not k.startswith("_")}
                DeepCoinExchange._interval_cache_ts = file_data.get("_ts", 0)
            except Exception:
                pass
        # 過期就在背景重驗（1/s、約 4 分鐘跑完），當下先回既有快取
        if now - DeepCoinExchange._interval_cache_ts >= INTERVAL_CACHE_TTL:
            if DeepCoinExchange._verify_task is None or DeepCoinExchange._verify_task.done():
                DeepCoinExchange._verify_task = asyncio.create_task(self._verify_all(list(inst_ids)))
        return DeepCoinExchange._interval_cache

    async def _verify_all(self, inst_ids: list[str]) -> None:
        """背景：逐幣打歷史、用最近兩次結算間隔算真實週期，嚴格遵守 1/s 限速（序列 + 間隔）。
        驗證失敗的幣「保留上次正確值」，絕不退回可能過期的宣告值。約 4 分鐘跑完 ~230 幣。"""
        if not inst_ids:
            return
        session = await self._get_session()
        updated = dict(DeepCoinExchange._interval_cache)  # 從既有值開始，失敗不丟
        ok = 0
        try:
            for inst_id in inst_ids:
                try:
                    d = await self._get_json(session, "/deepcoin/trade/fund-rate/history",
                                              {"instId": inst_id, "page": 1, "size": 2})
                    rows = (d.get("data") or {}).get("rows") or []
                    if len(rows) >= 2:
                        gap_s = abs(int(rows[0].get("CreateTime") or 0) - int(rows[1].get("CreateTime") or 0))
                        snapped = round((gap_s / 3600) * 2) / 2
                        if snapped > 0:
                            updated[inst_id] = snapped
                            ok += 1
                except Exception:
                    pass  # 保留 updated 內既有值
                await asyncio.sleep(INTERVAL_VERIFY_SPACING)
        finally:
            DeepCoinExchange._interval_cache = updated
            DeepCoinExchange._interval_cache_ts = time.time()
            try:
                INTERVAL_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
                with open(INTERVAL_CACHE_PATH, "w") as f:
                    json.dump({"_ts": DeepCoinExchange._interval_cache_ts, **updated}, f)
            except Exception:
                pass
            logger.info(f"[deepcoin] 週期背景驗證完成：本輪成功 {ok}/{len(inst_ids)}，快取共 {len(updated)} 個")

    async def fetch_funding_history(self, symbol: str, since: int = 0, limit: int = 100) -> list[dict]:
        base = symbol.split("/")[0].upper()
        inst_id = f"{base}USDT"
        session = await self._get_session()
        try:
            # ⚠ 參數名是 limit 不是 size。官方文檔寫 size，但伺服器【完全不認】——
            # 實測 size=5 / size=100 一律回預設的 20 筆，改成 limit 才生效：
            #   limit=5 → 5 筆、limit=37 → 37 筆、limit=100 → 100 筆（涵蓋約 33 天）。
            # 原本用 size 等於永遠只拿 20 筆，1h 結算的幣連 24h 回補都填不滿。
            # （limit 上限 100，實測 200/500 會回 0 筆，故用 min(limit, 100)。）
            d = await self._get_json(session, "/deepcoin/trade/fund-rate/history",
                                      {"instId": inst_id, "page": 1, "limit": min(limit, 100)})
            items = ((d.get("data") or {}).get("rows")) or []
            entries = []
            for it in items:
                rate = it.get("rate")
                ts = it.get("CreateTime")
                if rate is not None and ts:
                    entries.append({"timestamp": int(ts) * 1000, "rate": float(rate)})
            if since:
                entries = [e for e in entries if e["timestamp"] >= since]
            entries.sort(key=lambda e: e["timestamp"])
            return entries[-limit:]
        except Exception as e:
            logger.debug(f"[{self.name}] 歷史費率查詢失敗 {symbol}: {e}")
            return []

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

"""Kraken Futures 永續 - 純 REST（脫離 ccxt）

- /derivatives/api/v3/tickers 一次拿全市場（fundingRate 絕對值、markPrice、indexPrice、bid、ask）
- Kraken 資費是「絕對值」（價格單位），相對率 = fundingRate / indexPrice
  （已用 /v4/historicalfundingrates 的 relativeFundingRate 對齊驗證）
- 結算週期：每 1 小時（歷史端點時間戳實測為整點 1h 間隔）
- 永續 symbol：PF_{BASE}USD，USD 本位（多抵押）；Kraken 用 XBT 代表 BTC
"""

import logging
from datetime import datetime, timezone, timedelta

import aiohttp

from exchanges.base import BaseExchange
from exchanges._session import make_session
from models import FundingRecord
from services.normalizer import calc_annual_rate

logger = logging.getLogger(__name__)

TICKERS_URL = "https://futures.kraken.com/derivatives/api/v3/tickers"
FUNDING_INTERVAL_H = 1  # Kraken Futures 每小時結算
_BASE_ALIAS = {"XBT": "BTC"}  # Kraken 用 XBT 表示 BTC


def _f(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


class KrakenExchange(BaseExchange):
    name = "kraken"

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = make_session(30)
        return self._session

    async def fetch_funding_rates(self) -> list[FundingRecord]:
        session = await self._get_session()
        try:
            async with session.get(TICKERS_URL) as r:
                data = await r.json()
        except Exception as e:
            logger.error(f"[{self.name}] tickers 取得失敗: {e}")
            raise

        tickers = data.get("tickers") or []
        now = datetime.now(timezone.utc)
        next_settle = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

        records: list[FundingRecord] = []
        seen: set[str] = set()
        for t in tickers:
            if t.get("tag") != "perpetual" or t.get("suspended"):
                continue
            # 只取 PF_（USD 多抵押永續），排除 PI_（inverse）避免 BTC/ETH 同 base 重複
            if not str(t.get("symbol", "")).startswith("PF_"):
                continue
            pair = t.get("pair") or ""  # 形如 "ALT:USD"
            base = pair.split(":")[0].upper() if ":" in pair else ""
            if not base:
                continue
            base = _BASE_ALIAS.get(base, base)
            if base in seen:
                continue
            seen.add(base)

            abs_fr = _f(t.get("fundingRate"))
            # 相對率的分母：Kraken 資費以 index 計算
            price = _f(t.get("indexPrice")) or _f(t.get("markPrice")) or _f(t.get("last"))
            if abs_fr is None or not price:
                continue
            rate = abs_fr / price  # 相對每小時費率
            pred_abs = _f(t.get("fundingRatePrediction"))
            next_rate = (pred_abs / price) if pred_abs is not None else None

            records.append(FundingRecord(
                exchange=self.name,
                symbol=f"{base}/USD:USD",
                funding_rate=rate,
                next_funding_rate=next_rate,
                funding_time=next_settle,
                mark_price=_f(t.get("markPrice")),
                index_price=_f(t.get("indexPrice")),
                bid_price=_f(t.get("bid")),
                ask_price=_f(t.get("ask")),
                annual_rate=calc_annual_rate(rate, FUNDING_INTERVAL_H),
                funding_interval_h=FUNDING_INTERVAL_H,
            ))
        logger.info(f"[{self.name}] 取得 {len(records)} 筆費率（{len(tickers)} tickers）")
        return records

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

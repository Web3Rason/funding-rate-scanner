"""Deribit 永續 - 純 REST（脫離 ccxt）

- /public/get_book_summary_by_currency?currency=X&kind=future 批量（含 funding_8h、mark/bid/ask）
- 永續：BTC-PERPETUAL / ETH-PERPETUAL（inverse, USD 本位）＋ {BASE}_USDC-PERPETUAL（USDC 線性）
- funding_8h 即「8h 基準費率」（Deribit 連續結算、以 8h 口徑報價）→ interval = 8h
- 同 base 若同時有 inverse 與 USDC 版，去重保留先出現者（BTC/ETH 保留主流 inverse）
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta

import aiohttp

from exchanges.base import BaseExchange
from exchanges._session import make_session
from models import FundingRecord
from services.normalizer import calc_annual_rate

logger = logging.getLogger(__name__)

BOOK_URL = "https://www.deribit.com/api/v2/public/get_book_summary_by_currency"
CURRENCIES = ["BTC", "ETH", "USDC"]  # USDT 目前無永續
FUNDING_INTERVAL_H = 8


def _f(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _base_of(inst: str) -> str:
    """BTC-PERPETUAL -> BTC ; SOL_USDC-PERPETUAL -> SOL"""
    name = inst.replace("-PERPETUAL", "")
    if "_" in name:
        name = name.split("_")[0]
    return name.upper()


class DeribitExchange(BaseExchange):
    name = "deribit"

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = make_session(30)
        return self._session

    async def fetch_funding_rates(self) -> list[FundingRecord]:
        session = await self._get_session()
        now = datetime.now(timezone.utc)
        # 下一個 8h 邊界（0/8/16 UTC），與 interval 一致
        floor_h = now.hour - (now.hour % 8)
        next_settle = now.replace(hour=floor_h, minute=0, second=0, microsecond=0) + timedelta(hours=8)

        async def _one(cur: str) -> list:
            try:
                async with session.get(f"{BOOK_URL}?currency={cur}&kind=future") as r:
                    d = await r.json()
                return d.get("result") or []
            except Exception as e:
                logger.warning(f"[deribit] {cur} 取得失敗: {e}")
                return []

        results = await asyncio.gather(*[_one(c) for c in CURRENCIES])

        records: list[FundingRecord] = []
        seen: set[str] = set()
        for rows in results:
            for x in rows:
                inst = x.get("instrument_name", "")
                if not inst.endswith("PERPETUAL"):
                    continue
                f8 = _f(x.get("funding_8h"))
                if f8 is None:
                    continue
                base = _base_of(inst)
                if base in seen:  # 同 base 去重（inverse 優先，因排在 USDC 前）
                    continue
                seen.add(base)
                quote = x.get("quote_currency") or "USD"
                records.append(FundingRecord(
                    exchange=self.name,
                    symbol=f"{base}/{quote}:{quote}",
                    funding_rate=f8,
                    next_funding_rate=None,
                    funding_time=next_settle,
                    mark_price=_f(x.get("mark_price")),
                    index_price=_f(x.get("estimated_delivery_price")),
                    bid_price=_f(x.get("bid_price")),
                    ask_price=_f(x.get("ask_price")),
                    annual_rate=calc_annual_rate(f8, FUNDING_INTERVAL_H),
                    funding_interval_h=FUNDING_INTERVAL_H,
                ))
        logger.info(f"[{self.name}] 取得 {len(records)} 筆費率")
        return records

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

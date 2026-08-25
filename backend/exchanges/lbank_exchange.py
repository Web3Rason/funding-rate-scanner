"""LBank USDT 永續 - 純 REST

文檔：https://www.lbank.com/docs/contract.html（公開行情免簽名）
REST base：https://lbkperp.lbank.com

endpoints（並行，皆需 productGroup=SwapU）：
- /cfd/openApi/v1/pub/instrument：合約清單（baseCurrency、symbol）
- /cfd/openApi/v1/pub/marketData：一次回全市場行情
  fundingRate（小數）、positionFeeTime（結算週期，秒）、nextFeeTime（下次結算，ms）、
  markedPrice（標記價）、underlyingPrice（指數價）、lastPrice、instrumentStatus（2=交易中）

實測（2026-06-11）：instrument 678 檔、marketData 1386 檔（含非交易中），
無 bid/ask 批次來源（訂單簿需逐合約查），bid/ask 設 None。
"""

import asyncio
import logging
from datetime import datetime, timezone

import aiohttp

from exchanges.base import BaseExchange
from exchanges._session import make_session
from models import FundingRecord
from services.normalizer import calc_annual_rate

logger = logging.getLogger(__name__)

REST_BASE = "https://lbkperp.lbank.com"
PRODUCT_GROUP = "SwapU"


class LbankExchange(BaseExchange):
    name = "lbank"

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = make_session(30)
        return self._session

    async def _get_json(self, session, path: str) -> dict:
        async with session.get(
            REST_BASE + path, params={"productGroup": PRODUCT_GROUP}
        ) as r:
            if r.status != 200:
                raise Exception(f"LBank HTTP {r.status}: {path}")
            d = await r.json()
        if not d.get("success", False):
            raise Exception(f"LBank error_code={d.get('error_code')}: {path}")
        return d

    async def fetch_funding_rates(self) -> list[FundingRecord]:
        session = await self._get_session()
        try:
            instr_d, market_d = await asyncio.gather(
                self._get_json(session, "/cfd/openApi/v1/pub/instrument"),
                self._get_json(session, "/cfd/openApi/v1/pub/marketData"),
            )
        except Exception as e:
            logger.error(f"[{self.name}] API 請求失敗: {e}")
            raise

        instr_map = {
            ins["symbol"]: ins for ins in instr_d.get("data") or []
            if ins.get("priceCurrency") == "USDT" and ins.get("symbol")
        }

        records: list[FundingRecord] = []
        skipped = 0
        for m in market_d.get("data") or []:
            sym = m.get("symbol")
            instr = instr_map.get(sym)
            # instrumentStatus 2 = 交易中
            if instr is None or str(m.get("instrumentStatus")) != "2":
                continue
            try:
                rate = float(m.get("fundingRate"))
                interval_s = int(m.get("positionFeeTime") or 0)
            except (TypeError, ValueError):
                skipped += 1
                continue
            if interval_s <= 0:
                skipped += 1
                continue
            interval_h = interval_s / 3600.0
            next_ms = m.get("nextFeeTime")
            funding_time = (
                datetime.fromtimestamp(int(next_ms) / 1000, tz=timezone.utc)
                if next_ms else None
            )
            base = instr.get("baseCurrency") or sym.removesuffix("USDT")
            mark = m.get("markedPrice")
            index = m.get("underlyingPrice")
            records.append(FundingRecord(
                exchange=self.name,
                symbol=f"{base}/USDT:USDT",
                funding_rate=rate,
                next_funding_rate=None,
                funding_time=funding_time,
                mark_price=float(mark) if mark else None,
                index_price=float(index) if index else None,
                bid_price=None,
                ask_price=None,
                annual_rate=calc_annual_rate(rate, interval_h),
                funding_interval_h=interval_h,
            ))
        logger.info(
            f"[{self.name}] 取得 {len(records)} 筆費率（清單 {len(instr_map)}，略過 {skipped}）"
        )
        return records

    async def fetch_bbo(self, base: str) -> tuple[float | None, float | None]:
        """單一合約最佳買賣價（訂單簿 marketOrder，一次只能查一個合約）。
        base 例如 'BTC' → symbol 'BTCUSDT'。回傳 (bid, ask)。
        批次行情無 bid/ask，僅在按需（詳細面板/即時查詢）時呼叫。
        """
        session = await self._get_session()
        sym = f"{base.upper()}USDT"
        try:
            async with session.get(
                REST_BASE + "/cfd/openApi/v1/pub/marketOrder",
                params={"symbol": sym, "depth": "5"},
            ) as r:
                d = await r.json()
            data = d.get("data") or {}
            asks = data.get("asks") or []
            bids = data.get("bids") or []
            ask = float(asks[0]["price"]) if asks and asks[0].get("price") else None
            bid = float(bids[0]["price"]) if bids and bids[0].get("price") else None
            return bid, ask
        except Exception as e:
            logger.debug(f"[{self.name}] BBO 查詢失敗 {sym}: {e}")
            return None, None

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

"""Trade.xyz (HIP-3 perpDEX on Hyperliquid) - REST API（每小時結算）

bid/ask 跟 HL 主 universe 共用 _HyperliquidL2Cache 單例（同一條 WS 連線），
不再對每個 coin 打 l2Book REST，避免在共用 IP weight pool 上撞 429。
"""

import logging
import aiohttp
from datetime import datetime, timezone, timedelta
from exchanges.base import BaseExchange
from exchanges._session import make_session
from exchanges.hyperliquid_exchange import _get_l2_cache
from models import FundingRecord
from services.normalizer import calc_annual_rate

logger = logging.getLogger(__name__)

API_URL = "https://api.hyperliquid.xyz/info"
DEX_NAME = "xyz"
FUNDING_INTERVAL_H = 1  # 每小時結算（同 Hyperliquid）


class TradeXyzExchange(BaseExchange):
    """Trade.xyz - Hyperliquid HIP-3 perpDEX"""

    name = "tradexyz"

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = make_session(30)
        return self._session

    async def _post(self, payload: dict) -> dict | list:
        session = await self._get_session()
        async with session.post(API_URL, json=payload) as resp:
            if resp.status != 200:
                raise Exception(f"Trade.xyz HTTP {resp.status}")
            return await resp.json()

    def _coin_to_symbol(self, coin: str) -> str:
        """將 xyz:TSLA 轉換為 TSLA/USDT:USDT"""
        # 去掉 xyz: 前綴
        base = coin.split(":", 1)[1] if ":" in coin else coin
        return f"{base}/USDT:USDT"

    def _symbol_to_coin(self, symbol: str) -> str:
        """將 TSLA/USDT:USDT 轉換為 xyz:TSLA"""
        base = symbol.split("/")[0].upper()
        return f"{DEX_NAME}:{base}"

    async def fetch_funding_rates(self) -> list[FundingRecord]:
        records = []

        # 1. 取得所有合約 metadata + 當前 funding/price（帶 dex 參數）
        data = await self._post({"type": "metaAndAssetCtxs", "dex": DEX_NAME})
        universe = data[0]["universe"]
        ctxs = data[1]

        # 過濾下架合約（訂閱已下架的 coin 會被 HL WS 直接 close 連線）
        active_pairs = [(u, ctx) for u, ctx in zip(universe, ctxs) if not u.get("isDelisted", False)]
        universe = [p[0] for p in active_pairs]
        ctxs = [p[1] for p in active_pairs]

        # 2. 確保 WS 訂閱涵蓋所有 tradexyz coin（HIP-3 universe 的 name 已含 xyz: 前綴，不要再加）
        coins_full = [u["name"] for u in universe]
        cache = _get_l2_cache()
        await cache.ensure_subscribed(coins_full, owner="tradexyz")
        await cache.wait_warmup(timeout=5.0)

        # 3. 組裝 records，bid/ask 直接讀 WS cache
        now = datetime.now(timezone.utc)
        next_hour = now.replace(minute=0, second=0, microsecond=0)
        if next_hour <= now:
            next_hour += timedelta(hours=1)

        bid_ask_hit = 0
        for u, ctx in zip(universe, ctxs):
            coin = u["name"]
            funding_str = ctx.get("funding")
            if funding_str is None:
                continue

            rate_1h = float(funding_str)
            mark_px = float(ctx.get("markPx", 0)) or None
            oracle_px = float(ctx.get("oraclePx", 0)) or None
            symbol = self._coin_to_symbol(coin)

            # coin 已含 xyz: 前綴，cache key 就是 coin 本身
            book = cache.get(coin) or {}
            bid = book.get("bid")
            ask = book.get("ask")
            if bid is not None or ask is not None:
                bid_ask_hit += 1

            records.append(FundingRecord(
                exchange=self.name,
                symbol=symbol,
                funding_rate=rate_1h,
                next_funding_rate=None,
                funding_time=next_hour,
                mark_price=mark_px,
                index_price=oracle_px,
                bid_price=bid,
                ask_price=ask,
                annual_rate=calc_annual_rate(rate_1h, FUNDING_INTERVAL_H),
                funding_interval_h=FUNDING_INTERVAL_H,
            ))

        logger.info(
            f"[{self.name}] 取得 {len(records)} 筆費率（{len(universe)} 合約），"
            f"bid/ask: {bid_ask_hit} 個（WS 共用 HL 連線）"
        )
        return records

    async def fetch_funding_history(self, symbol: str, since: int = 0, limit: int = 100) -> list[dict]:
        """取得歷史費率"""
        coin = self._symbol_to_coin(symbol)

        try:
            payload = {"type": "fundingHistory", "coin": coin, "startTime": since or 0}
            data = await self._post(payload)

            entries = []
            for item in data:
                rate_1h = float(item.get("fundingRate", 0))
                entries.append({
                    "timestamp": item.get("time"),
                    "rate": rate_1h,
                })

            entries.sort(key=lambda e: e["timestamp"])
            return entries[-limit:]
        except Exception as e:
            logger.debug(f"[{self.name}] 歷史費率查詢失敗 {symbol}: {e}")
            return []

    async def close(self):
        # WS singleton 不在 close 時關掉（跟 HL 共用）
        if self._session and not self._session.closed:
            await self._session.close()

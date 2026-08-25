"""BingX USDT-Perpetual - REST-based (脫離 ccxt)

BingX WS 每連線最多 ~150 streams（實測 200 即掉訊息），650 symbols × 2 streams 需要 9 條連線
工程複雜度高，且 hourly scan 模式下 REST bulk 與 WS 對 bid/ask 新鮮度的差別 < 1 秒。
故 scanner 走純 REST：每次 fetch_funding_rates 拿 2 個 bulk endpoint 一次填滿。

兩個 bulk endpoint:
- /openApi/swap/v2/quote/premiumIndex → funding/mark/index/nextFundingTime/fundingIntervalHours
- /openApi/swap/v2/quote/ticker       → bid/ask/last

幣種詳情頁的 LIVE 推送由 services/ws_manager.py _ws_bingx (per-symbol 訂閱) 獨立處理。
"""

import asyncio
import logging
import time
from datetime import datetime, timezone

import aiohttp

from exchanges.base import BaseExchange
from exchanges._session import make_session
from models import FundingRecord
from services.normalizer import calc_annual_rate

logger = logging.getLogger(__name__)

REST_BASE = "https://open-api.bingx.com"
DEFAULT_FUNDING_INTERVAL_H = 8
METADATA_TTL = 86400


def _f(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _ws_to_unified(s: str) -> str | None:
    parts = s.split("-")
    if len(parts) != 2:
        return None
    return f"{parts[0]}/{parts[1]}:{parts[1]}"


def _unified_to_ws(symbol: str) -> str:
    base, rest = symbol.split("/", 1)
    quote = rest.split(":", 1)[0]
    return f"{base}-{quote}"


class BingxExchange(BaseExchange):
    name = "bingx"

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None
        self._perp_symbols: set[str] = set()
        self._metadata_fetched_at: float = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = make_session(30)
        return self._session

    async def _ensure_metadata(self) -> None:
        if self._perp_symbols and time.time() - self._metadata_fetched_at < METADATA_TTL:
            return
        try:
            session = await self._get_session()
            async with session.get(f"{REST_BASE}/openApi/swap/v2/quote/contracts") as r:
                d = await r.json()
            self._perp_symbols = {
                x["symbol"] for x in d.get("data") or []
                if x.get("status") == 1 and x.get("currency") == "USDT" and x.get("symbol")
            }
            self._metadata_fetched_at = time.time()
            logger.info(f"[bingx] metadata 更新：{len(self._perp_symbols)} USDT 合約")
        except Exception as e:
            logger.warning(f"[bingx] contracts 取得失敗: {e}")

    async def _fetch_bulk(self, path: str, label: str) -> list:
        """拉一個 bulk endpoint，檢查 BingX 業務碼（code!=0 代表限速/暫時停用/錯誤），
        非成功就重試 3 次；不要把錯誤回應當成空資料默默吞掉（否則 BingX 整所會無聲消失）。"""
        session = await self._get_session()
        for attempt in range(3):
            try:
                async with session.get(f"{REST_BASE}{path}") as r:
                    d = await r.json()
                if d.get("code") == 0:
                    return d.get("data") or []
                logger.warning(f"[bingx] {label} code={d.get('code')} msg={str(d.get('msg'))[:60]}（第{attempt+1}/3次）")
            except Exception as e:
                logger.warning(f"[bingx] {label} 失敗（第{attempt+1}/3次）: {e}")
            if attempt < 2:
                await asyncio.sleep(0.6 * (attempt + 1))
        return []

    async def _fetch_two_bulks(self) -> tuple[dict, dict]:
        """並行拉兩個 bulk endpoint（各自帶重試 + code 檢查）。"""
        prem_list, tick_list = await asyncio.gather(
            self._fetch_bulk("/openApi/swap/v2/quote/premiumIndex", "premiumIndex"),
            self._fetch_bulk("/openApi/swap/v2/quote/ticker", "ticker"),
        )
        premium_map = {x["symbol"]: x for x in prem_list if x.get("symbol")}
        ticker_map = {x["symbol"]: x for x in tick_list if x.get("symbol")}
        return premium_map, ticker_map

    async def fetch_funding_rates(self) -> list[FundingRecord]:
        await self._ensure_metadata()
        premium_map, ticker_map = await self._fetch_two_bulks()

        records: list[FundingRecord] = []
        bid_ask_hit = 0
        for sym in sorted(self._perp_symbols):
            prem = premium_map.get(sym)
            if not prem:
                continue
            fr = _f(prem.get("lastFundingRate"))
            if fr is None:
                continue
            unified = _ws_to_unified(sym)
            if not unified:
                continue
            mark = _f(prem.get("markPrice"))
            index = _f(prem.get("indexPrice"))
            tk = ticker_map.get(sym) or {}
            bid = _f(tk.get("bidPrice"))
            ask = _f(tk.get("askPrice"))
            if bid is not None or ask is not None:
                bid_ask_hit += 1
            try:
                interval_h = float(prem.get("fundingIntervalHours") or DEFAULT_FUNDING_INTERVAL_H)
            except (TypeError, ValueError):
                interval_h = DEFAULT_FUNDING_INTERVAL_H
            ft = None
            nft = prem.get("nextFundingTime")
            if nft:
                try:
                    ft = datetime.fromtimestamp(int(nft) / 1000, timezone.utc)
                except (TypeError, ValueError):
                    pass
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
            f"[{self.name}] 取得 {len(records)} 筆費率（{len(self._perp_symbols)} USDT 合約），"
            f"bid/ask: {bid_ask_hit} 個（REST bulk premium {len(premium_map)} + ticker {len(ticker_map)}）"
        )
        return records

    async def fetch_funding_history(self, symbol: str, since: int = 0, limit: int = 100) -> list[dict]:
        sym = _unified_to_ws(symbol)
        params = {"symbol": sym, "limit": str(min(limit, 1000))}
        if since:
            params["startTime"] = str(int(since))
        try:
            session = await self._get_session()
            async with session.get(f"{REST_BASE}/openApi/swap/v2/quote/fundingRate", params=params) as r:
                d = await r.json()
            entries = [
                {"timestamp": int(it["fundingTime"]), "rate": float(it["fundingRate"])}
                for it in (d.get("data") or [])
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

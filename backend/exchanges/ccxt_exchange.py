import asyncio
import ccxt.async_support as ccxt
import aiohttp
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from exchanges.base import BaseExchange
from exchanges._session import make_session
from models import FundingRecord
from services.normalizer import normalize_rate_to_8h, calc_annual_rate

logger = logging.getLogger(__name__)

# 需要用直接 API 的交易所
DIRECT_API_EXCHANGES = {
    "mexc": "https://contract.mexc.com/api/v1/contract/funding_rate",
    "kucoinfutures": "https://api-futures.kucoin.com/api/v1/contracts/active",
}


def scope_bybit_markets(ex, exchange_id: str):
    """剔除 bybit 用不到的 option 市場再 load_markets。

    bybit 預設 fetchMarkets 會連 option（instruments-info?category=option）一起抓，
    那個端點常逾時，一旦失敗整個 load_markets 就拋錯，連帶歷史費率／掃描全部失敗
    （症狀：歷史矩陣顯示掃描時的「預測費率」而非實際結算費率）。
    只保留實際用到的 spot / linear / inverse。
    """
    if exchange_id == "bybit":
        # 只改 types 子鍵，保留 ccxt 在 fetchMarkets 下的其他預設（不整塊覆寫）
        ex.options.setdefault("fetchMarkets", {})["types"] = ["spot", "linear", "inverse"]
    return ex


def _extract_interval_h(info: dict) -> float:
    """從交易所市場 info 提取 funding interval（小時）"""
    # Bybit: fundingInterval (分鐘)
    val = info.get("fundingInterval")
    if val is not None:
        try:
            v = float(val)
            # Bybit 用分鐘（60, 120, 240, 480）
            if v >= 60:
                return v / 60
            return v
        except (ValueError, TypeError):
            pass
    # Bitget: fundInterval (小時，值為 4 或 8）
    val = info.get("fundInterval")
    if val is not None:
        try:
            return float(val)
        except (ValueError, TypeError):
            pass
    # Gate.io: funding_interval (秒，如 3600, 14400, 28800)
    val = info.get("funding_interval")
    if val is not None:
        try:
            v = float(val)
            if v >= 3600:
                return v / 3600
            return v
        except (ValueError, TypeError):
            pass
    return 8


def _parse_interval_str(interval) -> float:
    """解析 ccxt 回傳的 interval，可能是 '8h'、'30m'、純數字（OKX 對非整點結算合約回傳 ms 字串如 '7200000'）。"""
    if not interval:
        return 8

    # 字串：先處理 h/m 單位後綴，剩下純數字 fall through 至數字判斷
    if isinstance(interval, str):
        s = interval.strip().lower()
        if s.endswith("h"):
            try:
                return float(s[:-1])
            except ValueError:
                return 8
        if s.endswith("m"):
            try:
                return float(s[:-1]) / 60
            except ValueError:
                return 8
        try:
            interval = float(s)
        except ValueError:
            return 8

    # 數字：依量級推單位（ms / 分鐘 / 小時）
    try:
        v = float(interval)
        if v >= 3600000:  # 毫秒 (1h = 3,600,000)
            return v / 3600000
        if v >= 60:       # 分鐘 (1h = 60)
            return v / 60
        return v
    except (ValueError, TypeError):
        return 8


def _guess_interval_from_next_funding(next_funding_ts) -> float:
    """從 nextFundingTimestamp 的小時/分鐘推算 funding interval（跨交易所啟發式）

    原理：8h 結算固定在 0/8/16 點，4h 在 0/4/8/12/16/20 點。
    若 nextFundingTime 不在這些點上，代表 interval 較短。
    """
    if not next_funding_ts:
        return 8
    try:
        dt = datetime.fromtimestamp(int(next_funding_ts) / 1000, tz=timezone.utc)
        
        # 如果不是整點結算，保守回傳 1h (某些交易所如 Hyperliquid)
        if dt.minute != 0 or dt.second != 0:
            return 1.0
            
        hour = dt.hour
        if hour % 2 != 0:
            return 1.0
        if hour % 4 != 0:
            return 2.0
        if hour % 8 != 0:
            return 4.0
        return 8.0
    except (ValueError, TypeError, OSError):
        return 8.0


def _calc_next_funding_time(interval_h: float) -> datetime:
    """從 interval 計算下次結算時間（適用於固定排程的交易所如 Bitget）"""
    now = datetime.now(timezone.utc)
    interval_secs = int(interval_h * 3600)
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed = int((now - midnight).total_seconds())
    next_secs = ((elapsed // interval_secs) + 1) * interval_secs
    return midnight + timedelta(seconds=next_secs)


class CcxtExchange(BaseExchange):
    """用 ccxt 統一處理 CEX 交易所"""

    _MARKET_RELOAD_INTERVAL = 3600  # 每小時強制重新載入 market info（更新 funding_interval）

    def __init__(self, exchange_id: str):
        self.exchange_id = exchange_id
        self.name = exchange_id
        self._exchange = getattr(ccxt, exchange_id)({
            "enableRateLimit": True,
        })
        scope_bybit_markets(self._exchange, exchange_id)
        self._session: aiohttp.ClientSession | None = None
        self._markets_loaded_at: float = 0  # 上次強制重載 market info 的時間

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = make_session(15)
        return self._session

    async def fetch_funding_rates(self) -> list[FundingRecord]:
        # 檢查是否支援批次查詢
        if self._exchange.has.get("fetchFundingRates"):
            return await self._fetch_via_ccxt()

        # 備援：用直接 API
        if self.exchange_id == "mexc":
            return await self._fetch_mexc_direct()
        elif self.exchange_id == "kucoinfutures":
            return await self._fetch_kucoin_direct()

        raise Exception(f"{self.name} 不支援 fetchFundingRates")

    async def _fetch_via_ccxt(self) -> list[FundingRecord]:
        """用 ccxt fetchFundingRates 批次查詢"""
        import time as _time
        records = []
        try:
            # 載入市場資訊，用於過濾已下架合約 + 取得 funding interval
            # 每小時強制重載（避免 funding_interval 等欄位長期使用過時快取）
            now = _time.time()
            force_reload = (now - self._markets_loaded_at) >= self._MARKET_RELOAD_INTERVAL
            await self._exchange.load_markets(force_reload)
            if force_reload:
                self._markets_loaded_at = now
                logger.info(f"[{self.name}] 強制重新載入 market info")
            active_symbols = set()
            interval_map = {}  # symbol -> hours
            delisting_map = {}  # symbol -> bool
            for s, m in self._exchange.markets.items():
                if m.get("active", True):
                    active_symbols.add(s)
                elif self.exchange_id == "bybit":
                    # Bybit pre-listing 合約 ccxt 會標 active=False，但 ContinuousTrading 階段已可交易
                    _info = m.get("info", {})
                    if (_info.get("isPreListing") is True or _info.get("isPreListing") == "true") \
                       and (_info.get("preListingInfo") or {}).get("curAuctionPhase") == "ContinuousTrading":
                        active_symbols.add(s)
                info = m.get("info", {})
                interval_map[s] = _extract_interval_h(info)
                # 偵測即將下架的合約
                is_dl = False
                if self.exchange_id == "gateio":
                    is_dl = bool(info.get("in_delisting"))
                elif self.exchange_id == "binance":
                    is_dl = info.get("status", "TRADING") not in ("TRADING", "")
                elif self.exchange_id == "bybit":
                    is_dl = info.get("status", "Trading") not in ("Trading", "PreLaunch", "")
                    # deliveryTime 非零 = 合約已排定強制終止（ST 下架）
                    if not is_dl:
                        delivery_ts = info.get("deliveryTime")
                        if delivery_ts:
                            try:
                                delivery_ms = int(delivery_ts)
                                if delivery_ms > 0:
                                    now_ms = _time.time() * 1000
                                    days_until = (delivery_ms - now_ms) / 86400000
                                    if days_until <= 7:
                                        is_dl = True
                            except (ValueError, TypeError):
                                pass
                elif self.exchange_id == "bitget":
                    is_dl = info.get("symbolStatus", "normal") not in ("normal", "")
                elif self.exchange_id == "okx":
                    is_dl = info.get("state", "live") not in ("live", "")
                elif self.exchange_id == "mexc":
                    # automaticDelivery=1 = 自動結算交付（即將下架但仍可交易）
                    is_dl = str(info.get("automaticDelivery", "0")) == "1"
                elif self.exchange_id == "bingx":
                    # status=25 = pre-offline（offTime 已設，apiStateOpen=true 仍可開單）
                    is_dl = str(info.get("status", "1")) == "25"
                if is_dl:
                    delisting_map[s] = True

            # Binance / BingX / Bybit: 用專屬方法取得精確的 funding interval
            extra_interval_map = {}
            if self.exchange_id == "binance":
                extra_interval_map = await self._fetch_binance_funding_info()
            elif self.exchange_id == "bingx":
                extra_interval_map = await self._fetch_bingx_funding_intervals()
            elif self.exchange_id == "bybit":
                extra_interval_map = await self._fetch_bybit_funding_intervals()

            funding_rates = await self._exchange.fetch_funding_rates()

            # 並行取得補充資料
            mark_price_map = {}
            bid_ask_map = {}
            tasks = [self._fetch_bid_ask()]
            if self.exchange_id in ("okx",):
                tasks.append(self._fetch_mark_prices())
            results = await asyncio.gather(*tasks, return_exceptions=True)
            if not isinstance(results[0], Exception):
                bid_ask_map = results[0]
            if len(results) > 1 and not isinstance(results[1], Exception):
                mark_price_map = results[1]

            skipped = 0

            for symbol, data in funding_rates.items():
                if not symbol.endswith(":USDT"):
                    continue

                # 過濾已下架/結算中合約（active=False 一律不顯示）
                # 「即將下架但仍可交易」的合約由 delisting_overrides.json 手動標記
                if symbol not in active_symbols:
                    skipped += 1
                    continue

                rate = data.get("fundingRate")
                if rate is None:
                    continue

                rate_8h = normalize_rate_to_8h(
                    rate,
                    self.exchange_id,
                    data.get("fundingTimestamp"),
                )

                # 決定 funding interval（依優先順序）
                # 1. 優先使用交易所專屬的精確 API (Binance, Bybit, BingX)
                if symbol in extra_interval_map:
                    interval_h = extra_interval_map[symbol]
                # 2. 備援：使用市場資訊預設值或 ccxt 統一欄位
                else:
                    interval_h = interval_map.get(symbol, 8)
                    ccxt_interval = data.get("interval")
                    if ccxt_interval:
                        interval_h = _parse_interval_str(ccxt_interval)
                
                # 3. BingX 啟發式推算 (Fallback)
                if self.exchange_id == "bingx" and interval_h == 8:
                    nft = (data.get("nextFundingTimestamp")
                           or data.get("fundingTimestamp"))
                    interval_h = _guess_interval_from_next_funding(nft)

                # --- Cross-Validation ---
                # 用 nextFundingTimestamp 啟發式校驗，偵測快取/API 過時
                # 啟發式只能推出「至多」的 interval，所以只在推算值更短時覆蓋
                nft_for_check = data.get("nextFundingTimestamp") or data.get("fundingTimestamp")
                if nft_for_check and interval_h > 1:
                    guessed_h = _guess_interval_from_next_funding(nft_for_check)
                    if guessed_h < interval_h:
                        # BingX: 啟發式不夠精確（偶數小時分不清 1h/2h），
                        # 做即時 API 查詢取得精確值
                        if self.exchange_id == "bingx":
                            exact_h = await self._verify_bingx_interval(symbol)
                            if exact_h and exact_h < interval_h:
                                logger.info(
                                    f"[{self.name}] {symbol} interval 精確修正: "
                                    f"{interval_h}h → {exact_h}h（API 驗證）"
                                )
                                interval_h = exact_h
                                extra_interval_map[symbol] = exact_h
                            elif not exact_h:
                                interval_h = guessed_h
                        else:
                            logger.info(
                                f"[{self.name}] {symbol} interval 校驗修正: "
                                f"{interval_h}h → {guessed_h}h（時間戳推算）"
                            )
                            interval_h = guessed_h

                # 下次收費時間：優先 nextFundingTimestamp，備援 fundingTimestamp，最後用計算
                next_ts = data.get("nextFundingTimestamp") or data.get("fundingTimestamp")
                funding_time = None
                if next_ts:
                    funding_time = datetime.fromtimestamp(
                        next_ts / 1000, tz=timezone.utc
                    )
                elif interval_h > 0:
                    funding_time = _calc_next_funding_time(interval_h)

                # 價格：優先 ccxt 回傳，備援 mark_price_map
                mark_price = data.get("markPrice")
                index_price = data.get("indexPrice")
                if mark_price is None and symbol in mark_price_map:
                    mp = mark_price_map[symbol]
                    mark_price = mp.get("mark")
                    index_price = index_price or mp.get("index")

                # bid/ask
                ba = bid_ask_map.get(symbol, {})

                records.append(FundingRecord(
                    exchange=self.name,
                    symbol=symbol,
                    funding_rate=rate_8h,
                    next_funding_rate=data.get("nextFundingRate"),
                    funding_time=funding_time,
                    mark_price=mark_price,
                    index_price=index_price,
                    bid_price=ba.get("bid"),
                    ask_price=ba.get("ask"),
                    annual_rate=calc_annual_rate(rate_8h, interval_h),
                    funding_interval_h=interval_h,
                    is_delisting=delisting_map.get(symbol, False),
                ))

            if skipped:
                logger.info(f"[{self.name}] 過濾 {skipped} 個已下架合約")

            # BingX: 若交叉驗證修正了 interval，同步更新記憶體快取和檔案快取
            if self.exchange_id == "bingx" and extra_interval_map:
                updated = False
                for sym, val in extra_interval_map.items():
                    cached_val = CcxtExchange._bingx_interval_cache.get(sym)
                    if cached_val is not None and cached_val != val:
                        CcxtExchange._bingx_interval_cache[sym] = val
                        updated = True
                if updated:
                    try:
                        import json as _json
                        cache_path = Path(__file__).resolve().parent.parent / "data" / "bingx_intervals.json"
                        if cache_path.exists():
                            save_data = dict(CcxtExchange._bingx_interval_cache)
                            save_data["_cached_at"] = CcxtExchange._bingx_interval_cache_time
                            with open(cache_path, "w") as f:
                                _json.dump(save_data, f)
                            logger.info(f"[bingx] 已更新 interval 快取（交叉驗證修正）")
                    except Exception:
                        pass

        except Exception as e:
            logger.error(f"[{self.name}] fetch_funding_rates 失敗: {e}")
            raise

        logger.info(f"[{self.name}] 取得 {len(records)} 筆費率")
        return records

    async def _fetch_bid_ask(self) -> dict:
        """批次取得所有合約的 bid1/ask1 價格"""
        result = {}
        try:
            session = await self._get_session()

            if self.exchange_id == "binance":
                async with session.get(
                    "https://fapi.binance.com/fapi/v1/ticker/bookTicker"
                ) as resp:
                    if resp.status == 200:
                        for t in await resp.json():
                            sym = t.get("symbol", "")
                            if sym.endswith("USDT"):
                                base = sym[:-4]
                                bid = float(t["bidPrice"]) if t.get("bidPrice") else None
                                ask = float(t["askPrice"]) if t.get("askPrice") else None
                                if bid:
                                    result[f"{base}/USDT:USDT"] = {"bid": bid, "ask": ask}

            elif self.exchange_id == "bybit":
                async with session.get(
                    "https://api.bybit.com/v5/market/tickers",
                    params={"category": "linear"},
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for t in data.get("result", {}).get("list", []):
                            sym = t.get("symbol", "")
                            if sym.endswith("USDT"):
                                base = sym[:-4]
                                bid = float(t["bid1Price"]) if t.get("bid1Price") else None
                                ask = float(t["ask1Price"]) if t.get("ask1Price") else None
                                if bid:
                                    result[f"{base}/USDT:USDT"] = {"bid": bid, "ask": ask}

            elif self.exchange_id == "okx":
                async with session.get(
                    "https://www.okx.com/api/v5/market/tickers",
                    params={"instType": "SWAP"},
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for t in data.get("data", []):
                            inst_id = t.get("instId", "")
                            if inst_id.endswith("-USDT-SWAP"):
                                base = inst_id.replace("-USDT-SWAP", "")
                                bid = float(t["bidPx"]) if t.get("bidPx") else None
                                ask = float(t["askPx"]) if t.get("askPx") else None
                                if bid:
                                    result[f"{base}/USDT:USDT"] = {"bid": bid, "ask": ask}

            elif self.exchange_id == "bitget":
                async with session.get(
                    "https://api.bitget.com/api/v2/mix/market/tickers",
                    params={"productType": "USDT-FUTURES"},
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for t in data.get("data", []):
                            sym = t.get("symbol", "")
                            if sym.endswith("USDT"):
                                base = sym[:-4]
                                bid = float(t["bidPr"]) if t.get("bidPr") else None
                                ask = float(t["askPr"]) if t.get("askPr") else None
                                if bid:
                                    result[f"{base}/USDT:USDT"] = {"bid": bid, "ask": ask}

            elif self.exchange_id == "gateio":
                async with session.get(
                    "https://api.gateio.ws/api/v4/futures/usdt/tickers"
                ) as resp:
                    if resp.status == 200:
                        for t in await resp.json():
                            contract = t.get("contract", "")
                            if contract.endswith("_USDT"):
                                base = contract.replace("_USDT", "")
                                bid_val = t.get("highest_bid")
                                ask_val = t.get("lowest_ask")
                                bid = float(bid_val) if bid_val else None
                                ask = float(ask_val) if ask_val else None
                                if bid:
                                    result[f"{base}/USDT:USDT"] = {"bid": bid, "ask": ask}

            elif self.exchange_id == "bingx":
                async with session.get(
                    "https://open-api.bingx.com/openApi/swap/v2/quote/ticker"
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for t in data.get("data", []):
                            sym = t.get("symbol", "")
                            if sym.endswith("-USDT"):
                                base = sym.replace("-USDT", "")
                                bid = float(t["bidPrice"]) if t.get("bidPrice") else None
                                ask = float(t["askPrice"]) if t.get("askPrice") else None
                                if bid:
                                    result[f"{base}/USDT:USDT"] = {"bid": bid, "ask": ask}

            logger.info(f"[{self.name}] 取得 {len(result)} 個 bid/ask 價格")
        except Exception as e:
            logger.warning(f"[{self.name}] 取得 bid/ask 失敗: {e}")
        return result

    async def _fetch_bybit_funding_intervals(self) -> dict:
        """Bybit 專屬：從 /v5/market/instruments-info 取得各合約的精確 funding interval"""
        result = {}
        try:
            session = await self._get_session()
            cursor = ""
            while True:
                params = {"category": "linear", "limit": "1000"}
                if cursor:
                    params["cursor"] = cursor
                
                async with session.get(
                    "https://api.bybit.com/v5/market/instruments-info",
                    params=params,
                ) as resp:
                    if resp.status != 200:
                        break
                    data = await resp.json()
                
                res_data = data.get("result", {})
                items = res_data.get("list", [])
                for item in items:
                    sym = item.get("symbol", "")
                    # Bybit: fundingInterval 是分鐘 (integer)
                    interval_min = item.get("fundingInterval")
                    if not sym.endswith("USDT") or interval_min is None:
                        continue
                    
                    try:
                        base = sym[:-4]
                        ccxt_symbol = f"{base}/USDT:USDT"
                        # 轉換分鐘為小時
                        result[ccxt_symbol] = float(interval_min) / 60
                    except (ValueError, TypeError):
                        continue
                
                cursor = res_data.get("nextPageCursor")
                if not cursor:
                    break
            
            non_4h = sum(1 for h in result.values() if h != 4.0)
            logger.info(f"[bybit] 取得 {len(result)} 個 funding interval（{non_4h} 個非 4h）")
        except Exception as e:
            logger.warning(f"[bybit] 取得 funding interval 失敗: {e}")
        return result

    async def _fetch_binance_funding_info(self) -> dict:
        """Binance 專屬：從 /fapi/v1/fundingInfo 取得各合約的 funding interval"""
        try:
            session = await self._get_session()
            async with session.get(
                "https://fapi.binance.com/fapi/v1/fundingInfo"
            ) as resp:
                if resp.status != 200:
                    return {}
                data = await resp.json()
            result = {}
            for item in data:
                sym = item.get("symbol", "")
                if sym.endswith("USDT"):
                    base = sym[:-4]
                    ccxt_symbol = f"{base}/USDT:USDT"
                    hours = item.get("fundingIntervalHours")
                    if hours is not None:
                        result[ccxt_symbol] = float(hours)
            logger.info(f"[binance] 取得 {len(result)} 個 funding interval")
            return result
        except Exception as e:
            logger.warning(f"[binance] 取得 funding interval 失敗: {e}")
            return {}

    # BingX interval 快取（類別層級，所有實例共用）
    _bingx_interval_cache: dict = {}
    _bingx_interval_cache_time: float = 0

    async def _fetch_bingx_funding_intervals(self) -> dict:
        """BingX 專屬：從 funding rate 歷史推算各合約的真實結算週期（24h 快取）"""
        import time as _time

        # 快取有效期 4 小時 (避免交易所頻繁變動但快取未過期)
        if (CcxtExchange._bingx_interval_cache
                and _time.time() - CcxtExchange._bingx_interval_cache_time < 14400):
            logger.info(f"[bingx] 使用 interval 快取（{len(CcxtExchange._bingx_interval_cache)} 個）")
            return CcxtExchange._bingx_interval_cache

        # 先嘗試從本地檔案載入
        cache_path = Path(__file__).resolve().parent.parent / "data" / "bingx_intervals.json"
        if cache_path.exists():
            try:
                import json as _json
                with open(cache_path, "r") as f:
                    cached = _json.load(f)
                cached_at = cached.get("_cached_at", 0)
                if _time.time() - cached_at < 14400:
                    intervals = {k: v for k, v in cached.items() if not k.startswith("_")}
                    CcxtExchange._bingx_interval_cache = intervals
                    CcxtExchange._bingx_interval_cache_time = cached_at
                    non_8h = sum(1 for v in intervals.values() if v != 8)
                    logger.info(f"[bingx] 從檔案載入 interval（{len(intervals)} 個，{non_8h} 個非 8h）")
                    return intervals
            except Exception:
                pass

        # 從 API 查詢
        result = {}
        try:
            session = await self._get_session()
            symbols = [
                s for s in self._exchange.markets
                if s.endswith(":USDT") and self._exchange.markets[s].get("active", True)
            ]
            sem = asyncio.Semaphore(10)

            async def _check_one(ccxt_sym):
                bingx_sym = ccxt_sym.split("/")[0] + "-USDT"
                async with sem:
                    try:
                        async with session.get(
                            "https://open-api.bingx.com/openApi/swap/v2/quote/fundingRate",
                            params={"symbol": bingx_sym, "limit": "2"},
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                if data.get("code") != 0:
                                    return  # rate limit 或其他錯誤，跳過
                                entries = data.get("data", [])
                                if len(entries) >= 2:
                                    t1 = int(entries[0].get("fundingTime", 0))
                                    t2 = int(entries[1].get("fundingTime", 0))
                                    diff_h = abs(t1 - t2) / 3600000
                                    if diff_h > 0:
                                        result[ccxt_sym] = diff_h
                    except Exception:
                        pass

            # 分批查詢（每批 10 個，間隔 300ms）避免 rate limit
            batch_size = 10
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]
                await asyncio.gather(*[_check_one(s) for s in batch], return_exceptions=True)
                if i + batch_size < len(symbols):
                    await asyncio.sleep(0.3)
            non_8h = {s: h for s, h in result.items() if h != 8}
            logger.info(f"[bingx] 取得 {len(result)} 個 funding interval（{len(non_8h)} 個非 8h）")

            # 存到記憶體和檔案
            if result:
                CcxtExchange._bingx_interval_cache = result
                CcxtExchange._bingx_interval_cache_time = _time.time()
                try:
                    import json as _json
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    save_data = dict(result)
                    save_data["_cached_at"] = _time.time()
                    with open(cache_path, "w") as f:
                        _json.dump(save_data, f)
                except Exception:
                    pass
        except Exception as e:
            logger.warning(f"[bingx] 取得 funding interval 失敗: {e}")
        return result

    async def _verify_bingx_interval(self, ccxt_sym: str) -> float | None:
        """BingX: 查詢單一幣種的最近 2 筆結算歷史，精確計算 interval"""
        try:
            bingx_sym = ccxt_sym.split("/")[0] + "-USDT"
            session = await self._get_session()
            async with session.get(
                "https://open-api.bingx.com/openApi/swap/v2/quote/fundingRate",
                params={"symbol": bingx_sym, "limit": "2"},
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if data.get("code") != 0:
                    return None
                entries = data.get("data", [])
                if len(entries) >= 2:
                    t1 = int(entries[0].get("fundingTime", 0))
                    t2 = int(entries[1].get("fundingTime", 0))
                    diff_h = abs(t1 - t2) / 3600000
                    if diff_h > 0:
                        return diff_h
        except Exception:
            pass
        return None

    async def _fetch_mark_prices(self) -> dict:
        """取得標記價格 + 指數價格（OKX 等缺少價格的交易所用）"""
        result = {}
        try:
            if self.exchange_id == "okx":
                # OKX: 分別從 mark-price 和 index-tickers 取得
                session = await self._get_session()
                async with session.get(
                    "https://www.okx.com/api/v5/public/mark-price",
                    params={"instType": "SWAP"},
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for item in data.get("data", []):
                            inst_id = item.get("instId", "")
                            if inst_id.endswith("-USDT-SWAP"):
                                base = inst_id.replace("-USDT-SWAP", "")
                                sym = f"{base}/USDT:USDT"
                                result[sym] = {
                                    "mark": float(item["markPx"]),
                                    "index": None,
                                }
                async with session.get(
                    "https://www.okx.com/api/v5/market/index-tickers",
                    params={"quoteCcy": "USDT"},
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for item in data.get("data", []):
                            inst_id = item.get("instId", "")
                            if inst_id.endswith("-USDT"):
                                base = inst_id.replace("-USDT", "")
                                sym = f"{base}/USDT:USDT"
                                if sym in result:
                                    result[sym]["index"] = float(item["idxPx"])
                                else:
                                    result[sym] = {
                                        "mark": None,
                                        "index": float(item["idxPx"]),
                                    }
            else:
                tickers = await self._exchange.fetch_mark_prices()
                for symbol, t in tickers.items():
                    if not symbol.endswith(":USDT"):
                        continue
                    mark = t.get("markPrice")
                    index = t.get("indexPrice")
                    if mark is not None:
                        result[symbol] = {"mark": mark, "index": index}
            logger.info(f"[{self.name}] 取得 {len(result)} 個標記價格")
        except Exception as e:
            logger.warning(f"[{self.name}] 取得標記價格失敗: {e}")
        return result

    async def _fetch_mexc_direct(self) -> list[FundingRecord]:
        """MEXC 直接 API 取得所有合約費率"""
        records = []
        session = await self._get_session()

        # 先取合約列表
        async with session.get(
            "https://contract.mexc.com/api/v1/contract/detail"
        ) as resp:
            if resp.status != 200:
                raise Exception(f"MEXC contract/detail HTTP {resp.status}")
            data = await resp.json()

        contracts = data.get("data", [])
        usdt_symbols = []
        mexc_delisting_set: set[str] = set()
        for c in contracts:
            if c.get("quoteCoin") != "USDT":
                continue
            sym = c["symbol"]
            usdt_symbols.append(sym)
            # automaticDelivery=1 = 自動結算交付（即將下架但仍可交易）
            if c.get("automaticDelivery") == 1:
                mexc_delisting_set.add(sym)

        # 取 funding rate + collectCycle（結算頻率，小時）
        async with session.get(
            "https://contract.mexc.com/api/v1/contract/funding_rate"
        ) as resp:
            if resp.status != 200:
                raise Exception(f"MEXC funding_rate HTTP {resp.status}")
            data = await resp.json()

        funding_map = {}
        for item in data.get("data", []):
            sym = item.get("symbol", "")
            funding_map[sym] = item

        # 取 ticker（價格等額外資訊）
        async with session.get(
            "https://contract.mexc.com/api/v1/contract/ticker"
        ) as resp:
            if resp.status != 200:
                raise Exception(f"MEXC ticker HTTP {resp.status}")
            data = await resp.json()

        tickers = {t["symbol"]: t for t in data.get("data", [])}

        for sym in usdt_symbols:
            fr = funding_map.get(sym)
            ticker = tickers.get(sym)
            if not fr and not ticker:
                continue

            # 優先從 funding_rate 端點取費率
            rate = None
            if fr:
                rate = fr.get("fundingRate")
            if rate is None and ticker:
                rate = ticker.get("fundingRate")
            if rate is None:
                continue

            rate = float(rate)
            rate_8h = normalize_rate_to_8h(rate, "mexc")

            # 結算頻率（小時）
            interval_h = 8
            if fr and fr.get("collectCycle") is not None:
                try:
                    interval_h = float(fr["collectCycle"])
                except (ValueError, TypeError):
                    pass

            # 轉換 symbol：BTC_USDT -> BTC/USDT:USDT
            base = sym.replace("_USDT", "")
            ccxt_symbol = f"{base}/USDT:USDT"

            mark_price = None
            index_price = None
            bid_price = None
            ask_price = None
            if ticker:
                mark_price = float(ticker.get("fairPrice", 0)) or None
                index_price = float(ticker.get("indexPrice", 0)) or None
                bid_price = float(ticker.get("bid1", 0)) or None
                ask_price = float(ticker.get("ask1", 0)) or None

            # 下次收費時間
            funding_time = None
            nst = None
            if fr:
                nst = fr.get("nextSettleTime")
            if nst:
                try:
                    funding_time = datetime.fromtimestamp(
                        int(nst) / 1000, tz=timezone.utc
                    )
                except (ValueError, TypeError, OSError):
                    pass

            records.append(FundingRecord(
                exchange=self.name,
                symbol=ccxt_symbol,
                funding_rate=rate_8h,
                next_funding_rate=None,
                funding_time=funding_time,
                mark_price=mark_price,
                index_price=index_price,
                bid_price=bid_price,
                ask_price=ask_price,
                annual_rate=calc_annual_rate(rate_8h, interval_h),
                funding_interval_h=interval_h,
                is_delisting=sym in mexc_delisting_set,
            ))

        logger.info(f"[{self.name}] 直接 API 取得 {len(records)} 筆費率")
        return records

    async def _fetch_kucoin_direct(self) -> list[FundingRecord]:
        """KuCoin Futures 直接 API 取得合約費率"""
        records = []
        session = await self._get_session()

        async with session.get(
            "https://api-futures.kucoin.com/api/v1/contracts/active"
        ) as resp:
            if resp.status != 200:
                raise Exception(f"KuCoin contracts/active HTTP {resp.status}")
            data = await resp.json()

        contracts = data.get("data", [])

        # 取得 bid/ask：用 KuCoin allTickers 批次 API
        bid_ask_map = {}
        # 建立 KuCoin symbol -> ccxt symbol 映射
        kucoin_to_ccxt = {}
        for c in contracts:
            ksym = c.get("symbol", "")
            base = c.get("baseCurrency", "")
            if base and c.get("quoteCurrency") == "USDT":
                kucoin_to_ccxt[ksym] = f"{base}/USDT:USDT"
        try:
            async with session.get(
                "https://api-futures.kucoin.com/api/v1/allTickers"
            ) as resp:
                if resp.status == 200:
                    tdata = await resp.json()
                    for t in tdata.get("data", []):
                        ksym = t.get("symbol", "")
                        ccxt_sym = kucoin_to_ccxt.get(ksym)
                        if ccxt_sym:
                            bid = float(t["bestBidPrice"]) if t.get("bestBidPrice") else None
                            ask = float(t["bestAskPrice"]) if t.get("bestAskPrice") else None
                            if bid:
                                bid_ask_map[ccxt_sym] = {"bid": bid, "ask": ask}
            logger.info(f"[{self.name}] 取得 {len(bid_ask_map)} 個 bid/ask 價格")
        except Exception as e:
            logger.warning(f"[{self.name}] allTickers for bid/ask 失敗: {e}")

        for c in contracts:
            if c.get("quoteCurrency") != "USDT":
                continue
            if not c.get("isInverse", False) is False:
                continue

            rate_raw = c.get("fundingFeeRate")
            if rate_raw is None:
                continue

            rate = float(rate_raw)
            rate_8h = normalize_rate_to_8h(rate, "kucoinfutures")

            # 轉換 symbol：XBTUSDTM -> BTC/USDT:USDT
            base_currency = c.get("baseCurrency", "")
            if not base_currency:
                continue
            ccxt_symbol = f"{base_currency}/USDT:USDT"

            mark_price_raw = c.get("markPrice")
            mark_price = float(mark_price_raw) if mark_price_raw is not None else None

            predicted_raw = c.get("predictedFundingFeeRate")
            predicted = float(predicted_raw) if predicted_raw is not None else None

            # 結算頻率：fundingRateGranularity（毫秒）→ 小時
            interval_h = 8
            granularity = c.get("currentFundingRateGranularity") or c.get("fundingRateGranularity")
            if granularity is not None:
                try:
                    interval_h = float(granularity) / 3600000
                except (ValueError, TypeError):
                    pass

            # 指數價格
            index_raw = c.get("indexPrice")
            index_price = float(index_raw) if index_raw is not None else None

            # 下次收費時間
            # KuCoin nextFundingRateTime 可能是剩餘毫秒（小值）或絕對時間戳（大值）
            funding_time = None
            nfrt = c.get("nextFundingRateTime")
            if nfrt:
                try:
                    ts_val = int(nfrt)
                    if ts_val > 1577836800000:  # 絕對時間戳（> 2020-01-01 ms）
                        funding_time = datetime.fromtimestamp(
                            ts_val / 1000, tz=timezone.utc
                        )
                    elif ts_val > 0:  # 剩餘毫秒
                        funding_time = datetime.now(timezone.utc) + timedelta(milliseconds=ts_val)
                except (ValueError, TypeError, OSError):
                    pass
            # 備援：用 interval 計算下次結算時間
            if funding_time is None and interval_h > 0:
                funding_time = _calc_next_funding_time(interval_h)

            # bid/ask
            ba = bid_ask_map.get(ccxt_symbol, {})

            records.append(FundingRecord(
                exchange=self.name,
                symbol=ccxt_symbol,
                funding_rate=rate_8h,
                next_funding_rate=predicted,
                funding_time=funding_time,
                mark_price=mark_price,
                index_price=index_price,
                bid_price=ba.get("bid"),
                ask_price=ba.get("ask"),
                annual_rate=calc_annual_rate(rate_8h, interval_h),
                funding_interval_h=interval_h,
            ))

        logger.info(f"[{self.name}] 直接 API 取得 {len(records)} 筆費率")
        return records

    async def fetch_funding_history(self, symbol: str, since: int = 0, limit: int = 100) -> list[dict]:
        """用 ccxt 取得歷史費率"""
        try:
            # 部分交易所不支援 since 參數，一律不傳，改在結果中過濾
            history = await self._exchange.fetch_funding_rate_history(
                symbol, limit=limit
            )
            return [
                {"timestamp": h.get("timestamp"), "rate": h.get("fundingRate")}
                for h in history
                if h.get("fundingRate") is not None
                and (since == 0 or (h.get("timestamp") or 0) >= since)
            ]
        except Exception as e:
            logger.debug(f"[{self.name}] 歷史費率查詢失敗 {symbol}: {e}")
            return []

    async def close(self):
        await self._exchange.close()
        if self._session and not self._session.closed:
            await self._session.close()

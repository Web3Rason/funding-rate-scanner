"""倉位限制快取：從各交易所取得 max notional（最大名義價值），24h 快取"""

import asyncio
import json
import logging
import time
from pathlib import Path

import hashlib
import hmac

import ccxt.async_support as ccxt
from exchanges.ccxt_exchange import scope_bybit_markets
from exchanges._session import make_session

logger = logging.getLogger(__name__)

CACHE_PATH = Path(__file__).resolve().parent.parent / "data" / "leverage_tiers.json"
CACHE_TTL = 86400  # 24 小時

# 支援批次取得的交易所（1 call 取全部）
BATCH_EXCHANGES = {"bybit", "gateio"}

# 支援逐幣取得的交易所（bingx 太慢，改用按需查詢）
PER_SYMBOL_EXCHANGES = {"okx", "bitget", "kucoinfutures"}

# 記憶體快取：{exchange_id: {ccxt_symbol: max_notional_usdt}}
_cache: dict[str, dict[str, float]] = {}
_cache_ts: float = 0

# CoinW 額外快取：{ccxt_symbol: {"max_piece": 張, "lot_size": 每張幣數}}
# CoinW 無真正的分層，maxPosition 即為檔位 1 上限
_coinw_raw: dict[str, dict] = {}


def get_max_notional(exchange_id: str, symbol: str) -> float | None:
    """從快取取得某交易所某幣種的最大倉位（USDT）"""
    ex_data = _cache.get(exchange_id, {})
    return ex_data.get(symbol)


def get_coinw_position_info(symbol: str) -> dict | None:
    """取得 CoinW 檔位 1 最大持倉資訊：{max_piece, lot_size}"""
    return _coinw_raw.get(symbol)


async def _fetch_coinw_ladder_map(session) -> dict[str, int]:
    """從 getLadderConfig 取得每個 base 的檔位 1 最大持倉張數。
    回傳 {BASE_UPPER: max_piece_ladder1}
    """
    url = "https://futuresapi.coinw.com/v1/futuresc/thirdClient/trade/getLadderConfig"
    try:
        async with session.get(
            url, params={"quote": "-1"},
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.coinw.com/"},
        ) as r:
            if r.status != 200:
                return {}
            data = await r.json(content_type=None)
        if data.get("code") != 0:
            return {}
        result = {}
        for item in (data.get("data", {}).get("ladderConfig") or []):
            base = (item.get("base") or "").upper()
            if not base:
                continue
            for lad in (item.get("ladderList") or []):
                if lad.get("ladder") == 1:
                    mp = lad.get("maxPiece", 0)
                    if mp:
                        result[base] = mp
                    break
        return result
    except Exception as e:
        logger.warning(f"[leverage] coinw getLadderConfig 失敗: {e}")
        return {}


async def _bootstrap_coinw_raw():
    """補齊 CoinW 檔位1 maxPiece + oneLotSize + USDT notional。結合 3 個端點。"""
    try:
        async with make_session(15) as session:
            ladder_map = await _fetch_coinw_ladder_map(session)
            async with session.get("https://api.coinw.com/v1/perpum/instruments") as r:
                if r.status != 200:
                    return
                inst_data = await r.json()
            async with session.get("https://api.coinw.com/v1/perpumPublic/tickers") as r:
                if r.status == 200:
                    tick_data = await r.json()
                else:
                    tick_data = {}
        price_map = {}
        for t in (tick_data.get("data") or []):
            coin = (t.get("base_coin") or "").upper()
            px = t.get("fair_price") or t.get("last_price")
            if coin and px:
                price_map[coin] = float(px)
        ex_data = _cache.setdefault("coinw", {})
        for inst in (inst_data.get("data") or []):
            base = (inst.get("base") or "").upper()
            if inst.get("status") != "online":
                continue
            lot_size = inst.get("oneLotSize", 0)
            tier1_piece = ladder_map.get(base)
            sym = f"{base}/USDT:USDT"
            if tier1_piece and lot_size:
                _coinw_raw[sym] = {"max_piece": tier1_piece, "lot_size": lot_size}
                price = price_map.get(base)
                if price:
                    ex_data[sym] = round(tier1_piece * lot_size * price, 2)
        if _coinw_raw:
            _save_file_cache()
            logger.info(f"[leverage] coinw 檔位1 補齊：{len(_coinw_raw)} 個張數 / {len(ex_data)} 個 USDT（ladder map={len(ladder_map)}）")
    except Exception as e:
        logger.warning(f"[leverage] coinw 檔位1 補齊失敗: {e}")


async def fetch_missing_notionals(symbols_by_exchange: dict, api_keys: dict = None):
    """按需查詢快取中缺失的倉位限制（用於 BingX 等慢速交易所）

    symbols_by_exchange: {exchange_id: [symbol1, symbol2, ...]}
    """
    api_keys = api_keys or {}

    async def _fetch_one_exchange(eid, symbols):
        existing = _cache.get(eid, {})
        missing = [s for s in symbols if s not in existing]
        if not missing:
            return

        opts = {"enableRateLimit": True}
        opts.update(api_keys.get(eid, {}))
        ex = getattr(ccxt, eid)(opts)
        scope_bybit_markets(ex, eid)
        try:
            if not ex.has.get("fetchMarketLeverageTiers"):
                return
            await ex.load_markets()
            sem = asyncio.Semaphore(5)
            fetched = {}

            async def _one(sym):
                if sym not in ex.markets:
                    return
                async with sem:
                    try:
                        tier_list = await ex.fetch_market_leverage_tiers(sym)
                        val = _extract_max_notional(tier_list)
                        if val:
                            fetched[sym] = val
                    except Exception:
                        pass

            await asyncio.gather(*[_one(s) for s in missing])

            if fetched:
                if eid not in _cache:
                    _cache[eid] = {}
                _cache[eid].update(fetched)
                _save_file_cache()
                logger.info(f"[leverage] {eid}: 按需查詢 {len(fetched)}/{len(missing)} 個倉位限制")
        except Exception as e:
            logger.warning(f"[leverage] {eid} 按需查詢失敗: {e}")
        finally:
            await ex.close()

    async def _fetch_coinw_on_demand(symbols):
        """CoinW 按需：用公開 API 批次取得（不需逐幣）"""
        existing = _cache.get("coinw", {})
        missing = [s for s in symbols if s not in existing]
        if not missing:
            return
        # CoinW 公開 API 一次取全部，直接用全量更新
        try:
            async with make_session(15) as session:
                ladder_map = await _fetch_coinw_ladder_map(session)
                async with session.get("https://api.coinw.com/v1/perpum/instruments") as r:
                    if r.status != 200:
                        return
                    inst_data = await r.json()
                async with session.get("https://api.coinw.com/v1/perpumPublic/tickers") as r:
                    if r.status != 200:
                        return
                    tick_data = await r.json()

            price_map = {}
            for t in (tick_data.get("data") or []):
                coin = (t.get("base_coin") or "").upper()
                px = t.get("fair_price") or t.get("last_price")
                if coin and px:
                    price_map[coin] = float(px)

            fetched = {}
            for inst in (inst_data.get("data") or []):
                base = (inst.get("base") or "").upper()
                sym = f"{base}/USDT:USDT"
                if sym not in missing:
                    continue
                lot_size = inst.get("oneLotSize", 0)
                tier1_piece = ladder_map.get(base)
                price = price_map.get(base)
                if tier1_piece and lot_size:
                    _coinw_raw[sym] = {"max_piece": tier1_piece, "lot_size": lot_size}
                if tier1_piece and lot_size and price:
                    fetched[sym] = round(tier1_piece * lot_size * price, 2)

            if fetched:
                if "coinw" not in _cache:
                    _cache["coinw"] = {}
                _cache["coinw"].update(fetched)
                _save_file_cache()
                logger.info(f"[leverage] coinw: 按需取得 {len(fetched)} 個倉位限制（檔位1）")
        except Exception as e:
            logger.warning(f"[leverage] coinw 按需取得失敗: {e}")

    async def _fetch_mexc_on_demand(symbols):
        """MEXC 按需：用公開 contract API 計算（maxVol × contractSize × price）"""
        existing = _cache.get("mexc", {})
        missing = [s for s in symbols if s not in existing]
        if not missing:
            return
        try:
            async with make_session(15) as session:
                async with session.get(
                    "https://contract.mexc.com/api/v1/contract/detail"
                ) as r:
                    if r.status != 200:
                        return
                    detail_data = await r.json()
                if not detail_data.get("success"):
                    return
                async with session.get(
                    "https://contract.mexc.com/api/v1/contract/ticker"
                ) as r:
                    if r.status != 200:
                        return
                    tick_data = await r.json()
                if not tick_data.get("success"):
                    return

            price_map = {}
            for t in (tick_data.get("data") or []):
                sym = (t.get("symbol") or "").replace("_", "")
                px = t.get("fairPrice") or t.get("lastPrice")
                if sym and px:
                    price_map[sym] = float(px)

            fetched = {}
            for c in (detail_data.get("data") or []):
                raw_sym = c.get("symbol", "")
                if not raw_sym.endswith("_USDT"):
                    continue
                base = raw_sym.replace("_USDT", "")
                ccxt_sym = f"{base}/USDT:USDT"
                if ccxt_sym not in missing:
                    continue
                max_vol = c.get("maxVol", 0)
                contract_size = c.get("contractSize", 1)
                price = price_map.get(raw_sym.replace("_", ""))
                if max_vol and contract_size and price:
                    fetched[ccxt_sym] = round(
                        float(max_vol) * float(contract_size) * float(price), 2
                    )

            if fetched:
                if "mexc" not in _cache:
                    _cache["mexc"] = {}
                _cache["mexc"].update(fetched)
                _save_file_cache()
                logger.info(f"[leverage] mexc: 按需取得 {len(fetched)} 個倉位限制")
        except Exception as e:
            logger.warning(f"[leverage] mexc 按需取得失敗: {e}")

    tasks = []
    for eid, syms in symbols_by_exchange.items():
        if eid == "coinw":
            tasks.append(asyncio.wait_for(_fetch_coinw_on_demand(syms), timeout=15))
        elif eid == "mexc":
            tasks.append(asyncio.wait_for(_fetch_mexc_on_demand(syms), timeout=15))
        elif eid in ("bingx", "binance", "aster") and eid not in api_keys:
            continue
        elif not hasattr(ccxt, eid):
            # ccxt 沒有這間所（如 lighter）。_fetch_one_exchange 的 getattr(ccxt, eid) 在 try 之外，
            # 會丟 AttributeError；上面的 gather(return_exceptions=True) 雖然吞得掉，
            # 但那是每輪都白跑一次的靜默失敗，直接在這裡跳過。
            continue
        else:
            tasks.append(asyncio.wait_for(_fetch_one_exchange(eid, syms), timeout=30))

    await asyncio.gather(*tasks, return_exceptions=True)


def _load_file_cache() -> bool:
    """從檔案載入快取，成功回傳 True"""
    global _cache, _cache_ts, _coinw_raw
    try:
        if not CACHE_PATH.exists():
            return False
        with open(CACHE_PATH, "r") as f:
            data = json.load(f)
        cached_at = data.get("_cached_at", 0)
        if time.time() - cached_at > CACHE_TTL:
            return False
        _cache = {k: v for k, v in data.items() if not k.startswith("_")}
        _cache_ts = cached_at
        raw = data.get("_coinw_raw") or {}
        if isinstance(raw, dict):
            _coinw_raw.update(raw)
        total = sum(len(v) for v in _cache.values())
        logger.info(f"[leverage] 從檔案載入快取（{len(_cache)} 交易所, {total} 幣種, CoinW 張數 {len(_coinw_raw)}）")
        return True
    except Exception as e:
        logger.warning(f"[leverage] 檔案快取載入失敗: {e}")
        return False


def _save_file_cache():
    """將快取存到檔案"""
    try:
        CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        data = dict(_cache)
        data["_cached_at"] = time.time()
        data["_coinw_raw"] = _coinw_raw
        with open(CACHE_PATH, "w") as f:
            json.dump(data, f)
    except Exception as e:
        logger.warning(f"[leverage] 檔案快取儲存失敗: {e}")


def _extract_max_notional(tiers: list) -> float | None:
    """從 leverage tiers 中取出最大名義價值（最後一層的 maxNotional）"""
    if not tiers:
        return None
    last = tiers[-1]
    val = last.get("maxNotional")
    if val and val > 0:
        return float(val)
    return None


async def refresh_leverage_cache(api_keys: dict = None):
    """刷新所有支援交易所的倉位限制快取

    api_keys: {exchange_id: {apiKey, secret, ...}}
    """
    global _cache, _cache_ts

    # 先嘗試檔案快取
    if _cache and time.time() - _cache_ts < CACHE_TTL:
        # 舊檔案快取可能沒有 _coinw_raw，補一次
        if not _coinw_raw:
            await _bootstrap_coinw_raw()
        return
    if not _cache and _load_file_cache():
        if not _coinw_raw:
            await _bootstrap_coinw_raw()
        return

    api_keys = api_keys or {}

    async def _fetch_batch(eid):
        """批次取得（bybit, gateio）"""
        opts = {"enableRateLimit": True}
        opts.update(api_keys.get(eid, {}))
        ex = getattr(ccxt, eid)(opts)
        scope_bybit_markets(ex, eid)
        try:
            await ex.load_markets()
            params = {"type": "swap", "settle": "usdt"} if eid == "gateio" else {}
            tiers = await ex.fetch_leverage_tiers(params=params)
            ex_data = {}
            for sym, tier_list in tiers.items():
                if not sym.endswith(":USDT"):
                    continue
                val = _extract_max_notional(tier_list)
                if val:
                    ex_data[sym] = val
            _cache[eid] = ex_data
            logger.info(f"[leverage] {eid}: 批次取得 {len(ex_data)} 個倉位限制")
        except Exception as e:
            logger.warning(f"[leverage] {eid} 批次取得失敗: {e}")
        finally:
            await ex.close()

    async def _fetch_per_symbol(eid):
        """逐幣取得（okx, bitget, kucoinfutures）"""
        opts = {"enableRateLimit": True}
        opts.update(api_keys.get(eid, {}))
        ex = getattr(ccxt, eid)(opts)
        try:
            if not ex.has.get("fetchMarketLeverageTiers"):
                return
            await ex.load_markets()
            usdt_syms = [
                s for s in ex.markets
                if s.endswith(":USDT") and ex.markets[s].get("active", True)
            ]

            ex_data = {}
            sem = asyncio.Semaphore(5)
            errors = 0

            async def _one(sym):
                nonlocal errors
                if errors > 10:
                    return
                async with sem:
                    try:
                        tier_list = await ex.fetch_market_leverage_tiers(sym)
                        val = _extract_max_notional(tier_list)
                        if val:
                            ex_data[sym] = val
                    except Exception:
                        errors += 1

            # 分批（每批 5 個，間隔 200ms）
            batch_size = 5
            for i in range(0, len(usdt_syms), batch_size):
                batch = usdt_syms[i:i + batch_size]
                await asyncio.gather(*[_one(s) for s in batch])
                if i + batch_size < len(usdt_syms):
                    await asyncio.sleep(0.2)

            _cache[eid] = ex_data
            logger.info(f"[leverage] {eid}: 逐幣取得 {len(ex_data)} 個倉位限制")
        except Exception as e:
            logger.warning(f"[leverage] {eid} 逐幣取得失敗: {e}")
        finally:
            await ex.close()

    async def _fetch_coinw():
        """CoinW：結合 getLadderConfig（檔位1）+ instruments（lot size）+ tickers（價格）"""
        try:
            async with make_session(15) as session:
                # 0. 檔位表（真正的檔位1 最大張數）
                ladder_map = await _fetch_coinw_ladder_map(session)

                # 1. 取得合約資訊（oneLotSize）
                async with session.get("https://api.coinw.com/v1/perpum/instruments") as r:
                    if r.status != 200:
                        return
                    inst_data = await r.json()
                instruments = inst_data.get("data") or []

                # 2. 取得即時價格
                async with session.get("https://api.coinw.com/v1/perpumPublic/tickers") as r:
                    if r.status != 200:
                        return
                    tick_data = await r.json()
                tickers = tick_data.get("data") or []

            price_map = {}
            for t in tickers:
                coin = (t.get("base_coin") or "").upper()
                px = t.get("fair_price") or t.get("last_price")
                if coin and px:
                    price_map[coin] = float(px)

            ex_data = {}
            raw_data = {}
            for inst in instruments:
                base = (inst.get("base") or "").upper()
                if inst.get("status") != "online":
                    continue
                lot_size = inst.get("oneLotSize", 0)
                price = price_map.get(base)
                tier1_piece = ladder_map.get(base)
                sym = f"{base}/USDT:USDT"
                if tier1_piece and lot_size:
                    raw_data[sym] = {"max_piece": tier1_piece, "lot_size": lot_size}
                if tier1_piece and lot_size and price:
                    ex_data[sym] = round(tier1_piece * lot_size * price, 2)

            _cache["coinw"] = ex_data
            _coinw_raw.update(raw_data)
            logger.info(f"[leverage] coinw: 取得 {len(ex_data)} 個檔位1 倉位限制（ladder map={len(ladder_map)}）")
        except Exception as e:
            logger.warning(f"[leverage] coinw 取得失敗: {e}")

    async def _fetch_mexc():
        """MEXC：從公開 contract API 計算 max notional（maxVol × contractSize × price）"""
        try:
            async with make_session(15) as session:
                # 1. 取得合約資訊（maxVol, contractSize）
                async with session.get(
                    "https://contract.mexc.com/api/v1/contract/detail"
                ) as r:
                    if r.status != 200:
                        return
                    detail_data = await r.json()
                if not detail_data.get("success"):
                    return
                contracts = detail_data.get("data") or []

                # 2. 取得即時價格
                async with session.get(
                    "https://contract.mexc.com/api/v1/contract/ticker"
                ) as r:
                    if r.status != 200:
                        return
                    tick_data = await r.json()
                if not tick_data.get("success"):
                    return
                tickers = tick_data.get("data") or []

            # 建立價格表（symbol -> price）
            price_map = {}
            for t in tickers:
                sym = (t.get("symbol") or "").replace("_", "")
                px = t.get("fairPrice") or t.get("lastPrice")
                if sym and px:
                    price_map[sym] = float(px)

            ex_data = {}
            for c in contracts:
                raw_sym = c.get("symbol", "")  # e.g. "MOLT_USDT"
                if not raw_sym.endswith("_USDT"):
                    continue
                base = raw_sym.replace("_USDT", "")
                max_vol = c.get("maxVol", 0)
                contract_size = c.get("contractSize", 1)
                price = price_map.get(raw_sym.replace("_", ""))
                if max_vol and contract_size and price:
                    max_notional = float(max_vol) * float(contract_size) * float(price)
                    sym = f"{base}/USDT:USDT"
                    ex_data[sym] = round(max_notional, 2)

            _cache["mexc"] = ex_data
            logger.info(f"[leverage] mexc: 取得 {len(ex_data)} 個倉位限制")
        except Exception as e:
            logger.warning(f"[leverage] mexc 取得失敗: {e}")

    async def _fetch_leverage_bracket(eid, base_url, use_papi=False):
        """用 leverageBracket API 取得倉位限制（Binance PAPI / Aster FAPI）"""
        keys = api_keys.get(eid, {})
        api_key = keys.get("apiKey", "")
        secret = keys.get("secret", "")
        if not api_key or not secret:
            return
        try:
            ts = str(int(time.time() * 1000))
            query = f"timestamp={ts}"
            sig = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
            # Binance 統一帳戶用 PAPI，其他用 FAPI
            path = "/papi/v1/um/leverageBracket" if use_papi else "/fapi/v1/leverageBracket"
            url = f"{base_url}{path}?{query}&signature={sig}"
            headers = {"X-MBX-APIKEY": api_key}
            async with make_session(15) as session:
                async with session.get(url, headers=headers) as r:
                    if r.status != 200:
                        logger.warning(f"[leverage] {eid} leverageBracket HTTP {r.status}")
                        return
                    data = await r.json()

            ex_data = {}
            for item in data:
                raw_sym = item.get("symbol", "")
                if not raw_sym.endswith("USDT"):
                    continue
                brackets = item.get("brackets", [])
                if brackets:
                    cap = brackets[-1].get("notionalCap") or brackets[-1].get("notionalFloor", 0)
                    if cap and cap > 0:
                        base = raw_sym[:-4]
                        ex_data[f"{base}/USDT:USDT"] = float(cap)

            _cache[eid] = ex_data
            logger.info(f"[leverage] {eid}: leverageBracket 取得 {len(ex_data)} 個倉位限制")
        except Exception as e:
            logger.warning(f"[leverage] {eid} leverageBracket 失敗: {e}")

    # 各交易所獨立抓取，每個完成即更新快取，加 timeout 避免卡住
    async def _run_with_timeout(coro, eid, timeout_s=90):
        try:
            await asyncio.wait_for(coro, timeout=timeout_s)
        except asyncio.TimeoutError:
            logger.warning(f"[leverage] {eid} 超時（{timeout_s}s），已跳過")

    tasks = []
    for eid in BATCH_EXCHANGES:
        tasks.append(_run_with_timeout(_fetch_batch(eid), eid, 30))
    # Binance: 統一帳戶用 PAPI（需 API key）
    if "binance" in api_keys:
        tasks.append(_run_with_timeout(
            _fetch_leverage_bracket("binance", "https://papi.binance.com", use_papi=True), "binance", 30))
    # Aster: Binance 相容 FAPI（需 API key）
    if "aster" in api_keys:
        tasks.append(_run_with_timeout(
            _fetch_leverage_bracket("aster", "https://fapi.asterdex.com", use_papi=False), "aster", 30))
    for eid in PER_SYMBOL_EXCHANGES:
        tasks.append(_run_with_timeout(_fetch_per_symbol(eid), eid, 90))
    # CoinW / MEXC 公開 API
    tasks.append(_run_with_timeout(_fetch_coinw(), "coinw", 15))
    tasks.append(_run_with_timeout(_fetch_mexc(), "mexc", 15))

    await asyncio.gather(*tasks, return_exceptions=True)

    if _cache:
        _cache_ts = time.time()
        _save_file_cache()
        total = sum(len(v) for v in _cache.values() if isinstance(v, dict))
        logger.info(f"[leverage] 快取更新完成：{len(_cache)} 交易所, {total} 幣種")

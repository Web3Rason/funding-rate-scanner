"""5011 Funding Rate Scanner - FastAPI 入口"""

import asyncio
import logging
import hashlib
import json
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta, timezone
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from typing import Optional
from services.funding_scanner import FundingScanner
from services.ws_manager import WsManager
from services.premium_tracker import PremiumTracker
from services.binance_premium_tracker import BinancePremiumTracker
from services.premium_alert import PremiumAlerter
from services.arbitrage_detector import reload_aliases
import services.arbitrage_detector as _arb_mod

# 設定日誌
LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        # 輪替：單檔上限 50MB、保留 3 份備份（總計約 200MB 封頂），
        # 取代原本不輪替的 FileHandler（曾長到 3.9GB → 持續磁碟 I/O 拖慢全機）。
        RotatingFileHandler(
            LOG_DIR / "scanner.log", maxBytes=50 * 1024 * 1024,
            backupCount=3, encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)

# 讀取設定
CONFIG_PATH = Path(__file__).parent.parent / "config.json"
config = {}
if CONFIG_PATH.exists():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)

scanner = FundingScanner(
    scan_interval=config.get("scan_interval", 30),
    min_annual_diff=config.get("min_annual_diff", 10.0),
    bybit_api=config.get("bybit_api"),
    okx_api=config.get("okx_api"),
    bingx_api=config.get("bingx_api"),
    mexc_api=config.get("mexc_api"),
    binance_api=config.get("binance_api"),
    aster_api=config.get("aster_api"),
    bitget_api=config.get("bitget_api"),
    gateio_api=config.get("gateio_api"),
    bybit_vip_level=config.get("bybit_vip_level", "No VIP"),
    crossex_api=config.get("crossex_api"),
)
ws_manager = WsManager(scanner)
premium_tracker = PremiumTracker()
binance_premium_tracker = BinancePremiumTracker()
# 極端溢價即時告警：任一家 |premium|≥8% → TG（同幣 30 分去重）
# 閾值 8% 依歷史回測（樣本內外雙驗、鄰域穩健的最佳值；2% 全在雜訊區）
premium_alerter = PremiumAlerter(premium_tracker, binance_premium_tracker,
                                 threshold_pct=8.0, cooldown_min=30)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Funding Rate Scanner 啟動中...")
    await scanner.start()
    await premium_tracker.start()
    await binance_premium_tracker.start()
    # 極端溢價告警（⚠ 極端溢價 TG）已停用；溢價追蹤器仍運作供儀表板使用
    # await premium_alerter.start()
    yield
    # Shutdown
    logger.info("Funding Rate Scanner 關閉中...")
    # await premium_alerter.stop()
    await binance_premium_tracker.stop()
    await premium_tracker.stop()
    await scanner.stop()
    from exchanges._session import close_shared_connector
    await close_shared_connector()


app = FastAPI(title="Funding Rate Scanner", lifespan=lifespan)

# 這個 API 沒有身分驗證，所以 CORS 只開本機來源。
# 要從別的網域存取，用環境變數 CORS_ORIGINS 指定（逗號分隔），
# 但**先在前面加一層認證再說** —— 直接開 "*" 等於把控制權交給任何網頁。
_origins = os.environ.get("CORS_ORIGINS")
app.add_middleware(
    CORSMiddleware,
    allow_origins=(
        [o.strip() for o in _origins.split(",") if o.strip()]
        if _origins else
        ["http://localhost:5011", "http://127.0.0.1:5011",
         "http://localhost:5173", "http://127.0.0.1:5173"]
    ),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/status")
async def get_status():
    """掃描器狀態"""
    result = scanner.last_result
    return {
        "is_running": scanner.is_running,
        "scan_interval": scanner.scan_interval,
        "last_scan": result.timestamp.isoformat() if result else None,
        "scan_duration_ms": result.scan_duration_ms if result else None,
        "total_records": len(result.records) if result else 0,
        "total_arbitrage": len(result.arbitrage) if result else 0,
        "errors": result.errors if result else {},
        "exchanges": [ex.name for ex in scanner._exchanges],
    }


# 「無篩選、預設排序」這條熱路徑的序列化快取。
#
# 為什麼需要：前端 App.jsx 的 usePolling 每 2 秒打一次這支，而掃描 300 秒才更新一次
# → 每輪有 149/150 次是把【完全相同】的 9,147 筆資料重新序列化一遍。
# 實測單次回應 3.25 MB、耗時 0.24s，等於光這支端點就吃掉 12% 的一顆核心下限；
# py-spy 剖析裡 encoders.py(jsonable_encoder) 13.10% + iterencode 3.58%
# + model_dump 1.57% + is_dataclass 1.95% ≈ 20%，前 16 熱點有 5 個是它。
#
# 兩層優化：
#   1. 依 scan timestamp 快取序列化後的 bytes → 每輪只做 1 次而非 150 次
#   2. 附 ETag，瀏覽器下次帶 If-None-Match 就回 304（連 3.25 MB 都不用送）
# 帶篩選/排序參數的呼叫（幣種詳情頁）走原本的路徑，不影響。
_rates_cache: dict = {"key": None, "body": b"", "etag": ""}


def _build_rates_payload(result) -> tuple[bytes, str]:
    """序列化一次並算出 ETag。

    ⚠ 刻意沿用原本的 model_dump() + jsonable_encoder，不改用更快的 model_dump(mode="json")：
      兩者的 datetime 格式【不一樣】——jsonable_encoder 走 .isoformat() 產出
      "2026-08-11T16:00:00+00:00"，Pydantic 的 json 模式產出 "2026-08-11T16:00:00Z"。
      實測 9,147 筆逐欄比對，差異只在 funding_time 這個欄位，但前端有多處直接吃這個字串。
      快取本身已經把 150 次序列化降成 1 次（省掉 99.3%），
      為了再多 3.8 倍而冒格式不相容的風險不划算。
    """
    payload = jsonable_encoder({
        "records": [r.model_dump() for r in
                    sorted(result.records, key=lambda r: r.symbol)],
        "timestamp": result.timestamp.isoformat(),
        "scan_duration_ms": result.scan_duration_ms,
    })
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    return body, f'W/"{hashlib.md5(body).hexdigest()}"'


@app.get("/api/funding-rates")
async def get_funding_rates(
    request: Request,
    symbol: Optional[str] = Query(None, description="篩選幣種，例如 BTC/USDT:USDT"),
    exchange: Optional[str] = Query(None, description="篩選交易所"),
    sort: str = Query("symbol", description="排序欄位"),
    order: str = Query("asc", description="排序方向 asc/desc"),
):
    """費率查詢"""
    if not scanner.last_result:
        return {"records": [], "timestamp": None}

    # ── 熱路徑：前端每 2 秒的無參數輪詢 ──
    if not symbol and not exchange and sort == "symbol" and order.lower() == "asc":
        result = scanner.last_result
        key = (result.timestamp.isoformat(), len(result.records))
        if _rates_cache["key"] != key:
            body, etag = _build_rates_payload(result)
            _rates_cache.update(key=key, body=body, etag=etag)
        etag = _rates_cache["etag"]
        # no-cache = 每次都回來驗證，但內容沒變就只回 304（不送 body）
        headers = {"ETag": etag, "Cache-Control": "no-cache"}
        if request.headers.get("if-none-match") == etag:
            return Response(status_code=304, headers=headers)
        return Response(content=_rates_cache["body"],
                        media_type="application/json", headers=headers)

    records = scanner.last_result.records

    # 篩選
    if symbol:
        symbol_upper = symbol.upper()
        records = [r for r in records if symbol_upper in r.symbol.upper()]
    if exchange:
        exchange_lower = exchange.lower()
        records = [r for r in records if r.exchange.lower() == exchange_lower]

    # 排序
    reverse = order.lower() == "desc"
    if sort == "funding_rate":
        records = sorted(records, key=lambda r: r.funding_rate, reverse=reverse)
    elif sort == "annual_rate":
        records = sorted(records, key=lambda r: r.annual_rate, reverse=reverse)
    elif sort == "exchange":
        records = sorted(records, key=lambda r: r.exchange, reverse=reverse)
    else:
        records = sorted(records, key=lambda r: r.symbol, reverse=reverse)

    return {
        "records": [r.model_dump() for r in records],
        "timestamp": scanner.last_result.timestamp.isoformat(),
        "scan_duration_ms": scanner.last_result.scan_duration_ms,
    }


@app.get("/api/arbitrage")
async def get_arbitrage(
    min_diff: Optional[float] = Query(None, description="最低年化利差閾值"),
    symbol: Optional[str] = Query(None, description="篩選幣種"),
):
    """套利機會"""
    if not scanner.last_result:
        return {"arbitrage": [], "timestamp": None}

    opportunities = scanner.last_result.arbitrage

    if min_diff is not None:
        opportunities = [o for o in opportunities if o.annual_diff >= min_diff]
    if symbol:
        symbol_upper = symbol.upper()
        opportunities = [
            o for o in opportunities if symbol_upper in o.symbol.upper()
        ]

    return {
        "arbitrage": [o.model_dump() for o in opportunities],
        "timestamp": scanner.last_result.timestamp.isoformat(),
    }


@app.get("/api/funding-history")
async def get_funding_history(
    symbol: str = Query(..., description="幣種，例如 BTC/USDT:USDT"),
    limit: int = Query(100, description="每個交易所取幾筆歷史"),
):
    """某幣種在各交易所的歷史資金費率"""
    history = await scanner.fetch_symbol_history(symbol, limit)
    return {"symbol": symbol, "history": history}


@app.get("/api/funding-realtime")
async def get_funding_realtime(
    symbol: str = Query(..., description="幣種，例如 BTC/USDT:USDT"),
):
    """即時查詢某幣種在各交易所的最新費率"""
    from services.leverage_cache import get_max_notional, fetch_missing_notionals
    records = await scanner.fetch_symbol_realtime(symbol)

    # 附加倉位限制
    dumps = []
    missing_by_ex = {}
    for r in records:
        d = r.model_dump()
        mn = get_max_notional(r.exchange, r.symbol)
        if mn is None:
            missing_by_ex.setdefault(r.exchange, []).append(r.symbol)
        d["max_notional"] = mn
        dumps.append(d)

    # 按需查缺失的
    if missing_by_ex:
        try:
            await fetch_missing_notionals(missing_by_ex, scanner._leverage_api_keys())
            for d in dumps:
                if d["max_notional"] is None:
                    d["max_notional"] = get_max_notional(d["exchange"], d["symbol"])
        except Exception:
            pass

    return {
        "symbol": symbol,
        "records": dumps,
    }


@app.get("/api/price-premium")
async def get_price_premium(
    symbol: str = Query(..., description="幣種，例如 BTC/USDT:USDT"),
    exchange_a: str = Query(..., description="交易所 A（做空所）"),
    exchange_b: str = Query(..., description="交易所 B（做多所）"),
    days: int = Query(3, description="幾天"),
):
    """取得兩交易所的 5 分 K 線價格溢價歷史"""
    premium = await scanner.fetch_price_premium(symbol, exchange_a, exchange_b, days)
    return {"symbol": symbol, "exchange_a": exchange_a, "exchange_b": exchange_b, "data": premium}


@app.get("/api/premium-index")
async def get_premium_index(
    search: Optional[str] = Query(None, description="篩選幣種關鍵字"),
):
    """Bybit 全市場即時溢價指數（WS proxy + 焦點幣官方溢價）"""
    rows = premium_tracker.snapshot()
    if search:
        kw = search.upper()
        rows = [r for r in rows if kw in r["base"]]
    return {
        "data": rows,
        "total": len(rows),
        "ws_connected": premium_tracker.ws_connected,
        "last_bootstrap": premium_tracker.last_bootstrap.isoformat()
        if premium_tracker.last_bootstrap else None,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/futures-spot-premium")
async def get_futures_spot_premium(
    symbol: str = Query(..., description="幣種，例如 BTC/USDT:USDT"),
    exchange: str = Query(..., description="交易所"),
    interval: str = Query("1m", description="K線間隔：1m, 5m, 15m, 1h"),
    hours: int = Query(12, description="查詢時數"),
):
    """某幣種在指定交易所的合約對現貨溢價"""
    data = await scanner.fetch_futures_spot_premium(symbol, exchange, interval, hours)
    return {"symbol": symbol, "exchange": exchange, "interval": interval, "data": data}


@app.get("/api/spot-arbitrage")
async def get_spot_arbitrage(
    min_spread: float = Query(0.5, description="最低價差閾值（百分比）"),
    symbol: Optional[str] = Query(None, description="篩選幣種"),
):
    """現貨搬磚機會"""
    opportunities = scanner.spot_arbitrage

    if min_spread > 0:
        opportunities = [o for o in opportunities if o["spread_pct"] >= min_spread]
    if symbol:
        symbol_upper = symbol.upper()
        opportunities = [
            o for o in opportunities if symbol_upper in o["base_coin"].upper()
        ]

    return {
        "opportunities": opportunities,
        "total_coins": len(scanner.spot_arbitrage),
        "scan_duration_ms": scanner._spot_scan_duration_ms,
        "scanned_coins": scanner._spot_scan_total_coins,
    }


# 期現套利各所快取對照（新增交易所時在此註冊即併入統一端點）
SPOT_FUTURES_CACHES = {
    "bybit": "bybit_spot_futures",
    "bitget": "bitget_spot_futures",
    "okx": "okx_spot_futures",
    "binance": "binance_spot_futures",
    "gateio": "gateio_spot_futures",
    "mexc": "mexc_spot_futures",
    "bingx": "bingx_spot_futures",
}


@app.get("/api/spot-futures")
async def get_spot_futures(
    min_rate: float = Query(0, description="最低資費閾值（小數），0 = 不篩選"),
):
    """統一期現套利機會（合併各交易所快取，每筆標記 exchange）"""
    if not scanner.last_result:
        return {"opportunities": [], "timestamp": None, "totals": {}}

    opportunities = []
    totals = {}
    for ex, attr in SPOT_FUTURES_CACHES.items():
        cache = getattr(scanner, attr, None) or []
        for o in cache:
            if abs(o["funding_rate"]) >= min_rate:
                opportunities.append({**o, "exchange": ex})
        totals[ex] = sum(1 for r in scanner.last_result.records if r.exchange == ex)

    return {
        "opportunities": opportunities,
        "timestamp": scanner.last_result.timestamp.isoformat(),
        "totals": totals,
    }


@app.get("/api/rwa-arb")
async def get_rwa_arb():
    """RWA 期現套利：買現貨 + 空永續（Delta Neutral 收 Funding），限跨所統一帳戶可交易。"""
    from services import rwa_arb

    uni = rwa_arb.load_universe()
    return {
        "opportunities": scanner.rwa_arb_cache,       # 每小時由 _rwa_compute_loop 更新
        "universe_count": len(uni.get("universe") or {}),
        "universe_ts": uni.get("_ts"),
        "market_state": rwa_arb.us_market_state(),
        "computed_at": scanner.rwa_computed_at,
        "timestamp": scanner.last_result.timestamp.isoformat() if scanner.last_result else None,
    }


@app.get("/api/rwa-spread")
async def get_rwa_spread():
    """RWA 跨所價差套利：同一標的的永續在不同交易所的價差（指數更新時點不同造成）。"""
    from services import rwa_arb

    return {
        "opportunities": scanner.rwa_spread_cache,    # 每小時由 _rwa_compute_loop 更新
        "market_state": rwa_arb.us_market_state(),
        "notify_threshold": rwa_arb.XS_NOTIFY_PROFIT_PCT,
        "computed_at": scanner.rwa_computed_at,
        "timestamp": scanner.last_result.timestamp.isoformat() if scanner.last_result else None,
    }


@app.get("/api/rwa-leveraged")
async def get_rwa_leveraged():
    """槓桿型交易對套利：槓桿 ETF 實際價 vs 由標的推算的理論價之偏離。"""
    from services import rwa_arb

    return {
        "opportunities": scanner.rwa_leveraged_cache,   # 每小時由 _rwa_compute_loop 更新
        # Gate 自家再平衡槓桿代幣（只有現貨、借不到故無法做空）→ 只列可買進的負偏離
        "spot_tokens": scanner.rwa_lev_spot_cache,
        "market_state": rwa_arb.us_market_state(),
        "min_deviation": rwa_arb.LEV_MIN_DEVIATION_PCT,
        "spot_min_deviation": rwa_arb.LEV_SPOT_MIN_DEVIATION_PCT,
        "spot_min_volume": rwa_arb.LEV_SPOT_MIN_VOLUME_USDT,
        "computed_at": scanner.rwa_computed_at,
    }


@app.get("/api/bybit-spot-futures")
async def get_bybit_spot_futures(
    min_rate: float = Query(0, description="最低資費閾值（小數），0 = 不篩選"),
):
    """Bybit 期現套利機會（從掃描快取讀取，不即時抓取）"""
    if not scanner.last_result:
        return {"opportunities": [], "timestamp": None}

    total_bybit = sum(1 for r in scanner.last_result.records if r.exchange == "bybit")

    # 從快取篩選符合閾值的機會
    filtered = [
        o for o in scanner.bybit_spot_futures
        if abs(o["funding_rate"]) >= min_rate
    ]

    return {
        "opportunities": filtered,
        "timestamp": scanner.last_result.timestamp.isoformat(),
        "total_bybit": total_bybit,
    }


@app.get("/api/bitget-spot-futures")
async def get_bitget_spot_futures(
    min_rate: float = Query(0, description="最低資費閾值（小數），0 = 不篩選"),
):
    """Bitget 期現套利機會（從掃描快取讀取，不即時抓取）"""
    if not scanner.last_result:
        return {"opportunities": [], "timestamp": None}

    total_bitget = sum(1 for r in scanner.last_result.records if r.exchange == "bitget")

    # 從快取篩選符合閾值的機會
    filtered = [
        o for o in scanner.bitget_spot_futures
        if abs(o["funding_rate"]) >= min_rate
    ]

    return {
        "opportunities": filtered,
        "timestamp": scanner.last_result.timestamp.isoformat(),
        "total_bitget": total_bitget,
    }


@app.get("/api/coinw-meat")
async def get_meat_flow(
    min_daily_diff: float = Query(0.5, description="最低每日資費差閾值（百分比）"),
    mode: str = Query("all", description="計算模式：all=全交易所配對, coinw=以CoinW為一側, same_interval=同結算週期, custom=自選兩交易所"),
    ex_a: str = Query("", description="自選模式：交易所 A"),
    ex_b: str = Query("", description="自選模式：交易所 B"),
):
    """碎肉流：全交易所配對過去 24h 真實日資費差（從快取讀取）"""
    if not scanner.last_result:
        return {"opportunities": [], "timestamp": None}

    # 統計多交易所幣種數
    sym_exs = {}
    for r in scanner.last_result.records:
        key = r.normalized_symbol or r.symbol
        sym_exs.setdefault(key, set()).add(r.exchange)
    total_symbols = sum(1 for exs in sym_exs.values() if len(exs) >= 2)

    if mode == "custom":
        if not ex_a or not ex_b:
            return {"opportunities": [], "timestamp": None, "total_symbols": total_symbols,
                    "bootstrapping": scanner._meat_bootstrapping, "error": "custom 模式需指定 ex_a 與 ex_b"}
        source = scanner.compute_custom_pair_meat_flow(ex_a, ex_b)
    elif mode == "coinw":
        source = scanner.meat_flow_coinw
    elif mode == "same_interval":
        source = scanner.meat_flow_same_interval
    else:
        source = scanner.meat_flow

    filtered = [
        o for o in source
        if abs(o["daily_diff_pct"]) >= min_daily_diff
    ]

    # 歷史資料統計
    history_points = 0
    if scanner._rate_history:
        from datetime import datetime, timedelta, timezone
        cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        unique_ts = set(r["ts"] for r in scanner._rate_history if r["ts"] > cutoff_24h)
        history_points = len(unique_ts)

    return {
        "opportunities": filtered,
        "timestamp": scanner.last_result.timestamp.isoformat(),
        "total_symbols": total_symbols,
        "bootstrapping": scanner._meat_bootstrapping,
        "history_points": history_points,
        "history_total": len(scanner._rate_history),
    }


@app.get("/api/index-tracking")
async def get_index_tracking(
    min_deviation: float = Query(0.0, description="最低偏離百分比門檻"),
    exchange: str = Query("", description="篩選特定交易所"),
):
    """指數追蹤：各交易所指數價格相對 Binance 的偏離"""
    if not scanner.last_result:
        return {"anomalies": [], "all_deviations": [], "timestamp": None}

    anomalies = scanner.index_tracking
    all_devs = scanner.index_all_deviations

    # 篩選
    if min_deviation > 0:
        all_devs = [d for d in all_devs if abs(d["deviation_pct"]) >= min_deviation]
    if exchange:
        all_devs = [d for d in all_devs if d["exchange"] == exchange.lower()]
        anomalies = [a for a in anomalies if a["exchange"] == exchange.lower()]

    # 標記近期有成分「結構」變更的 symbol+exchange（只算交易所新增/移除，8 小時內，不算權重浮動）
    cutoff_8h = (datetime.now(timezone.utc) - timedelta(hours=8)).isoformat()
    sym_ex_with_changes = {
        (c["symbol"], c["source"])
        for c in scanner._constituent_changes
        if c.get("type") in ("added", "removed") and c.get("ts", "") >= cutoff_8h
    }
    def _tag(items):
        return [{**d, "constituent_changed": (d["symbol"], d["exchange"]) in sym_ex_with_changes} for d in items]

    return {
        "anomalies": _tag(anomalies),
        "all_deviations": _tag(all_devs),
        "timestamp": scanner.last_result.timestamp.isoformat(),
        "total_pairs": len(scanner.index_all_deviations),
    }


@app.get("/api/index-tracking/history")
async def get_index_tracking_history(
    symbol: str = Query(..., description="幣種（如 IR/USDT:USDT）"),
    exchange: str = Query("", description="篩選交易所（不填回傳所有）"),
):
    """指數追蹤歷史：特定幣種的偏離時序資料（串流讀磁碟 JSONL，放 thread 不阻塞事件迴圈）"""
    history = await asyncio.to_thread(scanner.read_index_history_api, symbol, exchange)
    return {"history": history, "symbol": symbol}


@app.get("/api/index-constituents")
async def get_index_constituents(
    symbol: str = Query(..., description="幣種（如 BTC/USDT:USDT）"),
):
    """查詢特定幣種的指數成分（各交易所）"""
    raw = scanner._constituent_snapshot.get(symbol, {})
    snapshot = {k: v for k, v in raw.items() if not k.startswith("_")}
    changes = [c for c in scanner._constituent_changes if c["symbol"] == symbol]
    return {
        "symbol": symbol,
        "constituents": snapshot,
        "changes": changes,
        "has_changes": len(changes) > 0,
        "watching": scanner.is_index_watched(symbol),
    }


@app.post("/api/index-constituents/scan")
async def scan_index_constituents(
    symbol: str = Query(..., description="幣種（如 BTC/USDT:USDT）"),
):
    """立即觸發單一幣種的指數成分掃描"""
    await scanner.scan_constituents_for_symbol(symbol)
    raw = scanner._constituent_snapshot.get(symbol, {})
    snapshot = {k: v for k, v in raw.items() if not k.startswith("_")}
    changes = [c for c in scanner._constituent_changes if c["symbol"] == symbol]
    return {
        "symbol": symbol,
        "constituents": snapshot,
        "changes": changes,
        "has_changes": len(changes) > 0,
        "watching": scanner.is_index_watched(symbol),
    }


@app.get("/api/spot-borrow-rates")
async def get_spot_borrow_rates(
    coin: str = Query(..., description="幣種（base coin，如 TAIKO）"),
):
    """查某幣現貨保證金借幣年化利率(%)，供套利建議空方現貨計入借貸成本（目前 Bybit/Bitget）"""
    return await scanner.fetch_spot_borrow_rates(coin.upper())


@app.post("/api/index-constituents/watch")
async def toggle_index_watch(
    symbol: str = Query(..., description="幣種（如 BTC/USDT:USDT）"),
    enable: bool = Query(..., description="true=開始持續監控，false=取消"),
):
    """切換單一幣種的指數成分「持續監控」（5011 開著就會每 5 秒比對，變動發 TG）"""
    watching = scanner.add_index_watch(symbol) if enable else scanner.remove_index_watch(symbol)
    return {"symbol": symbol, "watching": watching}


@app.get("/api/index-constituents/changes")
async def get_constituent_changes(
    hours: int = Query(24, description="回看小時數"),
):
    """查詢所有幣種的指數成分變更"""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    changes = [c for c in scanner._constituent_changes if c["ts"] > cutoff]
    by_symbol = {}
    for c in changes:
        sym = c["symbol"]
        if sym not in by_symbol:
            by_symbol[sym] = []
        by_symbol[sym].append(c)
    return {
        "changes": changes,
        "by_symbol": by_symbol,
        "total_changes": len(changes),
        "total_symbols": len(scanner._constituent_snapshot),
    }


@app.get("/api/aliases")
async def get_aliases():
    """回傳反向別名對照表：canonical -> [alias, ...]，供前端搜尋擴展用。
    直接讀 JSON 檔確保 reload 後立即反映，不依賴 import 的快取變數。"""
    import json as _json
    from pathlib import Path as _Path
    aliases_path = _Path(__file__).resolve().parent / "symbol_aliases.json"
    try:
        with open(aliases_path, "r", encoding="utf-8") as f:
            data = _json.load(f)
    except Exception:
        return {"reverse": {}}
    reverse: dict[str, list[str]] = {}
    for variant, canonical in data.get("aliases", {}).items():
        if variant.startswith("_"):
            continue
        reverse.setdefault(canonical.upper(), []).append(variant.upper())
    return {"reverse": reverse}


@app.post("/api/admin/reload-aliases")
async def reload_symbol_aliases():
    """熱重載 symbol_aliases.json（不需重啟）"""
    reload_aliases()
    return {"status": "ok", "message": "aliases reloaded"}


@app.post("/api/scan")
async def manual_scan():
    """手動觸發掃描"""
    result = await scanner.scan_once()
    return {
        "timestamp": result.timestamp.isoformat(),
        "total_records": len(result.records),
        "total_arbitrage": len(result.arbitrage),
        "errors": result.errors,
        "scan_duration_ms": result.scan_duration_ms,
    }


@app.websocket("/ws/symbol")
async def websocket_symbol(websocket: WebSocket):
    """即時 ticker 串流（資金費率 + 買一賣一）"""
    symbol = websocket.query_params.get("symbol")
    if not symbol:
        await websocket.close(code=4000, reason="Missing symbol parameter")
        return
    await websocket.accept()
    try:
        await ws_manager.handle_client(websocket, symbol)
    except WebSocketDisconnect:
        pass


DIST_DIR = Path(__file__).parent.parent / "frontend" / "dist"
if DIST_DIR.exists():
    app.mount("/assets", StaticFiles(directory=DIST_DIR / "assets"), name="assets")

    @app.get("/")
    async def serve_index():
        return FileResponse(DIST_DIR / "index.html")

    _DIST_ROOT = DIST_DIR.resolve()

    @app.get("/{path:path}")
    async def serve_spa(path: str):
        # safe-join：解析後必須仍在 dist/ 底下，否則一律回 index.html。
        # 不做這個檢查的話，`GET /../config.json` 之類的請求能逃出 dist/
        # 讀到 repo 裡的任意檔案（例如含金鑰的 config.json）。
        candidate = (DIST_DIR / path).resolve()
        if (candidate == _DIST_ROOT or _DIST_ROOT in candidate.parents) \
                and candidate.is_file():
            return FileResponse(candidate)
        return FileResponse(DIST_DIR / "index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=5011)

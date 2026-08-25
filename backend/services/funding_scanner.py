"""掃描排程器：背景 loop 定時掃描所有交易所費率"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import statistics
import time
from array import array
from datetime import datetime, timedelta, timezone
from pathlib import Path
import aiohttp
import ccxt.async_support as ccxt_async
from exchanges.base import BaseExchange
from exchanges.ccxt_exchange import CcxtExchange, scope_bybit_markets
from exchanges.binance_exchange import BinanceExchange
from exchanges.bybit_exchange import BybitExchange
from exchanges.okx_exchange import OkxExchange
from exchanges.bitget_exchange import BitgetExchange
from exchanges.mexc_exchange import MexcExchange
from exchanges.gateio_exchange import GateioExchange
from exchanges.bingx_exchange import BingxExchange
from exchanges.kucoin_exchange import KucoinExchange
from exchanges.aster_exchange import AsterExchange
from exchanges.coinw_exchange import CoinwExchange
from exchanges.hyperliquid_exchange import HyperliquidExchange
from exchanges.tradexyz_exchange import TradeXyzExchange
from exchanges.ourbit_exchange import OurbitExchange
from exchanges.deepcoin_exchange import DeepCoinExchange
from exchanges.lbank_exchange import LbankExchange
# BitMart 已停止營運，模組保留但不再註冊：from exchanges.bitmart_exchange import BitmartExchange
from exchanges.kraken_exchange import KrakenExchange
from exchanges.deribit_exchange import DeribitExchange
from exchanges.lighter_exchange import LighterExchange, LighterRhExchange
from exchanges._session import make_session
from services.arbitrage_detector import detect_arbitrage, _normalize_symbol, _alias_map, _exchange_alias_map, _strip_1000x
from services.leverage_cache import get_max_notional, refresh_leverage_cache, fetch_missing_notionals, get_coinw_position_info
from models import ScanResult

logger = logging.getLogger(__name__)

CEX_IDS = [
    # 全 8 家已脫離 ccxt → 對應 *_exchange.py (WS-first / REST bulk)
    # 保留空列表是為了避免 CcxtExchange 被建立。需要 ccxt 的話再加回來。
]


CACHE_PATH = Path(__file__).resolve().parent.parent.parent / "last_scan.json"
RATE_HISTORY_PATH = Path(__file__).resolve().parent.parent.parent / "logs" / "rate_history_24h.json"
INDEX_HISTORY_PATH = Path(__file__).resolve().parent.parent.parent / "logs" / "index_history.json"  # 舊單檔（僅供一次性移置）
INDEX_HISTORY_DIR = Path(__file__).resolve().parent.parent.parent / "logs" / "index_history"  # 新：按日 JSONL 目錄
INDEX_DEV_WINDOW = 864          # RAM 每 (幣,所) 保留的 dev 點數（3 天 @ 5 分鐘）
INDEX_HISTORY_RETAIN_DAYS = 3   # 磁碟按日 JSONL 保留天數
# 永遠不要用 ccxt 建這些所的 OHLCV 實例（不是效能取捨，是會漏 session）。
#
# ccxt 4.5.32 的 hyperliquid.load_markets() 100% 失敗：
#   async_support/hyperliquid.py:526 用 asyncio.gather 併發跑 fetch_swap/spot/hip3_markets，
#   :854 `mappedSymbol = mappedBase + '/' + mappedQuote` 撞上 mappedBase=None
#   → TypeError（ccxt 自身的 bug，與網路、API key 無關）。
# 致命的是 gather 不帶 return_exceptions 時【不會取消手足 task】：例外往外拋後
# fetch_swap_markets 仍在背景飛。我們的 except 正確執行了 asyncio.shield(ex.close())、
# session 也確實被關掉設成 None——但幾百毫秒後那些手足 task 醒來，
# base/exchange.py:208 fetch() 看到 `self.session is None` 就呼叫 open()
# 【重建一個全新的 ClientSession + TCPConnector】，而此時實例早已被丟棄，沒人會關它。
# → GC 時噴 "requires to release all resources" + Unclosed client session/connector。
#
# 實測（2026-08-11）：後端連續執行 70 小時，每小時穩定漏 1 個（節奏來自 3600 秒負向快取），
# 共 64 個。2026-08-09 那次把 except Exception 改成 except BaseException 沒有用——
# 洩漏發生在 close() 之【後】，不是 close 沒被呼叫。
# 所以正解是「不讓這個實例誕生」：hyperliquid / tradexyz 本來就有 _fetch_ohlcv_direct
# 走官方 candleSnapshot 的路徑，根本不需要 ccxt。
# lighter / lighter_rh：ccxt 4.5.32 根本沒有這間所（'lighter' in ccxt.exchanges → False），
#   _get_ohlcv_ccxt 的 getattr 本來就會回 None，明列進來只是讓意圖清楚、順便跳過負向快取。
_CCXT_OHLCV_BLOCKLIST = {"hyperliquid", "tradexyz", "lighter", "lighter_rh"}

# 本專案的交易所 id → 取 K 線時要用的 ccxt id（兩者不同名時才列）。
# kraken：我們的 KrakenExchange 抓的是【Kraken Futures】的永續（PF_XXXUSD），
#   但 ccxt 的 "kraken" 是【現貨】，markets 裡沒有 BTC/USD:USD 這種永續代號
#   → 每個變體都 `sym not in ccxt_ex.markets` 而被跳過 → 價差圖永遠「無價差歷史資料」。
#   正確的是 ccxt "krakenfutures"，實測 280 個 swap，我們的 BTC/USD:USD 與 KAITO/USD:USD
#   都直接命中、fetch_ohlcv 回得了 K 線。
_CCXT_OHLCV_ID = {"kraken": "krakenfutures"}


def _purge_delisted(history: list[dict], records) -> list[dict]:
    """清掉「已下架合約」的歷史，但【只針對本輪真的有回資料的交易所】。

    ⚠ 原本是無條件比對 valid_pairs（只由本輪掃描結果構成），造成一個致命後果：
      某所本輪回 0 筆（WS 斷線 / 429 / 逾時 / 交易所維護）→ 它的 (ex, sym) 一個都不在
      valid_pairs → 該所【整整 72 小時的歷史被當成「全部下架」刪光】，
      而且要再跑滿 72 小時才長得回來。

    日誌鐵證（logs/scanner.log，11 次「取得 0 筆費率」全部在 90 秒內緊接大量清理）：
      08-07 11:19:48 [coinw] 回 0 筆 → 11:20:24 清理 11,484 筆
      08-07 15:21:07 [coinw] 回 0 筆 → 15:21:31 清理 21,057 筆
      08-08 19:22:32 [coinw]/[okx] 回 0 筆 → 19:23:12 清理 7,072 筆
      08-09 00:29:33 [coinw]/[okx] 回 0 筆 → 00:30:16 清理 6,427 筆
    這就是 CoinW 平均每幣只有 3.9 個結算點的真正原因（它回 0 筆的次數最多），
    與它自己的 fetch_funding_history 無關。

    正確語意：「這一輪掃到了這個交易所、但沒掃到這個合約」才叫下架。
    掃描結果裡完全沒有這個交易所 → 沒有任何資訊，應保留原樣。
    """
    live_exchanges = {r.exchange for r in records}
    valid_pairs = {(r.exchange, r.normalized_symbol or r.symbol) for r in records}
    return [e for e in history
            if e["ex"] not in live_exchanges or (e["ex"], e["sym"]) in valid_pairs]


CONSTITUENT_SNAPSHOT_PATH = Path(__file__).resolve().parent.parent.parent / "logs" / "constituent_snapshot.json"
CONSTITUENT_CHANGES_PATH = Path(__file__).resolve().parent.parent.parent / "logs" / "constituent_changes.json"
INDEX_WATCH_PATH = Path(__file__).resolve().parent.parent.parent / "logs" / "index_watch.json"

# 指數成分「持續監控」設定（沿用 LAB 監控規則）
INDEX_WATCH_INTERVAL = 5                       # 輪詢秒數
INDEX_WATCH_TARGET = "bitget"                  # 只通知與此交易所相關的變動
INDEX_WATCH_SOURCES = ("binance", "bybit", "okx", "kucoinfutures", "gateio", "bitget", "mexc")
INDEX_WATCH_SOURCE_THRESHOLD = {"bybit": 0.05}  # Bybit 指數權重變動 <5% 不通知（更新太頻繁）
INDEX_WATCH_ROUND = 6                          # 權重比對精度（僅濾浮點雜訊）
# 持續監控通知的目標 TG 頻道。設定方式：環境變數 TG_CHAT_ID / TG_TOPIC_ID。
# 群組 id 是負數且帶 -100 前綴（例：-1001234567890）；topic id 是討論串編號。
# 沒設就不發通知，掃描功能不受影響。
INDEX_WATCH_TG_CHAT_ID = int(os.environ.get("TG_CHAT_ID") or 0)
INDEX_WATCH_TG_TOPIC_ID = int(os.environ.get("TG_TOPIC_ID") or 0)
# 通知走一支本機 relay 服務；沒有的話設成自己的 webhook 即可。
TG_SEND_URL = os.environ.get("TG_SEND_URL", "")  # 設成自己的 TG 推送 webhook；沒設就不發通知

# DeepCoin 套利機會：BN/BY 滿資費（頂到 cap）→ 縮週期到 1h，但 DeepCoin 還停在 4h/8h。
# 事件驅動：60s 追蹤迴圈，滿資費→追蹤翻 1h→查 DC 是否跟上。
DC_REF_FUNDING_GATE = 0.0025   # 1h 幣「仍算高資費」的每期門檻 0.25%（含負向 <=-0.25%）
DC_SHORTENED_INTERVAL = 1      # BN/BY 結算週期 <= 1h 視為「已縮短」
DC_LAG_MIN_INTERVAL = 4        # DeepCoin 週期 >= 4h 視為「沒跟上（還在 4h/8h）」
DC_WATCH_INTERVAL = 60         # 追蹤輪詢秒數（獨立於主掃描 5 分鐘）
DC_WATCH_EXPIRE_S = 3 * 3600   # watch list 幣種過期（滿資費事件消退後清掉）
DC_CAP_TOLERANCE = 0.9         # 4h/8h 幣 |rate| >= cap*此值 視為「頂到滿資費」

# 尺度對齊：以下交易所用「原生尺度」（如 PEPE 而非 1000PEPE），需依同幣基準價乘倍數對齊
# lighter 實測【不在此列】：它跟 Binance 一樣用 1000PEPE/1000SHIB/1000BONK/1000FLOKI 命名，
#   mark_price 也對得上（1000PEPE 0.002659 vs Binance 0.00266009）。加進來會被亂乘 1000 → 假價差。
RAW_SCALE_EXCHANGES = {"kraken", "deribit"}
VALID_SCALE_FACTORS = {1000, 10000, 100000, 1000000}  # 合約倍數只會是 10 的次方（且 >=1000）


## 鏈名正規化映射（統一不同交易所對同一鏈的命名）
NETWORK_ALIASES = {
    "BEP20": "BSC", "BEP20(BSC)": "BSC", "BSC(BEP20)": "BSC", "BNB Smart Chain(BEP20)": "BSC",
    "ERC20": "ETH", "Ethereum(ERC20)": "ETH", "ETH(ERC20)": "ETH",
    "TRC20": "TRX", "TRON(TRC20)": "TRX", "TRX(TRC20)": "TRX",
    "SPL": "SOL", "Solana(SPL)": "SOL", "SOL(SPL)": "SOL",
    "Polygon": "MATIC", "MATIC": "MATIC", "POLYGON": "MATIC",
    "AVAXC": "AVAX-C", "AVAX C-Chain": "AVAX-C", "Avalanche C-Chain": "AVAX-C",
    "ARB": "ARBITRUM", "Arbitrum One": "ARBITRUM", "ARBITRUMONE": "ARBITRUM",
    "ARBONE": "ARBITRUM", "Arbitrum": "ARBITRUM",
    "OP": "OPTIMISM", "Optimism": "OPTIMISM", "OPTIMISM": "OPTIMISM",
    "BASE": "BASE", "Base": "BASE",
    "BTC": "BTC", "Bitcoin": "BTC",
    "TON": "TON", "Toncoin": "TON",
    "ALGO": "ALGO", "Algorand": "ALGO",
    "NEAR": "NEAR",
    "SUI": "SUI",
    "APT": "APT", "Aptos": "APT",
    "SEI": "SEI",
}

# 現貨搬磚：支援的交易所（排除純合約所）
SPOT_EXCHANGE_IDS = ["binance", "bybit", "okx", "bitget", "mexc", "gateio", "bingx"]

SPOT_EXCHANGE_LABELS = {
    "binance": "Binance", "bybit": "Bybit", "okx": "OKX", "bitget": "Bitget",
    "mexc": "MEXC", "gateio": "Gate.io", "bingx": "BingX",
    "binance_alpha": "Binance Alpha",
}

# Binance Alpha chainId → 正規化鏈名
ALPHA_CHAIN_MAP = {
    1: "ETH", 56: "BSC", 137: "MATIC", 42161: "ARBITRUM", 10: "OPTIMISM",
    8453: "BASE", 43114: "AVAX-C", 324: "ZKSYNC", 59144: "LINEA",
    534352: "SCROLL", 5000: "MANTLE", 169: "MANTA", 81457: "BLAST",
    1101: "POLYGON-ZKEVM",
}


def _normalize_network(name: str) -> str:
    """將交易所鏈名正規化"""
    if not name:
        return name
    return NETWORK_ALIASES.get(name, name.upper())


_DELISTING_OVERRIDES_PATH = Path(__file__).resolve().parent.parent / "delisting_overrides.json"


def _apply_delisting_overrides(records: list) -> None:
    """套用 delisting_overrides.json 的手動下架標記"""
    try:
        if not _DELISTING_OVERRIDES_PATH.exists():
            return
        with open(_DELISTING_OVERRIDES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        overrides = data.get("overrides", [])
        if not overrides:
            return
        today = datetime.now(timezone.utc).date()
        # 建立 (exchange, base_symbol) lookup set（只比對 base 幣種）
        active_set: set[tuple[str, str]] = set()
        for ov in overrides:
            date_str = ov.get("date", "")
            if date_str:
                try:
                    ov_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    if today > ov_date:
                        continue  # 已過下架日期，不再標記
                except ValueError:
                    pass
            ex = ov.get("exchange", "").lower()
            sym = ov.get("symbol", "")
            base = sym.split("/")[0].upper() if sym else ""
            if ex and base:
                active_set.add((ex, base))
        if not active_set:
            return
        count = 0
        for r in records:
            base = (r.normalized_symbol or r.symbol).split("/")[0].upper()
            if (r.exchange, base) in active_set:
                r.is_delisting = True
                count += 1
        if count:
            logger.info(f"手動下架標記套用：{count} 筆記錄")
    except Exception as e:
        logger.warning(f"載入 delisting_overrides.json 失敗: {e}")


BN_EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
_BN_DELISTING_CACHE_TTL = 3600  # 每小時更新一次

# 碎肉流「剛結算」窗：每輪掃描只對「最近一次結算落在此窗內」的合約抓實際結算費率
# （覆蓋 _append_scan_to_history 寫入的掃描 placeholder）。結算費率產生後不變，窗外者
# 早已抓過、重抓只會被 _merge_entries 去重丟掉。45 分鐘涵蓋各所「結算後最多 ~30 分鐘
# 才有資料」的延遲 + 重試餘裕。無法判斷結算時間的合約不套此窗、一律照抓（見 _needs_settled_fetch）。
_MEAT_SETTLE_LOOKBACK_MINUTES = 45


async def _fetch_bn_delistings_from_api() -> dict[str, int]:
    """從 Binance 抓取即將下架的永續合約（deliveryDate 在 30 天內）
    回傳 {base_symbol: delivery_ts_ms}，e.g. {"HIPPO": 1775638800000}
    """
    try:
        now_ms = time.time() * 1000
        cutoff_ms = now_ms + 30 * 86400 * 1000  # 30 天後
        result: dict[str, int] = {}
        async with make_session(15) as session:
            async with session.get(BN_EXCHANGE_INFO_URL) as resp:
                data = await resp.json()
        for s in data.get("symbols", []):
            if s.get("contractType") != "PERPETUAL":
                continue
            delivery_ms = s.get("deliveryDate", 0)
            if delivery_ms and delivery_ms < cutoff_ms:
                base = s.get("baseAsset", "")
                if base:
                    result[base.upper()] = delivery_ms
        logger.info(f"BN 下架偵測：找到 {len(result)} 個即將下架合約")
        return result
    except Exception as e:
        logger.warning(f"BN 下架偵測失敗: {e}")
        return {}


class FundingScanner:
    def __init__(self, scan_interval: int = 30, min_annual_diff: float = 10.0,
                 bybit_api: dict = None, okx_api: dict = None, bingx_api: dict = None, mexc_api: dict = None,
                 binance_api: dict = None, aster_api: dict = None, bitget_api: dict = None,
                 gateio_api: dict = None, bybit_vip_level: str = "No VIP",
                 crossex_api: dict = None):
        self.scan_interval = scan_interval
        self.min_annual_diff = min_annual_diff
        self.last_result: ScanResult | None = None
        self.bybit_spot_futures: list[dict] = []  # Bybit 期現快取
        self.bitget_spot_futures: list[dict] = []  # Bitget 期現快取
        self.okx_spot_futures: list[dict] = []  # OKX 期現快取
        self.binance_spot_futures: list[dict] = []  # Binance 期現快取
        self.gateio_spot_futures: list[dict] = []  # Gate 期現快取
        self.mexc_spot_futures: list[dict] = []  # MEXC 期現快取（僅公開欄位）
        self.bingx_spot_futures: list[dict] = []  # BingX 期現快取（僅公開欄位）
        self.meat_flow: list[dict] = []  # 碎肉流快取（全交易所配對）
        self.meat_flow_coinw: list[dict] = []  # 碎肉流快取（CoinW 為一側）
        self.meat_flow_same_interval: list[dict] = []  # 碎肉流快取（同結算週期）
        self._rate_history: list[dict] = []  # 費率歷史累積（保留 72h）
        self._meat_bootstrapping = False  # bootstrap 進行中標記
        self._meat_updating = False  # 防止並發 _update_meat_flow
        self.index_tracking: list[dict] = []  # 指數追蹤：當前異常列表
        self.index_all_deviations: list[dict] = []  # 指數追蹤：所有偏離（含非異常）
        # 指數偏離歷史：RAM 只留每 (幣,所) 最近 INDEX_DEV_WINDOW 個 dev（算基線/趨勢用）；
        # 完整明細（含 market_price/ex_price，供單幣歷史 API）只寫按日 JSONL、不進 RAM。
        self._index_devs: dict[tuple[str, str], array] = {}  # (幣,所)→float32 陣列（最近 window 個 dev，省 RAM）
        self._index_history_ready = False  # 背景載入(重建 _index_devs)完成前為 False：暫不寫 JSONL
        self._constituent_snapshot: dict = {}  # 指數成分快照 {symbol: {exchange: [{constituent_exchange, weight, price}]}}
        self._constituent_changes: list[dict] = []  # 成分變更歷史（保留 7 天）
        self._index_watch: set[str] = set()  # 持續監控中的幣種（normalized symbol）
        self._index_watch_state: dict[str, dict] = {}  # {symbol: {source: {ex: weight}}} 上次通知基準
        self._index_watch_wallet: dict[str, dict] = {}  # {symbol: {exchange: {chain: {deposit, withdraw}}}} 充提基準
        self._index_watch_task: asyncio.Task | None = None
        self.spot_arbitrage: list[dict] = []  # 現貨搬磚快取
        self._spot_networks: dict[str, dict] = {}  # {exchange: {currency: [{network, deposit, withdraw}]}}
        self._spot_networks_ts: float = 0  # 上次更新時間
        self._spot_exchanges: dict[str, object] = {}  # exchange_id -> ccxt spot instance
        # 重查 OHLCV 專用的常駐 ccxt 快取：建一次、load_markets 一次、重複使用，只在關機關閉。
        # 徹底消除「每次結構性價差重查現建現關 ccxt」→ 不再有逾時取消漏 session（洩漏根治）。
        self._ohlcv_ccxt: dict[str, object] = {}      # exchange_name -> 常駐 ccxt async 實例
        self._ohlcv_ccxt_lock = asyncio.Lock()         # 建立時鎖，避免並發重複建
        self._ohlcv_ccxt_failed: dict[str, float] = {}  # 負向快取 name -> 上次失敗 monotonic：
        #   ccxt 不相容的所（如 hyperliquid）load_markets 每次都失敗，不記住就每輪重建洩漏 session。
        self._alpha_tokens: dict = {}  # {symbol: {price, contractAddress, chainId, tokenId, alphaId}}
        self._alpha_tokens_ts: float = 0  # 上次更新時間
        self._spot_scan_duration_ms: int = 0
        self._spot_scan_total_coins: int = 0
        self.is_running = False
        self._task: asyncio.Task | None = None
        self._spot_task: asyncio.Task | None = None
        self._exchanges: list[BaseExchange] = []
        # 滿資費通知去重（同幣種每輪結算只通知一次）
        self._max_funding_notified: dict[str, str] = {}  # {symbol: funding_time_iso}
        # DeepCoin 週期落後：滿資費 watch list {symbol: {"since": ts, "last": ts}} + 已通知去重
        self._dc_watch: dict[str, dict] = {}
        self._dc_lag_notified: set[str] = set()
        # RWA 跨所價差通知去重：{(base, 做多所, 做空所)}
        self._rwa_spread_notified: set[tuple] = set()
        # RWA 兩張表的快取（每小時由 _rwa_compute_loop 更新）
        self.rwa_arb_cache: list = []
        self.rwa_spread_cache: list = []
        self.rwa_leveraged_cache: list = []
        self.rwa_lev_spot_cache: list = []          # Gate 現貨槓桿代幣（只列可買進的負偏離）
        self.rwa_computed_at: str | None = None
        # BN 自動下架偵測快取：{base_symbol: delivery_ts_ms}
        self._bn_delistings: dict[str, int] = {}
        self._bn_delistings_ts: float = 0  # 上次更新時間
        self._bybit_api_key = bybit_api.get("apiKey", "") if bybit_api else ""
        self._bybit_api_secret = bybit_api.get("secret", "") if bybit_api else ""
        self._bybit_vip_level = bybit_vip_level
        self._bitget_api_key = bitget_api.get("apiKey", "") if bitget_api else ""
        self._bitget_api_secret = bitget_api.get("secret", "") if bitget_api else ""
        self._bitget_passphrase = bitget_api.get("passphrase", "") if bitget_api else ""
        self._okx_api = okx_api or {}
        self._bingx_api = bingx_api or {}
        self._mexc_api = mexc_api or {}
        self._binance_api = binance_api or {}
        self._aster_api = aster_api or {}
        self._gateio_api = gateio_api or {}
        self._crossex_api = crossex_api or {}   # RWA 標的清單用（唯讀查 CrossEx 可交易標的）

    def _load_cache(self):
        """從檔案載入上次掃描結果"""
        try:
            if not CACHE_PATH.exists():
                return
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.last_result = ScanResult.model_validate(data)
            logger.info(
                f"載入快取：{len(self.last_result.records)} 筆費率，"
                f"時間 {self.last_result.timestamp.isoformat()}"
            )
        except Exception as e:
            logger.warning(f"載入快取失敗（將等待新掃描）: {e}")
            # 刪除損壞的快取
            try:
                CACHE_PATH.unlink(missing_ok=True)
            except Exception:
                pass

    def _save_cache(self):
        """將掃描結果存入檔案"""
        if not self.last_result:
            return
        try:
            with open(CACHE_PATH, "w", encoding="utf-8") as f:
                json.dump(self.last_result.model_dump(mode="json"), f)
        except Exception as e:
            logger.warning(f"儲存快取失敗: {e}")

    def _init_exchanges(self):
        """初始化所有交易所實例"""
        self._exchanges = []
        # 全 8 家走 WS-first / REST-bulk 專屬實作（脫離 ccxt）
        self._exchanges.append(BinanceExchange())
        self._exchanges.append(BybitExchange())
        self._exchanges.append(OkxExchange())
        self._exchanges.append(BitgetExchange())
        self._exchanges.append(MexcExchange())
        self._exchanges.append(GateioExchange())
        self._exchanges.append(BingxExchange())
        self._exchanges.append(KucoinExchange())
        for eid in CEX_IDS:
            self._exchanges.append(CcxtExchange(eid))
        self._exchanges.append(AsterExchange(
            api_key=self._aster_api.get("apiKey", ""),
            secret=self._aster_api.get("secret", ""),
        ))
        self._exchanges.append(CoinwExchange())
        self._exchanges.append(HyperliquidExchange())
        self._exchanges.append(TradeXyzExchange())
        self._exchanges.append(OurbitExchange())
        # BitMart 已停止營運（交易所關閉），不再掃描
        self._exchanges.append(DeepCoinExchange())
        self._exchanges.append(LbankExchange())
        self._exchanges.append(KrakenExchange())
        self._exchanges.append(DeribitExchange())
        self._exchanges.append(LighterExchange())
        self._exchanges.append(LighterRhExchange())
        logger.info(f"已初始化 {len(self._exchanges)} 個交易所")

    async def start(self):
        """啟動背景掃描"""
        self._load_cache()
        self._load_rate_history()
        # 指數偏離歷史可達 1GB/數百萬筆，同步載入會阻塞啟動約 10 秒。
        # 改背景非同步載入（檔案讀取在 thread 內），讓 API 與掃描立即啟動；
        # 載入完成前指數追蹤基線資料較少，完成後自動補齊。
        asyncio.create_task(self._load_index_history_async())
        self._load_constituent_snapshot()
        self._load_constituent_changes()
        self._load_index_watch()
        self._init_exchanges()
        self.is_running = True
        # 還原持續監控（5011 重啟後自動接續）
        self._ensure_index_watch_task()
        # 如果有快取，立即計算指數追蹤 + Bybit 期現 + 碎肉流
        if self.last_result:
            self._compute_index_tracking()
            asyncio.create_task(self._update_bybit_spot_futures())
            asyncio.create_task(self._update_bitget_spot_futures())
            # 其他五所（依賴 _spot_exchanges）改在 _spot_init_and_loop 現貨就緒後才觸發
            asyncio.create_task(self._bootstrap_and_compute_meat())
            # 啟動時立即修正快取中的 BN 下架標記（快取可能是舊版本沒有此標記）
            asyncio.create_task(self._apply_bn_delistings_to_cache())
        self._task = asyncio.create_task(self._scan_loop())
        # 現貨交易所初始化 + 掃描迴圈放背景，不阻塞 API 服務
        self._spot_task = asyncio.create_task(self._spot_init_and_loop())
        # DeepCoin 週期落後：60s 事件驅動追蹤（滿資費→翻 1h→查 DC）
        asyncio.create_task(self._dc_lag_watch_loop())
        # RWA 期現標的清單（CrossEx 可交易 ∩ 非加密貨幣），每日背景刷新
        asyncio.create_task(self._rwa_universe_loop())
        # RWA 兩張表每小時計算一次並快取（含跨所價差 TG 通知）
        asyncio.create_task(self._rwa_compute_loop())
        # 背景抓取倉位限制快取
        asyncio.create_task(self._refresh_leverage_tiers())
        logger.info("FundingScanner 已啟動")

    async def _apply_bn_delistings_to_cache(self):
        """啟動時將 BN 下架標記補入快取 last_result（修正舊快取沒有此標記的問題）"""
        try:
            delistings = await _fetch_bn_delistings_from_api()
            if not delistings or not self.last_result:
                return
            self._bn_delistings = delistings
            self._bn_delistings_ts = time.time()
            count = 0
            for r in self.last_result.records:
                if r.exchange != "binance":
                    continue
                base = (r.normalized_symbol or r.symbol).split("/")[0].upper()
                delivery_ms = delistings.get(base)
                if delivery_ms:
                    r.is_delisting = True
                    r.delisting_time = datetime.fromtimestamp(delivery_ms / 1000, tz=timezone.utc)
                    count += 1
            if count:
                logger.info(f"啟動修正：{count} 筆 BN 快取記錄補標 is_delisting")
        except Exception as e:
            logger.warning(f"啟動 BN 下架補標失敗: {e}")

    async def _spot_init_and_loop(self):
        """背景初始化現貨交易所後啟動掃描迴圈"""
        await self._init_spot_exchanges()
        # 現貨實例就緒後才補跑其他所期現（OKX/Binance/Gate/MEXC/BingX 依賴 _spot_exchanges）；
        # 啟動時若在此之前觸發會因現貨未載入而首輪空白。
        if self.last_result:
            asyncio.create_task(self._update_all_other_spot_futures())
        await self._spot_scan_loop()

    async def stop(self):
        """停止掃描並關閉連線"""
        self.is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._spot_task:
            self._spot_task.cancel()
            try:
                await self._spot_task
            except asyncio.CancelledError:
                pass
        # 關閉所有交易所連線
        for ex in self._exchanges:
            try:
                await ex.close()
            except Exception as e:
                logger.warning(f"關閉 {ex.name} 失敗: {e}")
        # 關閉現貨交易所連線
        for ex in self._spot_exchanges.values():
            try:
                await ex.close()
            except Exception as e:
                logger.warning(f"關閉現貨交易所失敗: {e}")
        # 關閉重查 OHLCV 常駐 ccxt 快取（先 list 快照：背景 shielded 建立 task 可能在
        # 迭代中完成並插入字典，避免 dictionary changed size during iteration）
        for ex in list(self._ohlcv_ccxt.values()):
            try:
                await ex.close()
            except Exception as e:
                logger.warning(f"關閉 OHLCV ccxt 失敗: {e}")
        self._ohlcv_ccxt.clear()
        logger.info("FundingScanner 已停止")

    async def _scan_loop(self):
        """背景掃描迴圈：每 5 分鐘一次（整點分鐘對齊 :00/:05/:10/.../:55）"""
        _SCAN_MINUTES = list(range(0, 60, 5))
        # 啟動時先掃一次
        await self.scan_once()
        while self.is_running:
            now = datetime.now(timezone.utc)
            # 找出下一個最近的掃描時間點
            target = None
            for m in _SCAN_MINUTES:
                candidate = now.replace(minute=m, second=0, microsecond=0)
                if candidate > now:
                    target = candidate
                    break
            if target is None:
                # 今小時所有時間點都已過，取明小時的第一個
                target = (now + timedelta(hours=1)).replace(minute=_SCAN_MINUTES[0], second=0, microsecond=0)
            wait = (target - now).total_seconds()
            logger.info(f"下次掃描：{target.strftime('%H:%M')} UTC（{wait:.0f}s 後）")
            await asyncio.sleep(wait)
            if self.is_running:
                await self.scan_once()

    async def scan_once(self) -> ScanResult:
        """執行一次完整掃描"""
        start_time = time.time()
        all_records = []
        errors = {}

        # 並行查詢所有交易所
        tasks = []
        for ex in self._exchanges:
            tasks.append(self._fetch_with_name(ex))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for ex, result in zip(self._exchanges, results):
            if isinstance(result, Exception):
                errors[ex.name] = str(result)
                logger.warning(f"[{ex.name}] 掃描失敗: {result}")
            else:
                all_records.extend(result)

        # 填入正規化 symbol（別名合併用，考慮交易所專屬別名）
        for r in all_records:
            r.normalized_symbol = _normalize_symbol(r.symbol, r.exchange)

        # 尺度對齊：Kraken/Deribit 用原生尺度（如 PEPE），別家用 1000PEPE。
        # 依同幣組別的基準價把它們的價格乘上倍數對齊，而非被價格污染防護排除。
        self._align_raw_scale_prices(all_records)

        # 套用手動下架標記（delisting_overrides.json）
        _apply_delisting_overrides(all_records)

        # 自動 BN 下架偵測（每小時更新一次快取）
        now_ts = time.time()
        if now_ts - self._bn_delistings_ts > _BN_DELISTING_CACHE_TTL:
            self._bn_delistings = await _fetch_bn_delistings_from_api()
            self._bn_delistings_ts = now_ts
        if self._bn_delistings:
            for r in all_records:
                if r.exchange != "binance":
                    continue
                base = (r.normalized_symbol or r.symbol).split("/")[0].upper()
                delivery_ms = self._bn_delistings.get(base)
                if delivery_ms:
                    r.is_delisting = True
                    r.delisting_time = datetime.fromtimestamp(delivery_ms / 1000, tz=timezone.utc)

        # 套利偵測
        arbitrage = detect_arbitrage(all_records)

        # 附加倉位限制（先用快取，再按需查缺失的）
        missing_by_ex: dict[str, list[str]] = {}
        for opp in arbitrage:
            opp.long_max_notional = get_max_notional(opp.long_exchange, opp.long_symbol)
            opp.short_max_notional = get_max_notional(opp.short_exchange, opp.short_symbol)
            if opp.long_max_notional is None:
                missing_by_ex.setdefault(opp.long_exchange, []).append(opp.long_symbol)
            if opp.short_max_notional is None:
                missing_by_ex.setdefault(opp.short_exchange, []).append(opp.short_symbol)

        if missing_by_ex:
            try:
                await fetch_missing_notionals(missing_by_ex, self._leverage_api_keys())
                # 重新填入
                for opp in arbitrage:
                    if opp.long_max_notional is None:
                        opp.long_max_notional = get_max_notional(opp.long_exchange, opp.long_symbol)
                    if opp.short_max_notional is None:
                        opp.short_max_notional = get_max_notional(opp.short_exchange, opp.short_symbol)
            except Exception as e:
                logger.warning(f"按需查詢倉位限制失敗: {e}")

        # 計算預期收益
        for opp in arbitrage:
            limit = min(opp.long_max_notional or 0, opp.short_max_notional or 0)
            capped_limit = min(limit, 50000)
            opp.expected_profit = round(opp.estimated_profit * capped_limit / 100, 2)

        # 過濾結構性價差：價差主導的機會需要歷史收斂證據
        arbitrage = await self._filter_structural_spread(arbitrage)

        duration_ms = (time.time() - start_time) * 1000

        self.last_result = ScanResult(
            timestamp=datetime.now(timezone.utc),
            records=all_records,
            arbitrage=arbitrage,
            errors=errors,
            scan_duration_ms=round(duration_ms, 1),
        )

        self._save_cache()

        logger.info(
            f"掃描完成：{len(all_records)} 筆費率，"
            f"{len(arbitrage)} 個套利機會，"
            f"{len(errors)} 個錯誤，"
            f"耗時 {duration_ms:.0f}ms"
        )

        # 掃描完成後更新各所期現快取 + 碎肉流
        asyncio.create_task(self._update_bybit_spot_futures())
        asyncio.create_task(self._update_bitget_spot_futures())
        asyncio.create_task(self._update_all_other_spot_futures())
        # 先把當期掃描費率直接寫入 _rate_history（同步、快速；給 binance/bybit 等
        # fetch_funding_history 偶發失敗的所留 placeholder，再由 _fetch_settled_and_update_meat
        # 用實際結算值覆蓋）
        self._append_scan_to_history(all_records)
        # 掃描後從各交易所抓最新結算費率（非預測值），更新碎肉流快取
        asyncio.create_task(self._fetch_settled_and_update_meat(all_records))
        # 指數追蹤：偵測交易所指數價格偏離
        self._compute_index_tracking()
        # 在啟動 async task 前先快照，避免 task 完成時 index_tracking 已被下一次掃描覆蓋
        index_tracking_snapshot = list(self.index_tracking)
        index_all_deviations_snapshot = list(self.index_all_deviations)
        # 指數成分追蹤：偵測成分交易所/權重變化
        asyncio.create_task(self._update_constituents(index_tracking_snapshot, index_all_deviations_snapshot))
        # 滿資費通知：BN 費率到頂 + 即將結算 → TG 通知
        asyncio.create_task(self._check_max_funding_after_scan(all_records))
        # DeepCoin 週期落後改由獨立 60s 追蹤迴圈 _dc_lag_watch_loop 處理（不在此觸發）
        # RWA 兩張表改由 _rwa_compute_loop 每小時算一次（含 TG），不在每輪掃描觸發

        return self.last_result

    def _leverage_api_keys(self) -> dict:
        """取得 leverage cache 可用的 API keys"""
        keys = {}
        if self._bingx_api:
            keys["bingx"] = self._bingx_api
        if self._okx_api:
            keys["okx"] = self._okx_api
        if self._binance_api:
            keys["binance"] = self._binance_api
        if self._aster_api:
            keys["aster"] = self._aster_api
        return keys

    async def _filter_structural_spread(self, arbitrage: list) -> list:
        """過濾結構性價差：利潤主要來自價差的機會，需要歷史收斂證據。
        查 12h 歷史價差，如果價差一直穩定（標準差小），代表不會收斂，過濾掉。
        """
        if not arbitrage:
            return arbitrage

        # 費率差 >= 0.5%：有足夠費率收入，不管價差 → 直接保留
        # 費率差 < 0.1% 且價差 > 0.5%：純靠價差 → 查歷史收斂 → 結構性就過濾
        # 中間地帶（0.1~0.5%）：保留（有一定費率收入）
        suspects = []
        clean = []
        for opp in arbitrage:
            rate_diff_pct = abs(opp.rate_diff) * 100
            if rate_diff_pct < 0.1 and opp.spread_pct < -0.5:
                suspects.append(opp)
            else:
                clean.append(opp)

        if not suspects:
            return arbitrage

        # P0-2 防棘輪：正常 suspects 約 20~30 個；資料短暫失真時會暴增到上千，
        # 每個都現建 ccxt 查歷史 → 記憶體棘輪 + 掃描卡死。設硬上限：
        # 只重查「宣稱利潤最高」的前 N 個（最需要驗證），其餘暫時保留不過濾（不誤殺）。
        RECHECK_CAP = 60
        if len(suspects) > RECHECK_CAP:
            suspects.sort(key=lambda o: o.estimated_profit, reverse=True)
            logger.warning(
                f"[套利] 價差主導機會暴增到 {len(suspects)} 個（異常），"
                f"只重查前 {RECHECK_CAP} 個，其餘暫時保留"
            )
            clean.extend(suspects[RECHECK_CAP:])
            suspects = suspects[:RECHECK_CAP]

        logger.info(f"[套利] 檢查 {len(suspects)} 個價差主導的機會是否為結構性價差")

        async def _check_one(opp):
            """查歷史價差，判斷是否結構性"""
            try:
                # 做空所 / 做多所 的歷史溢價（跟套利方向一致）
                history = await self.fetch_price_premium(
                    opp.long_symbol, opp.short_exchange, opp.long_exchange, days=1
                )
                if len(history) < 10:
                    return opp  # 資料不足，保留

                premiums = [h["premium"] for h in history]
                avg = sum(premiums) / len(premiums)
                p_min, p_max = min(premiums), max(premiums)
                p_range = p_max - p_min

                # 判斷：價差波動幅度
                # 如果歷史波動很小（range < 2%），代表價差幾乎不動 → 結構性
                if p_range < 2.0:
                    logger.info(
                        f"[套利] {opp.symbol} L={opp.long_exchange} S={opp.short_exchange} "
                        f"結構性價差（avg={avg:.2f}% range={p_range:.2f}% 波動過小），已過濾"
                    )
                    return None  # 過濾

                # 用歷史最低價差修正預計利潤
                # premium 正值 = 做空所更貴（有利），歷史最低 = 價差最窄時
                # 實際可收的價差利潤 = 當前價差 - 歷史最低價差（cap 到 0）
                hist_min = max(p_min, 0)  # 最小到 0
                current_premium = abs(opp.spread_pct)
                realistic_spread_profit = current_premium - hist_min
                rate_diff_pct = abs(opp.rate_diff) * 100
                opp.estimated_profit = round(rate_diff_pct + realistic_spread_profit, 4)
                opp.spread_pct = round(-realistic_spread_profit, 4)  # 維持負值慣例
                # 重算預期收益
                limit = min(opp.long_max_notional or 0, opp.short_max_notional or 0)
                capped_limit = min(limit, 50000)
                opp.expected_profit = round(opp.estimated_profit * capped_limit / 100, 2)
                logger.info(
                    f"[套利] {opp.symbol} 價差修正: 當前{current_premium:.2f}% "
                    f"歷史最低{hist_min:.2f}% → 實際{realistic_spread_profit:.2f}%"
                )
                return opp
            except Exception as e:
                logger.debug(f"[套利] 價差檢查失敗 {opp.symbol}: {e}")
                return opp  # 檢查失敗，保留

        # 並行檢查：semaphore 限併發（P0-2 防棘輪 + 洩漏放大）。
        # 每個 _check_one 會現建 ccxt 實例查歷史；不限併發時 suspects 一多就一次
        # 現建上千個實例 → 峰值記憶體暴衝。限 6 併發 → 峰值實例數固定在十幾個。
        _recheck_sem = asyncio.Semaphore(6)

        async def _check_bounded(opp):
            async with _recheck_sem:
                return await asyncio.wait_for(_check_one(opp), timeout=15)

        results = await asyncio.gather(
            *[_check_bounded(opp) for opp in suspects], return_exceptions=True
        )

        for i, r in enumerate(results):
            if isinstance(r, Exception):
                clean.append(suspects[i])  # 查詢失敗/超時 → 保留（不誤殺）
            elif r is not None:
                clean.append(r)

        # 重新排序
        clean.sort(key=lambda x: x.estimated_profit, reverse=True)
        filtered_count = len(suspects) - sum(1 for r in results if not isinstance(r, Exception) and r is not None)
        if filtered_count:
            logger.info(f"[套利] 過濾 {filtered_count} 個結構性價差機會")
        return clean

    def _align_raw_scale_prices(self, records: list):
        """Kraken/Deribit 用原生尺度（PEPE），別家用 1000PEPE → 價格差整數倍。
        依同幣組別中「非原生尺度所」的基準價，把原生尺度所的價格乘上對齊倍數
        （比值取最接近的 10 次方，且需落在合理倍數 {1000,1e4,1e5,1e6} 並貼近才對齊），
        避免被價格污染防護當成不同幣排除。費率(%)與尺度無關，不動。"""
        import math
        from collections import defaultdict
        groups: dict[str, list] = defaultdict(list)
        for r in records:
            if r.normalized_symbol:
                groups[r.normalized_symbol].append(r)
        for group in groups.values():
            raws = [r for r in group if r.exchange in RAW_SCALE_EXCHANGES]
            if not raws:
                continue
            ref_prices = sorted(
                r.mark_price for r in group
                if r.exchange not in RAW_SCALE_EXCHANGES and r.mark_price and r.mark_price > 0
            )
            if not ref_prices:
                continue
            ref = ref_prices[len(ref_prices) // 2]  # 中位數
            for r in raws:
                if not r.mark_price or r.mark_price <= 0:
                    continue
                ratio = ref / r.mark_price
                if ratio <= 0:
                    continue
                factor = 10 ** round(math.log10(ratio))
                if factor in VALID_SCALE_FACTORS and abs(ratio / factor - 1) < 0.15:
                    for attr in ("mark_price", "index_price", "bid_price", "ask_price"):
                        v = getattr(r, attr)
                        if v is not None:
                            setattr(r, attr, v * factor)
                    logger.debug(f"[scale] {r.exchange} {r.symbol} 價格 ×{factor} 對齊 {r.normalized_symbol}")

    # ── 滿資費通知（scan_once 後呼叫）───────────────────────

    async def _check_max_funding_after_scan(self, records: list):
        """掃描後檢查 BN 費率是否到頂且即將結算，發 TG 通知"""
        try:
            # 取得 BN rate cap（從 fundingInfo API）
            bn_caps = {}
            async with make_session() as session:
                async with session.get("https://fapi.binance.com/fapi/v1/fundingInfo") as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for item in data:
                            sym = item.get("symbol", "")
                            cap = item.get("adjustedFundingRateCap")  # 幣安實際欄位（非 fundingRateCap）
                            if sym.endswith("USDT") and cap is not None:
                                base = sym[:-4]
                                bn_caps[f"{base}/USDT:USDT"] = float(cap)

            if not bn_caps:
                return

            now = datetime.now(timezone.utc)
            notify_tools_path = str(Path(__file__).resolve().parent.parent.parent.parent / "tools" / "notify.py")

            for r in records:
                if r.exchange != "binance":
                    continue

                cap = bn_caps.get(r.symbol)
                if cap is None:
                    continue

                # 費率是否到頂：funding_rate 存的是原始「每期費率」，直接比 cap（每期上限）
                per_period_rate = abs(r.funding_rate)
                if per_period_rate < cap * 0.95:  # 容差 5%，接近上限就算
                    continue

                # 距離結算是否 < 1hr
                if r.funding_time is None:
                    continue
                time_to_settle = (r.funding_time - now).total_seconds()
                if time_to_settle < 0 or time_to_settle > 3600:
                    continue

                # 去重：同幣種同一輪結算只通知一次
                ft_key = r.funding_time.isoformat()
                if self._max_funding_notified.get(r.symbol) == ft_key:
                    continue
                self._max_funding_notified[r.symbol] = ft_key

                # 找 Aster 配對
                aster_rate = None
                norm_sym = r.normalized_symbol or r.symbol
                for ar in records:
                    if ar.exchange == "aster":
                        ar_norm = ar.normalized_symbol or ar.symbol
                        if ar_norm == norm_sym:
                            aster_rate = ar.funding_rate
                            break

                # 組通知訊息
                base = r.symbol.split("/")[0]
                direction = "正" if r.funding_rate > 0 else "負"
                rate_pct = r.funding_rate * 100
                mins = int(time_to_settle / 60)
                msg = f"[滿資費] {base}USDT ({direction})\nBN 費率: {rate_pct:.4f}% (每期) | {r.funding_interval_h}h結算\n距結算: {mins}分鐘"

                if aster_rate is not None:
                    aster_pct = aster_rate * 100
                    msg += f"\nAster: {aster_pct:.4f}% (8h基準)"
                    msg += f"\n→ Long BN + Short Aster"

                logger.info(f"[max_funding] 通知: {base} 費率到頂 {rate_pct:.4f}%，{mins}分後結算")

                try:
                    await asyncio.create_subprocess_exec("python", notify_tools_path, "-s", "5011", msg)
                except Exception as e:
                    logger.warning(f"[max_funding] TG 通知失敗: {e}")

            # 清理過期的去重記錄
            self._max_funding_notified = {
                s: ft for s, ft in self._max_funding_notified.items()
                if datetime.fromisoformat(ft) > now - timedelta(hours=2)
            }

        except Exception as e:
            logger.warning(f"[max_funding] 檢查失敗: {e}")

    async def _dc_lag_watch_loop(self):
        """60s 事件驅動追蹤（獨立於主掃描）：
        1) 滿資費：BN/BY 4h/8h 幣 |rate| 頂到 cap，或 1h 幣仍高資費 → 進 watch list
        2) 追蹤：watch 幣直到 BN/BY 週期翻 1h
        3) 查 DC：翻 1h 的幣新鮮確認 DeepCoin 週期，還在 4h/8h 才發警報（發指數頻道）
        BN/BY 週期切 1h 有幾十分鐘延遲，故用 60s 高頻抓「翻 1h 的瞬間」，儘量吃滿套利窗口。"""
        # 等交易所與首輪掃描就緒
        await asyncio.sleep(20)
        dc_ex = None
        while self.is_running:
            try:
                if dc_ex is None:
                    dc_ex = next((e for e in self._exchanges if getattr(e, "name", "") == "deepcoin"), None)
                await self._dc_lag_evaluate(dc_ex)
            except Exception as e:
                logger.warning(f"[dc-watch] 迴圈錯誤: {e}")
            await asyncio.sleep(DC_WATCH_INTERVAL)

    async def _dc_lag_evaluate(self, dc_ex):
        """一輪追蹤：抓 BN/BY 即時資費+週期+cap，更新 watch list，對翻 1h 的幣查 DC 落後。"""
        bn, by = await self._fetch_bn_by_funding_snapshot()
        if not bn and not by:
            return
        now = time.time()

        # 1) 更新 watch list（滿資費：頂 cap 或 1h 仍高資費）
        for k in set(bn) | set(by):
            maxfund = False
            for m in (bn.get(k), by.get(k)):
                if not m:
                    continue
                rate, ih, chi, clo = m["rate"], m["ih"], m["cap_hi"], m["cap_lo"]
                # 4h/8h 幣頂到自身 cap（±0.3%/±0.5%）→ 滿資費、即將切 1h
                if ih and ih > DC_SHORTENED_INTERVAL and chi and clo:
                    if rate >= chi * DC_CAP_TOLERANCE or rate <= clo * DC_CAP_TOLERANCE:
                        maxfund = True
                # 已是 1h 且仍高資費 → 視為滿資費事件進行中（涵蓋啟動時已切換的幣）
                if ih and ih <= DC_SHORTENED_INTERVAL and abs(rate) >= DC_REF_FUNDING_GATE:
                    maxfund = True
            if maxfund:
                self._dc_watch.setdefault(k, {"since": now})["last"] = now

        # 2)+3) 對 watch 幣，若 BN/BY 已翻 1h 且仍高資費 → 查 DC 週期
        alerts = []
        for k in list(self._dc_watch):
            w = self._dc_watch[k]
            if now - w.get("last", w["since"]) > DC_WATCH_EXPIRE_S:
                self._dc_watch.pop(k, None)
                self._dc_lag_notified.discard(k)
                continue
            legs = []
            for name, m in (("Binance", bn.get(k)), ("Bybit", by.get(k))):
                if m and m["ih"] and m["ih"] <= DC_SHORTENED_INTERVAL and abs(m["rate"]) >= DC_REF_FUNDING_GATE:
                    legs.append((name, m))
            if not legs:
                continue  # 尚未翻 1h（或翻了但資費已回落）→ 繼續追蹤

            dc_int = await self._deepcoin_fresh_interval(dc_ex, k)
            if dc_int is None:
                continue  # 無法確認 DC → 這輪先跳過，下輪 60s 再試（避免誤報）
            if dc_int < DC_LAG_MIN_INTERVAL:
                self._dc_lag_notified.discard(k)  # DC 已跟上(1h) → 機會關閉，恢復後可再通知
                continue
            if k in self._dc_lag_notified:
                continue  # 去重：同幣落後期間只通知一次
            self._dc_lag_notified.add(k)
            base = k.split("/")[0]
            cmp = " / ".join(f"{name} 1h {m['rate']*100:+.3f}%" for name, m in legs)
            alerts.append(f"{base}：{cmp}，但 DeepCoin 仍 {dc_int:.0f}h → 沒跟上縮短週期（滿資費套利機會）")

        if alerts:
            msg = "🎯 DeepCoin 結算週期落後（BN/BY 滿資費轉 1h，DeepCoin 還在 4h/8h）\n" + "\n".join(alerts)
            await self._send_index_watch_tg(msg)
            logger.info(f"[dc-watch] 通知 {len(alerts)} 個機會")

    async def _fetch_bn_by_funding_snapshot(self):
        """抓 Binance/Bybit 即時：每幣 {rate(每期), ih(週期h), cap_hi, cap_lo}。
        週期會隨滿資費即時變動，故 60s 迴圈自己抓、不吃主掃描 5 分鐘的舊值。"""
        def _pf(x):
            try:
                return float(x)
            except (TypeError, ValueError):
                return None

        bn: dict[str, dict] = {}
        by: dict[str, dict] = {}
        try:
            async with make_session() as s:
                async def bn_fetch():
                    info = {}
                    async with s.get("https://fapi.binance.com/fapi/v1/fundingInfo") as r:
                        for x in await r.json():
                            info[x.get("symbol")] = x
                    async with s.get("https://fapi.binance.com/fapi/v1/premiumIndex") as r:
                        for x in await r.json():
                            sym = x.get("symbol", "")
                            if not sym.endswith("USDT"):
                                continue
                            rate = _pf(x.get("lastFundingRate"))
                            if rate is None:
                                continue
                            fi = info.get(sym) or {}
                            ih = _pf(fi.get("fundingIntervalHours")) or 8.0
                            bn[f"{sym[:-4]}/USDT:USDT"] = {
                                "rate": rate, "ih": ih,
                                "cap_hi": _pf(fi.get("adjustedFundingRateCap")),
                                "cap_lo": _pf(fi.get("adjustedFundingRateFloor")),
                            }

                async def by_fetch():
                    info = {}
                    async with s.get("https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000") as r:
                        for x in ((await r.json()).get("result") or {}).get("list") or []:
                            info[x.get("symbol")] = x
                    async with s.get("https://api.bybit.com/v5/market/tickers?category=linear") as r:
                        for x in ((await r.json()).get("result") or {}).get("list") or []:
                            sym = x.get("symbol", "")
                            if not sym.endswith("USDT"):
                                continue
                            rate = _pf(x.get("fundingRate"))
                            if rate is None:
                                continue
                            ci = info.get(sym) or {}
                            fim = _pf(ci.get("fundingInterval"))  # 分鐘
                            by[f"{sym[:-4]}/USDT:USDT"] = {
                                "rate": rate, "ih": (fim / 60.0) if fim else 8.0,
                                "cap_hi": _pf(ci.get("upperFundingRate")),
                                "cap_lo": _pf(ci.get("lowerFundingRate")),
                            }

                await asyncio.gather(bn_fetch(), by_fetch())
        except Exception as e:
            logger.warning(f"[dc-watch] BN/BY 快照失敗: {e}")
        return bn, by

    async def _rwa_compute_loop(self):
        """RWA 兩張表每小時算一次並快取（含 TG 通知）。

        每小時足夠：標的是美股/商品、且期現那張表每次計算都要即時打各所現貨報價，
        跟著 5 分鐘掃描跑太重。API 直接回快取，前端不必等計算。
        """
        from services import rwa_arb

        await asyncio.sleep(40)                   # 等首輪掃描與現貨實例就緒
        while self.is_running:
            try:
                if self.last_result:
                    self.rwa_arb_cache = await rwa_arb.build_opportunities(self)
                    self.rwa_spread_cache = rwa_arb.build_cross_exchange_spreads(self)
                    self.rwa_leveraged_cache = await rwa_arb.build_leveraged_pairs(self)
                    self.rwa_lev_spot_cache = await rwa_arb.build_gate_leveraged_spot(self)
                    self.rwa_computed_at = datetime.now(timezone.utc).isoformat()
                    logger.info(f"[rwa] 更新：期現 {len(self.rwa_arb_cache)} 筆、跨所價差 {len(self.rwa_spread_cache)} 筆、槓桿對 {len(self.rwa_leveraged_cache)} 筆、Gate槓桿代幣 {len(self.rwa_lev_spot_cache)} 筆")
                    await self._check_rwa_spread()
            except Exception as e:
                logger.warning(f"[rwa] 計算失敗: {e}")
            await asyncio.sleep(3600)

    async def _check_rwa_spread(self):
        """RWA 跨所價差機會（價差−資費 >= 門檻）→ TG。同幣同方向去重，機會消失後可再通知。"""
        from services import rwa_arb

        try:
            opps = self.rwa_spread_cache or []
            hits = [o for o in opps if o["est_profit_pct"] >= rwa_arb.XS_NOTIFY_PROFIT_PCT]
            current, alerts = set(), []
            for o in hits:
                key = (o["base"], o["long_exchange"], o["short_exchange"])
                current.add(key)
                if key in self._rwa_spread_notified:
                    continue
                self._rwa_spread_notified.add(key)
                alerts.append(
                    f"{o['base']}（{o['name']}）預計利潤 {o['est_profit_pct']:+.2f}%\n"
                    f"  做多 {o['long_exchange']} @{o['long_ask']}｜做空 {o['short_exchange']} @{o['short_bid']}\n"
                    f"  價差 {-o['spread_pct']:+.2f}%｜資費 {o['funding_diff_pct']:+.3f}%（{o['norm_interval_h']:.0f}h基準）"
                    + (f"｜指數差 {o['index_diff_pct']:+.2f}%" if o.get("index_diff_pct") is not None else "")
                )
            for k in list(self._rwa_spread_notified):
                if k not in current:
                    self._rwa_spread_notified.discard(k)
            if alerts:
                msg = "📊 RWA 跨所價差機會（做多便宜所＋做空貴的所，賺價差收斂）\n" + "\n".join(alerts)
                await self._send_index_watch_tg(msg)
                logger.info(f"[rwa-spread] 通知 {len(alerts)} 個機會")
        except Exception as e:
            logger.warning(f"[rwa-spread] 檢查失敗: {e}")

    async def _rwa_universe_loop(self):
        """RWA 標的清單背景刷新：清單只在交易所上新標的時才變，一天一次足夠。
        失敗不影響掃描（沿用磁碟上的舊清單）。"""
        from services import rwa_arb

        await asyncio.sleep(15)
        while self.is_running:
            ce = self._crossex_api or {}
            key, secret = ce.get("apiKey"), ce.get("secret")
            if key and secret:
                cached = rwa_arb.load_universe()
                if time.time() - (cached.get("_ts") or 0) >= rwa_arb.UNIVERSE_TTL:
                    try:
                        await rwa_arb.refresh_universe(key, secret)
                    except Exception as e:
                        logger.warning(f"[rwa] 標的清單刷新失敗（沿用舊清單）: {e}")
            else:
                logger.info("[rwa] config 未設定 crossex_api，RWA 標的清單不刷新")
                return
            await asyncio.sleep(3600)

    async def _deepcoin_fresh_interval(self, dc_ex, symbol: str, retries: int = 2):
        """對單一 DeepCoin 幣種用結算歷史算出「當前真實週期」（最近兩次結算間隔，snap 到 0.5h）。
        避免依賴會限速失敗、且對剛換週期的幣不可信的批量宣告值。失敗重試，仍拿不到回 None。"""
        if dc_ex is None:
            return None
        for attempt in range(retries + 1):
            try:
                hist = await dc_ex.fetch_funding_history(symbol, limit=3)
                if hist and len(hist) >= 2:
                    # 最近一次結算要夠新（12h 內）；太舊代表 DeepCoin 已停/下架該幣，
                    # 不能套利（否則會拿數月前的老資料算出假的落後週期）。
                    if time.time() * 1000 - hist[-1]["timestamp"] > 12 * 3600 * 1000:
                        return None
                    gap_h = (hist[-1]["timestamp"] - hist[-2]["timestamp"]) / 3600000.0
                    if gap_h > 0:
                        return round(gap_h * 2) / 2
            except Exception:
                pass
            if attempt < retries:
                await asyncio.sleep(1.2)  # 遵守 DeepCoin 1/s
        return None

    async def _refresh_leverage_tiers(self):
        """背景刷新倉位限制快取（24h 一次）"""
        try:
            await refresh_leverage_cache(self._leverage_api_keys())
        except Exception as e:
            logger.warning(f"倉位限制快取刷新失敗: {e}")

    def _spot_markets_cache_path(self, eid: str) -> Path:
        """現貨 markets 的 disk cache 路徑"""
        base = Path(__file__).resolve().parent.parent / "data" / "spot_markets"
        base.mkdir(parents=True, exist_ok=True)
        return base / f"{eid}.json"

    def _save_spot_markets_cache(self, eid: str, ex):
        """把 ccxt 實例的 markets 寫入 disk（成功載入後呼叫）"""
        try:
            markets = getattr(ex, "markets", None)
            if not markets:
                return
            path = self._spot_markets_cache_path(eid)
            with path.open("w", encoding="utf-8") as f:
                json.dump({"ts": int(time.time()), "markets": markets}, f)
        except Exception as e:
            logger.debug(f"現貨 markets cache 寫入失敗 {eid}: {e}")

    @staticmethod
    def _slim_spot_markets(markets: dict) -> dict:
        """只保留現貨 USDT 計價對、並清空 info（省 RAM）。現貨搬磚/watch_order_book 只用
        /USDT 現貨對，info 原始欄位本專案唯讀行情用不到。回傳新 dict，不動原物件。"""
        slim = {}
        for sym, m in markets.items():
            if not isinstance(m, dict) or m.get("quote") != "USDT" or not m.get("spot"):
                continue
            m2 = dict(m)
            m2["info"] = {}  # 剝掉最肥的原始欄位
            slim[sym] = m2
        return slim

    def _apply_spot_markets_slim(self, ex, eid):
        """把 ccxt 實例的 markets 就地換成瘦身版（USDT 現貨 + 無 info），省 RAM。
        務必於「已存完整版到磁碟後」呼叫，磁碟備援不受影響。
        bybit 例外不瘦身：其 defaultType=swap、fetch_tickers 回的是永續 ticker，瘦身成只剩
        現貨後，永續 ticker 會被 ccxt safe_market 誤掛成現貨 symbol 污染搬磚（item B 審查 P0）；
        保留完整 markets 讓永續 ticker 仍解析成 :USDT 被濾掉，維持原行為（bybit 貢獻 0 筆）。"""
        if eid == "bybit":
            return
        try:
            slim = self._slim_spot_markets(getattr(ex, "markets", None) or {})
            if slim:
                ex.set_markets(list(slim.values()))
        except Exception as e:
            logger.debug(f"現貨 markets 瘦身失敗: {e}")

    def _inject_spot_markets_from_cache(self, eid: str, ex) -> bool:
        """從 disk cache 注入 markets 到 ccxt 實例，繞過 REST load_markets。

        用於 api.mexc.com 這類 REST endpoint 被 Akamai 間歇性封鎖時，讓
        ccxt.pro WS 仍能啟動（watch_order_book 內部需要 markets 做 symbol→id 解析）。
        """
        try:
            path = self._spot_markets_cache_path(eid)
            if not path.exists():
                return False
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            markets = data.get("markets") or {}
            if not markets:
                return False
            # 注入前瘦身：只留 USDT 現貨 + 剝 info（磁碟快取仍是完整版）。bybit 例外不瘦身
            # （defaultType=swap，瘦身後永續 ticker 會被誤掛現貨污染搬磚，見審查 P0）；
            # 瘦身為空（快取異常）時退回完整版，不註冊空殼。
            if eid == "bybit":
                ex.set_markets(list(markets.values()))
            else:
                ex.set_markets(list((self._slim_spot_markets(markets) or markets).values()))
            return True
        except Exception as e:
            logger.debug(f"現貨 markets cache 載入失敗 {eid}: {e}")
            return False

    async def _fetch_mexc_spot_markets_fallback(self) -> dict | None:
        """從 www.mexc.com 平台 API 拉現貨 symbols，轉成 ccxt markets 格式。

        用於 api.mexc.com 被 Akamai 封鎖且 disk cache 也不存在的啟動場景。
        回傳：{symbol: market_dict}，符合 ccxt.set_markets() 所需格式。
        """
        try:
            async with make_session(10) as session:
                async with session.get(
                    "https://www.mexc.com/api/platform/spot/market/symbols",
                    headers={"User-Agent": "Mozilla/5.0"},
                ) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
            groups = data.get("data") or {}
            markets = {}
            for quote_id, entries in groups.items():
                if not isinstance(entries, list):
                    continue
                for e in entries:
                    base = e.get("currency")
                    quote = e.get("market") or quote_id
                    if not base or not quote:
                        continue
                    symbol = f"{base}/{quote}"
                    sym_id = f"{base}{quote}"
                    price_scale = e.get("priceScale") or 8
                    qty_scale = e.get("quantityScale") or 4
                    markets[symbol] = {
                        "id": sym_id,
                        "symbol": symbol,
                        "base": base,
                        "quote": quote,
                        "baseId": base,
                        "quoteId": quote,
                        "type": "spot",
                        "spot": True,
                        "swap": False,
                        "future": False,
                        "option": False,
                        "contract": False,
                        "active": True,
                        "precision": {
                            "price": 10 ** -int(price_scale),
                            "amount": 10 ** -int(qty_scale),
                        },
                        "limits": {
                            "amount": {"min": None, "max": None},
                            "price": {"min": None, "max": None},
                            "cost": {"min": None, "max": None},
                        },
                        "info": {},
                    }
            return markets if markets else None
        except Exception as e:
            logger.debug(f"MEXC www.mexc.com markets fallback 失敗: {e}")
            return None

    async def _init_spot_exchanges(self):
        """初始化現貨 ccxt 實例（並行載入市場）

        任何交易所初始化失敗時：若 disk cache 存在則注入 markets（WS 仍可運作），
        同時啟動背景 retry 等待 REST 封鎖期過去後重新載入最新 markets。
        """
        async def _init_one(eid):
            ex = getattr(ccxt_async, eid)({"enableRateLimit": True})
            scope_bybit_markets(ex, eid)
            try:
                try:
                    await ex.load_markets()
                    self._spot_exchanges[eid] = ex
                    self._save_spot_markets_cache(eid, ex)  # 先存完整版到磁碟
                    self._apply_spot_markets_slim(ex, eid)  # 再把 RAM markets 瘦身（bybit 例外，見 P0）
                    return
                except Exception as e:
                    logger.warning(f"現貨交易所 {eid} 初始化失敗: {e}")

                # Fallback 1: disk cache
                if self._inject_spot_markets_from_cache(eid, ex):
                    self._spot_exchanges[eid] = ex
                    logger.info(f"現貨交易所 {eid} 已從 disk cache 注入 markets")
                    asyncio.create_task(self._retry_spot_init(eid))
                    return

                # Fallback 2: MEXC 特殊路徑（www.mexc.com 平台 API 避開 Akamai）
                if eid == "mexc":
                    markets = await self._fetch_mexc_spot_markets_fallback()
                    if markets:
                        ex.set_markets(list(markets.values()))
                        self._spot_exchanges[eid] = ex
                        self._save_spot_markets_cache(eid, ex)  # 先存完整版
                        self._apply_spot_markets_slim(ex, eid)  # 再瘦身 RAM
                        logger.info(f"現貨交易所 mexc 已從 www.mexc.com 注入 {len(markets)} 個 markets")
                        asyncio.create_task(self._retry_spot_init(eid))
                        return

                asyncio.create_task(self._retry_spot_init(eid))
            finally:
                # 沒被收進 _spot_exchanges（含中途被取消，CancelledError 不被 except Exception
                # 捕捉）的實例一律關閉，避免漏 ccxt session/connector。
                if self._spot_exchanges.get(eid) is not ex:
                    try:
                        # shield：被取消時 close 仍 detached 跑完底層 connector 釋放，
                        # CancelledError 照常向上傳（不吞、不卡 shutdown）。
                        await asyncio.shield(ex.close())
                    except Exception:
                        pass

        await asyncio.gather(*[_init_one(eid) for eid in SPOT_EXCHANGE_IDS])
        logger.info(f"現貨交易所初始化完成：{len(self._spot_exchanges)} 個")

    async def _retry_spot_init(self, eid: str):
        """背景重試 REST load_markets，每 60 秒試一次直到成功。

        成功後：若 _spot_exchanges 已有 cache-injected 實例則更新其 markets；
        否則塞入新實例。無論哪種都寫 disk cache（讓下次啟動有最新資料）。
        """
        attempt = 0
        while self.is_running:
            await asyncio.sleep(60)
            attempt += 1
            ex = None
            try:
                ex = getattr(ccxt_async, eid)({"enableRateLimit": True})
                scope_bybit_markets(ex, eid)
                await ex.load_markets()
                existing = self._spot_exchanges.get(eid)
                if existing is not None:
                    existing.set_markets(list(ex.markets.values()))
                    logger.info(f"現貨交易所 {eid} REST 恢復，markets 已刷新（第 {attempt} 次）")
                else:
                    self._spot_exchanges[eid] = ex
                    logger.info(f"現貨交易所 {eid} 背景重試成功（第 {attempt} 次）")
                self._save_spot_markets_cache(eid, self._spot_exchanges[eid])  # 先存完整版
                self._apply_spot_markets_slim(self._spot_exchanges[eid], eid)  # 再瘦身 RAM
                return
            except Exception as e:
                logger.debug(f"現貨交易所 {eid} 背景重試 #{attempt} 失敗: {str(e)[:80]}")
            finally:
                # 沒被收進 _spot_exchanges 的實例一律關閉——含「在 load_markets 途中被取消」
                # 的情形：CancelledError 是 BaseException，不被 except Exception 捕捉，
                # 寫在 finally 才能在關閉/重啟取消任務時也關掉，避免漏 ccxt session/connector。
                if ex is not None and self._spot_exchanges.get(eid) is not ex:
                    try:
                        # shield：被取消時 close 仍 detached 跑完底層 connector 釋放，
                        # CancelledError 照常向上傳（不吞、不卡 shutdown）。
                        await asyncio.shield(ex.close())
                    except Exception:
                        pass

    async def _spot_scan_loop(self):
        """現貨搬磚掃描迴圈：每 60 秒掃描一次"""
        while self.is_running:
            try:
                await self._update_spot_arbitrage()
            except Exception as e:
                logger.warning(f"現貨搬磚掃描失敗: {e}")
            await asyncio.sleep(60)

    async def _fetch_spot_networks(self):
        """取得各交易所的充提鏈資訊（每小時更新一次）"""
        now = time.time()
        if now - self._spot_networks_ts < 3600 and self._spot_networks:
            return  # 快取未過期

        tasks = [
            self._fetch_networks_binance(),
            self._fetch_networks_bybit(),
            self._fetch_networks_ccxt("bitget"),
            self._fetch_networks_ccxt("gateio"),
            self._fetch_networks_auth("okx", self._okx_api),
            self._fetch_networks_auth("bingx", self._bingx_api),
            self._fetch_networks_auth("mexc", self._mexc_api),
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
        self._spot_networks_ts = now
        # 更新合約地址對照表（供套利偵測同名不同幣比對）
        self._save_token_contracts()

    def _save_token_contracts(self):
        """從 _spot_networks 提取合約地址，存成本地對照表 data/token_contracts.json
        格式：{coin: {exchange: {chain: contract_address}}}
        """
        table: dict[str, dict[str, dict[str, str]]] = {}
        for exchange, coins in self._spot_networks.items():
            if exchange == "binance_alpha":
                continue  # Alpha 代幣另外處理
            for coin, networks in coins.items():
                for net in networks:
                    contract = net.get("contract")
                    if not contract:
                        continue
                    chain = net.get("network", "unknown")
                    table.setdefault(coin, {}).setdefault(exchange, {})[chain] = contract
        try:
            out_path = Path(__file__).resolve().parent.parent / "data" / "token_contracts.json"
            out_path.parent.mkdir(exist_ok=True)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(table, f, ensure_ascii=False, indent=2)
            logger.info(f"合約地址對照表已更新：{len(table)} 個幣種")
        except Exception as e:
            logger.warning(f"儲存合約地址對照表失敗: {e}")

    async def _fetch_networks_binance(self):
        """Binance 公開網頁 API 取得充提鏈"""
        try:
            async with make_session() as session:
                async with session.get(
                    "https://www.binance.com/bapi/capital/v1/public/capital/getNetworkCoinAll"
                ) as resp:
                    if resp.status != 200:
                        return
                    data = await resp.json()
                    items = data.get("data", [])

            ex_networks = {}
            for item in items:
                coin = item.get("coin", "")
                net_list_raw = item.get("networkList", [])
                net_list = []
                for n in net_list_raw:
                    net_list.append({
                        "network": _normalize_network(n.get("network", "")),
                        "raw_name": n.get("name", n.get("network", "")),
                        "deposit": bool(n.get("depositEnable", False)),
                        "withdraw": bool(n.get("withdrawEnable", False)),
                        "contract": (n.get("contractAddress") or "").lower() or None,
                    })
                if net_list:
                    ex_networks[coin] = net_list
            self._spot_networks["binance"] = ex_networks
            logger.info(f"[binance] 充提鏈取得完成：{len(ex_networks)} 個幣種")
        except Exception as e:
            logger.warning(f"[binance] 充提鏈取得失敗: {e}")

    async def _fetch_networks_bybit(self):
        """Bybit 認證 API 取得充提鏈"""
        if not self._bybit_api_key or not self._bybit_api_secret:
            return
        try:
            async with make_session() as session:
                ts = str(int(time.time() * 1000))
                recv = "5000"
                params = ""
                sign = self._bybit_sign(params, ts, recv)
                headers = {
                    "X-BAPI-API-KEY": self._bybit_api_key,
                    "X-BAPI-SIGN": sign,
                    "X-BAPI-TIMESTAMP": ts,
                    "X-BAPI-RECV-WINDOW": recv,
                }
                async with session.get(
                    "https://api.bybit.com/v5/asset/coin/query-info",
                    headers=headers,
                ) as resp:
                    if resp.status != 200:
                        return
                    data = await resp.json()
                    rows = data.get("result", {}).get("rows", [])

            ex_networks = {}
            for row in rows:
                coin = row.get("coin", "")
                chains = row.get("chains", [])
                net_list = []
                for c in chains:
                    net_list.append({
                        "network": _normalize_network(c.get("chain", "")),
                        "raw_name": c.get("chainType", c.get("chain", "")),
                        "deposit": c.get("chainDeposit") == "1",
                        "withdraw": c.get("chainWithdraw") == "1",
                        "contract": (c.get("contractAddress") or "").lower() or None,
                    })
                if net_list:
                    ex_networks[coin] = net_list
            self._spot_networks["bybit"] = ex_networks
            logger.info(f"[bybit] 充提鏈取得完成：{len(ex_networks)} 個幣種")
        except Exception as e:
            logger.warning(f"[bybit] 充提鏈取得失敗: {e}")

    async def _fetch_networks_auth(self, exchange_id: str, api_config: dict):
        """用認證的 ccxt 實例取得充提鏈（OKX/BingX/MEXC 等需要 API Key 的交易所）"""
        if not api_config or not api_config.get("apiKey"):
            return
        try:
            config = {
                "apiKey": api_config["apiKey"],
                "secret": api_config["secret"],
                "enableRateLimit": True,
            }
            if api_config.get("password"):
                config["password"] = api_config["password"]

            ex = getattr(ccxt_async, exchange_id)(config)
            try:
                currencies = await ex.fetch_currencies()
                if not currencies:
                    return
                ex_networks = {}
                for code, info in currencies.items():
                    networks = info.get("networks", {})
                    net_list = []
                    if networks:
                        for net_id, net_info in networks.items():
                            raw = net_info.get("info", {}) or {}
                            contract = (
                                raw.get("contractAddress")  # BingX, Bitget
                                or raw.get("ctAddr")        # OKX
                                or raw.get("contract")      # MEXC
                                or raw.get("addr")          # Gate.io
                                or ""
                            )
                            net_list.append({
                                "network": _normalize_network(net_id),
                                "raw_name": net_id,
                                "deposit": bool(net_info.get("deposit", True)),
                                "withdraw": bool(net_info.get("withdraw", True)),
                                "contract": contract.lower() if contract else None,
                            })
                    if net_list:
                        ex_networks[code] = net_list
                self._spot_networks[exchange_id] = ex_networks
                logger.info(f"[{exchange_id}] 充提鏈取得完成（認證）：{len(ex_networks)} 個幣種")
            finally:
                await ex.close()
        except Exception as e:
            logger.warning(f"[{exchange_id}] 充提鏈取得失敗（認證）: {e}")

    async def _fetch_networks_ccxt(self, exchange_id: str):
        """用 ccxt fetchCurrencies 取得充提鏈（Bitget/Gate.io 公開可用）"""
        ex = self._spot_exchanges.get(exchange_id)
        if not ex:
            return
        try:
            currencies = await ex.fetch_currencies()
            if not currencies:
                return
            ex_networks = {}
            for code, info in currencies.items():
                networks = info.get("networks", {})
                net_list = []
                if networks:
                    for net_id, net_info in networks.items():
                        raw = net_info.get("info", {}) or {}
                        contract = (
                            raw.get("contractAddress")
                            or raw.get("ctAddr")
                            or raw.get("contract")
                            or raw.get("addr")
                            or ""
                        )
                        net_list.append({
                            "network": _normalize_network(net_id),
                            "raw_name": net_id,
                            "deposit": bool(net_info.get("deposit", True)),
                            "withdraw": bool(net_info.get("withdraw", True)),
                            "contract": contract.lower() if contract else None,
                        })
                if net_list:
                    ex_networks[code] = net_list
            self._spot_networks[exchange_id] = ex_networks
            logger.info(f"[{exchange_id}] 充提鏈取得完成：{len(ex_networks)} 個幣種")
        except Exception as e:
            logger.warning(f"[{exchange_id}] 充提鏈取得失敗: {e}")

    async def _fetch_alpha_tokens(self):
        """取得 Binance Alpha token 清單（含價格、合約地址、chainId）"""
        now = time.time()
        if now - self._alpha_tokens_ts < 300 and self._alpha_tokens:
            return  # 5 分鐘快取
        try:
            async with make_session() as session:
                async with session.get(
                    "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"[binance_alpha] token list HTTP {resp.status}")
                        return
                    data = await resp.json()
                    items = data.get("data", [])

            tokens = {}
            skipped_cex = 0
            for item in items:
                symbol = (item.get("symbol") or "").upper()
                if not symbol:
                    continue
                # 已上正式幣安現貨或已下架的跳過
                if item.get("listingCex") or item.get("offline"):
                    skipped_cex += 1
                    continue
                price = item.get("price")
                if price is not None:
                    price = float(price)
                vol_str = item.get("volume24h")
                tokens[symbol] = {
                    "price": price,
                    "volume24h": float(vol_str) if vol_str else None,
                    "contractAddress": (item.get("contractAddress") or "").lower() or None,
                    "chainId": item.get("chainId"),
                    "tokenId": item.get("tokenId"),
                    "alphaId": item.get("alphaId"),
                }

            self._alpha_tokens = tokens
            self._alpha_tokens_ts = now

            # 建立 Alpha 的 network 資訊
            alpha_networks = {}
            for sym, info in tokens.items():
                chain_id_raw = info.get("chainId")
                contract = info.get("contractAddress")
                if chain_id_raw is not None:
                    # chainId 可能是字串 "56" 或非標準格式 "CT_784"
                    try:
                        chain_id_int = int(chain_id_raw)
                    except (ValueError, TypeError):
                        chain_id_int = None
                    net_name = ALPHA_CHAIN_MAP.get(chain_id_int, f"CHAIN-{chain_id_raw}") if chain_id_int else f"CHAIN-{chain_id_raw}"
                    alpha_networks[sym] = [{
                        "network": net_name,
                        "raw_name": f"chainId:{chain_id_raw}",
                        "deposit": True,
                        "withdraw": True,
                        "contract": contract,
                    }]
            self._spot_networks["binance_alpha"] = alpha_networks

            logger.info(f"[binance_alpha] token list 取得完成：{len(tokens)} 個 Alpha 代幣（跳過 {skipped_cex} 個已上CEX/已下架）")
        except Exception as e:
            logger.warning(f"[binance_alpha] token list 取得失敗: {e}")

    def _get_common_networks(self, currency: str, buy_ex: str, sell_ex: str) -> list[str]:
        """找出兩交易所共同支援的充提鏈（買入所可提現 & 賣出所可充值）

        優先用合約地址精準匹配，確保是同一幣種；
        若雙方都沒有合約地址（原生幣如 BTC/ETH），則退回鏈名匹配。
        """
        buy_nets = self._spot_networks.get(buy_ex, {}).get(currency, [])
        sell_nets = self._spot_networks.get(sell_ex, {}).get(currency, [])

        if not buy_nets or not sell_nets:
            return []

        # 買入所可提現的鏈
        buy_withdraw = [n for n in buy_nets if n["withdraw"]]
        # 賣出所可充值的鏈
        sell_deposit = [n for n in sell_nets if n["deposit"]]

        common = []
        for bw in buy_withdraw:
            for sd in sell_deposit:
                if bw["network"] != sd["network"]:
                    continue
                # 鏈名匹配，再驗證合約地址
                bw_addr = bw.get("contract")
                sd_addr = sd.get("contract")
                if bw_addr and sd_addr:
                    # 雙方都有合約地址 → 必須一致
                    if bw_addr == sd_addr:
                        common.append(bw["network"])
                elif not bw_addr and not sd_addr:
                    # 雙方都沒有合約地址（原生幣）→ 信任鏈名
                    common.append(bw["network"])
                else:
                    # 一方有一方沒有 → 信任鏈名（部分交易所不回傳原生幣地址）
                    common.append(bw["network"])

        return sorted(set(common))

    def _get_exchange_networks(self, currency: str, exchange: str) -> list[dict]:
        """取得某交易所對某幣種的所有充提鏈"""
        nets = self._spot_networks.get(exchange, {}).get(currency, [])
        return nets

    async def _update_spot_arbitrage(self):
        """掃描各交易所現貨價格，偵測搬磚機會"""
        if not self._spot_exchanges:
            return

        start_time = time.time()

        # 更新充提鏈資訊 + Alpha tokens
        t0 = time.time()
        await asyncio.gather(
            self._fetch_spot_networks(),
            self._fetch_alpha_tokens(),
            return_exceptions=True,
        )
        t_networks = (time.time() - t0) * 1000

        # 並行取得所有交易所的現貨 USDT tickers
        all_tickers = {}  # {exchange: {symbol: ticker}}
        ticker_times = {}  # 每個交易所的耗時

        async def _fetch_tickers(eid, ex):
            t1 = time.time()
            try:
                tickers = await ex.fetch_tickers()
                # 只保留 USDT 交易對
                usdt_tickers = {}
                for sym, t in tickers.items():
                    if "/USDT" in sym and ":USDT" not in sym and not sym.endswith(":"):
                        base = sym.split("/")[0]
                        bid = t.get("bid")
                        ask = t.get("ask")
                        vol = t.get("quoteVolume") or t.get("baseVolume")
                        if bid and ask and bid > 0 and ask > 0:
                            usdt_tickers[base] = {
                                "bid": bid, "ask": ask,
                                "volume": vol,
                                "symbol": sym,
                            }
                all_tickers[eid] = usdt_tickers
                ticker_times[eid] = (time.time() - t1) * 1000
            except Exception as e:
                ticker_times[eid] = (time.time() - t1) * 1000
                logger.warning(f"[{eid}] 現貨 tickers 取得失敗: {e}")

        t0 = time.time()
        await asyncio.gather(
            *[_fetch_tickers(eid, ex) for eid, ex in self._spot_exchanges.items()],
            return_exceptions=True,
        )
        t_tickers = (time.time() - t0) * 1000
        ticker_detail = " ".join(f"{k}:{v:.0f}" for k, v in sorted(ticker_times.items(), key=lambda x: -x[1]))
        logger.info(f"現貨搬磚計時：networks={t_networks:.0f}ms, tickers={t_tickers:.0f}ms [{ticker_detail}]")

        # 加入 Binance Alpha tokens：用 token list 的 price 預篩，候選再用 fullDepth 取精確深度
        if self._alpha_tokens:
            alpha_tickers = {}
            for sym, info in self._alpha_tokens.items():
                price = info.get("price")
                if price and price > 0:
                    alpha_tickers[sym] = {
                        "bid": price, "ask": price,
                        "volume": info.get("volume24h"),
                        "symbol": f"{sym}/USDT",
                        "is_alpha": True,
                    }
            if alpha_tickers:
                all_tickers["binance_alpha"] = alpha_tickers
                logger.debug(f"[binance_alpha] token list 價格: {len(alpha_tickers)} 個")

        if not all_tickers:
            return

        # 按幣種分組
        coin_prices = {}  # {base_coin: {exchange: {bid, ask, volume}}}
        for eid, tickers in all_tickers.items():
            for base, data in tickers.items():
                if base not in coin_prices:
                    coin_prices[base] = {}
                coin_prices[base][eid] = data

        # === 第一階段：用 ticker 預篩候選（spread > 0.3%，留寬一點給深度修正） ===
        candidates = []  # [(base, best_buy_eid, best_sell_eid, exchanges_dict)]
        for base, exchanges in coin_prices.items():
            if len(exchanges) < 2:
                continue

            best_buy = None
            best_sell = None
            min_ask = float("inf")
            max_bid = 0

            for eid, data in exchanges.items():
                if data["ask"] < min_ask:
                    min_ask = data["ask"]
                    best_buy = eid
                if data["bid"] > max_bid:
                    max_bid = data["bid"]
                    best_sell = eid

            if best_buy == best_sell or min_ask <= 0:
                continue

            spread_pct = (max_bid - min_ask) / min_ask * 100
            if spread_pct < 0.3:  # 預篩閾值比最終低，留餘地
                continue

            # 先檢查共同鏈
            common_nets = self._get_common_networks(base, best_buy, best_sell)
            if not common_nets:
                continue

            candidates.append((base, best_buy, best_sell, exchanges, common_nets))

        logger.info(f"現貨搬磚預篩：{len(candidates)} 個候選（ticker spread > 0.3%）")

        # === 第二階段：對候選幣種抓 5 檔深度，算加權平均價 ===
        ob_tasks = {}  # key: (eid, base) -> symbol
        for base, best_buy, best_sell, exchanges, _ in candidates:
            for eid in [best_buy, best_sell]:
                sym = exchanges[eid]["symbol"]
                ob_tasks[(eid, base)] = sym

        # 並行抓 orderbook，按交易所分組避免 rate limit
        ob_results = {}  # (eid, base) -> {w_bid, w_ask}

        # 分離 Alpha 和一般交易所的 ob_tasks
        alpha_ob_bases = set()  # Alpha 候選的 base coins
        normal_ob_tasks = {}
        for key, sym in ob_tasks.items():
            eid, base = key
            if eid == "binance_alpha":
                alpha_ob_bases.add(base)
            else:
                normal_ob_tasks[key] = sym
        # 候選幣種中有 Alpha 價格的，一律抓 fullDepth（獨立 semaphore，不影響一般交易所）
        for base, _, _, exchanges, _ in candidates:
            if "binance_alpha" in exchanges:
                alpha_ob_bases.add(base)

        async def _fetch_ob(eid, base, sym):
            try:
                ex = self._spot_exchanges.get(eid)
                if not ex:
                    return
                ob = await ex.fetch_order_book(sym, limit=5)
                bids = ob.get("bids", [])[:5]
                asks = ob.get("asks", [])[:5]
                w_bid = self._weighted_avg(bids)
                w_ask = self._weighted_avg(asks)
                if w_bid and w_ask:
                    ob_results[(eid, base)] = {
                        "w_bid": w_bid, "w_ask": w_ask,
                        "bid_qty": sum(lv[1] for lv in bids),
                        "ask_qty": sum(lv[1] for lv in asks),
                    }
            except Exception as e:
                logger.debug(f"[{eid}] orderbook {sym} 失敗: {e}")

        sem = asyncio.Semaphore(20)

        async def _fetch_ob_limited(eid, base, sym):
            async with sem:
                await _fetch_ob(eid, base, sym)

        async def _fetch_alpha_depth_rest():
            """用 REST fullDepth 端點批次取得所有 Alpha 候選的 5 檔深度"""
            if not alpha_ob_bases:
                return

            alpha_sem = asyncio.Semaphore(10)

            async def _fetch_one(base):
                info = self._alpha_tokens.get(base, {})
                alpha_id = info.get("alphaId")
                if not alpha_id:
                    return
                try:
                    async with alpha_sem:
                        async with make_session() as session:
                            url = f"https://www.binance.com/bapi/defi/v1/public/alpha-trade/fullDepth?symbol={alpha_id}USDT&limit=5"
                            async with session.get(url) as resp:
                                if resp.status != 200:
                                    return
                                data = await resp.json()
                                if not data.get("success"):
                                    return
                                d = data.get("data", {})
                                bids = [[float(p), float(q)] for p, q in d.get("bids", [])[:5]]
                                asks = [[float(p), float(q)] for p, q in d.get("asks", [])[:5]]
                                w_bid = self._weighted_avg(bids)
                                w_ask = self._weighted_avg(asks)
                                if w_bid and w_ask:
                                    ob_results[("binance_alpha", base)] = {
                                        "w_bid": w_bid, "w_ask": w_ask,
                                        "bid_qty": sum(lv[1] for lv in bids),
                                        "ask_qty": sum(lv[1] for lv in asks),
                                    }
                except Exception as e:
                    logger.debug(f"[binance_alpha] fullDepth {base} 失敗: {e}")

            await asyncio.gather(
                *[_fetch_one(base) for base in alpha_ob_bases],
                return_exceptions=True,
            )
            ok_count = sum(1 for b in alpha_ob_bases if ("binance_alpha", b) in ob_results)
            logger.info(f"[binance_alpha] fullDepth 完成：{len(alpha_ob_bases)} 個候選，{ok_count} 個取得深度")

        # 並行：一般交易所 REST orderbook + Alpha fullDepth
        t0 = time.time()
        await asyncio.gather(
            asyncio.gather(
                *[_fetch_ob_limited(eid, base, sym) for (eid, base), sym in normal_ob_tasks.items()],
                return_exceptions=True,
            ),
            _fetch_alpha_depth_rest(),
            return_exceptions=True,
        )
        t_depth = (time.time() - t0) * 1000
        logger.info(f"現貨搬磚計時：depth={t_depth:.0f}ms（一般{len(normal_ob_tasks)}個 + Alpha{len(alpha_ob_bases)}個）")

        # === 第三階段：用加權價重新計算價差，建立最終結果 ===
        opportunities = []
        for base, best_buy, best_sell, exchanges, common_nets in candidates:
            # 用深度加權價，沒抓到就退回 ticker 價
            buy_ob = ob_results.get((best_buy, base))
            sell_ob = ob_results.get((best_sell, base))

            buy_ask = buy_ob["w_ask"] if buy_ob else exchanges[best_buy]["ask"]
            sell_bid = sell_ob["w_bid"] if sell_ob else exchanges[best_sell]["bid"]

            if buy_ask <= 0:
                continue

            spread_pct = (sell_bid - buy_ask) / buy_ask * 100
            if spread_pct < 0.5:
                continue

            # 所有交易所價格列表（用深度價覆蓋）
            all_prices = []
            for eid in list(SPOT_EXCHANGE_IDS) + ["binance_alpha"]:
                if eid in exchanges:
                    d = exchanges[eid]
                    ob = ob_results.get((eid, base))
                    nets = self._get_exchange_networks(base, eid)
                    all_prices.append({
                        "exchange": eid,
                        "label": SPOT_EXCHANGE_LABELS.get(eid, eid),
                        "bid": ob["w_bid"] if ob else d["bid"],
                        "ask": ob["w_ask"] if ob else d["ask"],
                        "bid1": d["bid"],
                        "ask1": d["ask"],
                        "volume": d.get("volume"),
                        "networks": nets,
                    })

            buy_qty = buy_ob["ask_qty"] if buy_ob else None
            sell_qty = sell_ob["bid_qty"] if sell_ob else None
            tradeable_qty = min(buy_qty, sell_qty) if (buy_qty and sell_qty) else (buy_qty or sell_qty)

            opportunities.append({
                "symbol": f"{base}/USDT",
                "base_coin": base,
                "buy_exchange": best_buy,
                "buy_label": SPOT_EXCHANGE_LABELS.get(best_buy, best_buy),
                "sell_exchange": best_sell,
                "sell_label": SPOT_EXCHANGE_LABELS.get(best_sell, best_sell),
                "buy_ask": round(buy_ask, 10),
                "sell_bid": round(sell_bid, 10),
                "spread_pct": round(spread_pct, 4),
                "tradeable_qty": round(tradeable_qty, 4) if tradeable_qty else None,
                "tradeable_usd": round(tradeable_qty * buy_ask, 2) if tradeable_qty else None,
                "common_networks": common_nets,
                "buy_volume": exchanges[best_buy].get("volume"),
                "sell_volume": exchanges[best_sell].get("volume"),
                "all_prices": all_prices,
            })

        # 按價差排序
        opportunities.sort(key=lambda x: x["spread_pct"], reverse=True)
        self.spot_arbitrage = opportunities
        self._spot_scan_duration_ms = round((time.time() - start_time) * 1000)
        self._spot_scan_total_coins = len(coin_prices)

        duration = self._spot_scan_duration_ms
        logger.info(
            f"現貨搬磚掃描完成：{len(coin_prices)} 個幣種，"
            f"{len(opportunities)} 個機會（≥0.5%），耗時 {duration:.0f}ms"
        )

    @staticmethod
    def _weighted_avg(levels: list) -> float | None:
        """計算 orderbook 檔位的加權平均價（按數量加權）

        levels: [[price, amount], ...] — ccxt orderbook 格式
        """
        if not levels:
            return None
        total_qty = sum(lv[1] for lv in levels)
        if total_qty <= 0:
            return None
        return sum(lv[0] * lv[1] for lv in levels) / total_qty

    def _bybit_sign(self, params_str: str, timestamp: str, recv_window: str = "5000") -> str:
        """產生 Bybit V5 API HMAC-SHA256 簽名"""
        pre_sign = f"{timestamp}{self._bybit_api_key}{recv_window}{params_str}"
        return hmac.new(
            self._bybit_api_secret.encode("utf-8"),
            pre_sign.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    async def _fetch_bybit_max_borrowable(self, session: aiohttp.ClientSession, currency: str) -> float | None:
        """查詢帳號對某幣種的最大可借數量（認證 API）"""
        url = "https://api.bybit.com/v5/spot-margin-trade/max-borrowable"
        recv_window = "5000"
        timestamp = str(int(time.time() * 1000))
        params_str = f"currency={currency}"
        sign = self._bybit_sign(params_str, timestamp, recv_window)

        headers = {
            "X-BAPI-API-KEY": self._bybit_api_key,
            "X-BAPI-SIGN": sign,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
        }
        try:
            async with session.get(f"{url}?{params_str}", headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("retCode") == 0:
                        max_loan = data.get("result", {}).get("maxLoan", "0")
                        return float(max_loan) if max_loan else 0
        except Exception as e:
            logger.debug(f"Bybit max-borrowable {currency} 失敗: {e}")
        return None

    async def _fetch_bybit_borrow_rates(self) -> dict[str, dict]:
        """從公開 API 一次取得所有幣種的借貸資訊（利率 + 可借上限 + 是否可借）"""
        info_map = {}  # currency -> {hourly_rate, max_borrowing_amount, borrowable}
        try:
            async with make_session() as session:
                url = "https://api.bybit.com/v5/spot-margin-trade/data"
                async with session.get(url, params={"vipLevel": self._bybit_vip_level}) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        vip_list = data.get("result", {}).get("vipCoinList", [])
                        if vip_list:
                            for coin in vip_list[0].get("list", []):
                                currency = coin.get("currency", "")
                                if not currency:
                                    continue
                                rate_str = coin.get("hourlyBorrowRate", "")
                                max_borrow_str = coin.get("maxBorrowingAmount", "")
                                info_map[currency] = {
                                    "hourly_rate": float(rate_str) if rate_str else None,
                                    "max_borrowing_amount": float(max_borrow_str) if max_borrow_str else None,
                                    "borrowable": coin.get("borrowable", False),
                                }
            logger.info(f"Bybit 借貸資訊取得完成：{len(info_map)} 個幣種")
        except Exception as e:
            logger.warning(f"Bybit 借貸資訊取得失敗: {e}")
        return info_map

    async def fetch_spot_borrow_rates(self, coin: str) -> dict:
        """查某幣現貨保證金「借幣年化利率(%)」，供套利建議空方現貨計入借貸成本。
        目前涵蓋有公開單幣端點的 Bybit / Bitget（其餘所端點各異、多需認證，暫未接）。
        """
        out = {}  # {exchange_id: annual_pct}
        # Bitget（公開單幣，annualInterest 為年化小數）
        try:
            async with make_session() as session:
                ir = await self._fetch_bitget_margin_loan(session, coin)
            if ir and ir.get("annual_rate") is not None:
                out["bitget"] = round(ir["annual_rate"] * 100, 4)
        except Exception as e:
            logger.debug(f"Bitget 借貸利率查詢失敗 {coin}: {e}")
        # Bybit（公開全幣，hourly_rate 為每小時小數）
        try:
            info = await self._fetch_bybit_borrow_rates()
            b = info.get(coin)
            if b and b.get("hourly_rate") is not None:
                out["bybit"] = round(b["hourly_rate"] * 24 * 365 * 100, 4)
        except Exception as e:
            logger.debug(f"Bybit 借貸利率查詢失敗 {coin}: {e}")
        return {"coin": coin, "annual_pct": out}

    async def _fetch_bybit_borrowed_amounts(self) -> dict[str, float]:
        """查詢帳戶各幣種的已借出數量（UTA wallet-balance）"""
        borrowed = {}  # currency -> borrowed_amount
        if not self._bybit_api_key or not self._bybit_api_secret:
            return borrowed
        try:
            url = "https://api.bybit.com/v5/account/wallet-balance"
            recv_window = "5000"
            timestamp = str(int(time.time() * 1000))
            params_str = "accountType=UNIFIED"
            sign = self._bybit_sign(params_str, timestamp, recv_window)
            headers = {
                "X-BAPI-API-KEY": self._bybit_api_key,
                "X-BAPI-SIGN": sign,
                "X-BAPI-TIMESTAMP": timestamp,
                "X-BAPI-RECV-WINDOW": recv_window,
            }
            async with make_session() as session:
                async with session.get(f"{url}?{params_str}", headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("retCode") == 0:
                            accounts = data.get("result", {}).get("list", [])
                            for acc in accounts:
                                for coin_info in acc.get("coin", []):
                                    coin_name = coin_info.get("coin", "")
                                    borrow_amt = float(coin_info.get("borrowAmount", "0") or "0")
                                    if borrow_amt > 0:
                                        borrowed[coin_name] = borrow_amt
            if borrowed:
                logger.info(f"Bybit 已借出幣種：{len(borrowed)} 個")
        except Exception as e:
            logger.warning(f"Bybit 已借出數量查詢失敗: {e}")
        return borrowed

    async def _fetch_bybit_margin_data(self, opportunities: list[dict]) -> dict[str, dict]:
        """批次查詢各幣種的借貸資訊（公開 + 帳戶可借額度 + 已借出數量）"""
        margin_map = {}  # base_coin -> {max_loan, borrowed, hourly_rate, max_borrowing_amount, borrowable}

        # 先取公開借貸資訊（不需認證，一次全拿）
        borrow_info = await self._fetch_bybit_borrow_rates()

        # 初始化所有幣種的公開資訊
        for o in opportunities:
            base = o["base_coin"]
            info = borrow_info.get(base, {})
            margin_map[base] = {
                "max_loan": None,
                "borrowed": 0,
                "hourly_rate": info.get("hourly_rate"),
                "max_borrowing_amount": info.get("max_borrowing_amount"),
                "borrowable": info.get("borrowable", False),
            }

        if not self._bybit_api_key or not self._bybit_api_secret:
            logger.warning("Bybit API Key 未設定，無法查詢即時可借額度")
            return margin_map

        # 同時查帳戶可借額度 + 已借出數量
        borrowed_amounts = await self._fetch_bybit_borrowed_amounts()
        for base, amt in borrowed_amounts.items():
            if base in margin_map:
                margin_map[base]["borrowed"] = amt

        # 查帳戶即時可借額度（只查有現貨的幣種，按費率排序取前 50）
        candidates = [o for o in opportunities if o["has_spot"]]
        candidates.sort(key=lambda x: abs(x["funding_rate"]), reverse=True)
        candidates = candidates[:50]

        if not candidates:
            return margin_map

        async with make_session() as session:
            sem = asyncio.Semaphore(5)

            async def _check_one(o):
                async with sem:
                    base = o["base_coin"]
                    result = await self._fetch_bybit_max_borrowable(session, base)
                    margin_map[base]["max_loan"] = result

            await asyncio.gather(*[_check_one(o) for o in candidates], return_exceptions=True)

        logger.info(f"Bybit 帳戶借幣額度查詢完成：{len([v for v in margin_map.values() if v.get('max_loan') is not None])}/{len(candidates)} 個幣種")
        return margin_map

    async def _update_bybit_spot_futures(self):
        """掃描後自動抓取 Bybit 現貨價格與借幣額度，計算期現套利機會"""
        if not self.last_result:
            return

        bybit_records = [
            r for r in self.last_result.records if r.exchange == "bybit"
        ]
        if not bybit_records:
            self.bybit_spot_futures = []
            return

        exchange = ccxt_async.bybit()
        scope_bybit_markets(exchange, "bybit")
        try:
            await exchange.load_markets()

            # 分類：有現貨 vs 無現貨
            spot_syms = []
            for r in bybit_records:
                spot_sym = r.symbol.replace(":USDT", "")
                if spot_sym in exchange.markets:
                    spot_syms.append(spot_sym)

            # 批次抓有現貨的 ticker
            tickers = {}
            if spot_syms:
                tickers = await exchange.fetch_tickers(spot_syms)

            opportunities = []
            for r in bybit_records:
                spot_sym = r.symbol.replace(":USDT", "")
                has_spot = spot_sym in exchange.markets
                ticker = tickers.get(spot_sym)
                base_coin = spot_sym.split("/")[0]

                interval_h = r.funding_interval_h or 8
                funding_income_pct = abs(r.funding_rate) * 100
                periods_per_year = 8760 / interval_h
                annual_income_pct = funding_income_pct * periods_per_year

                entry = {
                    "symbol": r.symbol,
                    "base_coin": base_coin,
                    "contract_name": f"{base_coin}USDT",
                    "mark_price": r.mark_price,
                    "funding_rate": r.funding_rate,
                    "funding_interval_h": interval_h,
                    "annual_rate": r.annual_rate,
                    "has_spot": has_spot,
                    "spot_bid": None,
                    "spot_ask": None,
                    "futures_bid": r.bid_price,
                    "futures_ask": r.ask_price,
                    "entry_spread_pct": None,
                    "exit_spread_pct": None,
                    "funding_income_pct": round(funding_income_pct, 6),
                    "annual_income_pct": round(annual_income_pct, 4),
                    "funding_time": r.funding_time.isoformat() if r.funding_time else None,
                    "margin_max_loan": None,
                    "margin_borrowed": 0,
                    "margin_hourly_rate": None,
                    "margin_max_borrowing_amount": None,
                    "margin_borrowable": False,
                }

                if has_spot and ticker:
                    spot_bid = ticker.get("bid")
                    spot_ask = ticker.get("ask")
                    entry["spot_bid"] = spot_bid
                    entry["spot_ask"] = spot_ask

                    if spot_bid and spot_ask and r.bid_price and r.ask_price:
                        if r.funding_rate > 0:
                            # 多現貨/空合約：建倉=現貨ask/合約bid-1，關倉=合約ask/現貨bid-1
                            entry["direction"] = "buy_spot_short_futures"
                            entry["entry_spread_pct"] = round(
                                (spot_ask / r.bid_price - 1) * 100, 6
                            )
                            entry["exit_spread_pct"] = round(
                                (r.ask_price / spot_bid - 1) * 100, 6
                            )
                        else:
                            # 空現貨/多合約：建倉=合約ask/現貨bid-1，關倉=現貨ask/合約bid-1
                            entry["direction"] = "short_spot_long_futures"
                            entry["entry_spread_pct"] = round(
                                (r.ask_price / spot_bid - 1) * 100, 6
                            )
                            entry["exit_spread_pct"] = round(
                                (spot_ask / r.bid_price - 1) * 100, 6
                            )
                else:
                    if r.funding_rate > 0:
                        entry["direction"] = "buy_spot_short_futures"
                    else:
                        entry["direction"] = "short_spot_long_futures"

                opportunities.append(entry)

            # 查詢借貸資訊（公開 + 帳戶）
            margin_data = await self._fetch_bybit_margin_data(opportunities)
            for o in opportunities:
                coin_data = margin_data.get(o["base_coin"])
                if coin_data:
                    if coin_data.get("max_loan") is not None:
                        o["margin_max_loan"] = round(coin_data["max_loan"], 8)
                    o["margin_borrowed"] = coin_data.get("borrowed", 0)
                    if coin_data.get("hourly_rate") is not None:
                        o["margin_hourly_rate"] = coin_data["hourly_rate"]
                    if coin_data.get("max_borrowing_amount") is not None:
                        o["margin_max_borrowing_amount"] = coin_data["max_borrowing_amount"]
                    o["margin_borrowable"] = coin_data.get("borrowable", False)

            opportunities.sort(key=lambda x: abs(x["funding_rate"]), reverse=True)
            self.bybit_spot_futures = opportunities
            has_spot_count = sum(1 for o in opportunities if o["has_spot"])
            logger.info(f"Bybit 期現更新完成：{len(opportunities)} 個幣種，{has_spot_count} 個有現貨配對")
        except Exception as e:
            logger.warning(f"Bybit 期現更新失敗: {e}")
        finally:
            await exchange.close()

    # ---------------- Bitget 期現（全倉保證金借貸） ----------------

    def _bitget_sign(self, prehash: str) -> str:
        """Bitget V2 簽名：HMAC-SHA256(secret, prehash) 後 base64。
        prehash = timestamp + 'GET' + requestPath + '?' + queryString
        """
        mac = hmac.new(
            self._bitget_api_secret.encode("utf-8"),
            prehash.encode("utf-8"),
            hashlib.sha256,
        )
        return base64.b64encode(mac.digest()).decode("utf-8")

    async def _bitget_signed_get(self, session: aiohttp.ClientSession, path: str, params: dict | None = None):
        """對 Bitget 私有 GET 端點發簽名請求，成功回 data，失敗回 None。"""
        params = params or {}
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        timestamp = str(int(time.time() * 1000))
        prehash = f"{timestamp}GET{path}" + (f"?{qs}" if qs else "")
        headers = {
            "ACCESS-KEY": self._bitget_api_key,
            "ACCESS-SIGN": self._bitget_sign(prehash),
            "ACCESS-TIMESTAMP": timestamp,
            "ACCESS-PASSPHRASE": self._bitget_passphrase,
            "Content-Type": "application/json",
            "locale": "en-US",
        }
        url = f"https://api.bitget.com{path}" + (f"?{qs}" if qs else "")
        try:
            async with session.get(url, headers=headers) as resp:
                data = await resp.json()
                if data.get("code") == "00000":
                    return data.get("data")
                logger.debug(f"Bitget {path} 回傳非成功: {data.get('code')} {data.get('msg')}")
        except Exception as e:
            logger.debug(f"Bitget {path} 失敗: {e}")
        return None

    async def _bitget_signed_post(self, session: aiohttp.ClientSession, path: str, body: dict):
        """對 Bitget 私有 POST 端點發簽名請求，成功回 data，失敗回 None。
        POST 簽名 prehash = timestamp + 'POST' + requestPath + bodyJson（送出 body 須與簽名逐字相同）。
        """
        body_str = json.dumps(body)
        timestamp = str(int(time.time() * 1000))
        prehash = f"{timestamp}POST{path}{body_str}"
        headers = {
            "ACCESS-KEY": self._bitget_api_key,
            "ACCESS-SIGN": self._bitget_sign(prehash),
            "ACCESS-TIMESTAMP": timestamp,
            "ACCESS-PASSPHRASE": self._bitget_passphrase,
            "Content-Type": "application/json",
            "locale": "en-US",
        }
        url = f"https://api.bitget.com{path}"
        try:
            async with session.post(url, headers=headers, data=body_str) as resp:
                data = await resp.json()
                if data.get("code") == "00000":
                    return data.get("data")
                logger.debug(f"Bitget {path} 回傳非成功: {data.get('code')} {data.get('msg')}")
        except Exception as e:
            logger.debug(f"Bitget {path} 失敗: {e}")
        return None

    async def _fetch_bitget_max_open_sell(self, session: aiohttp.ClientSession, symbol: str, price) -> float | None:
        """查詢統一帳戶某合約「放空（sell）」的即時最大可開量（含可借量，幣本位）。
        端點：/api/v3/account/max-open-available（POST），category=MARGIN。
        maxOpen 即帳戶當下還能借多少（額度用完會回 0），比靜態 limit 更貼近實況。
        """
        body = {"category": "MARGIN", "symbol": symbol, "side": "sell"}
        if price:
            body["orderType"] = "limit"
            body["price"] = str(price)
        else:
            body["orderType"] = "market"
        data = await self._bitget_signed_post(session, "/api/v3/account/max-open-available", body)
        if not isinstance(data, dict):
            return None
        v = data.get("maxOpen", "")
        try:
            return float(v) if v not in (None, "") else None
        except (TypeError, ValueError):
            return None

    async def _fetch_bitget_margin_loan(self, session: aiohttp.ClientSession, coin: str) -> dict | None:
        """查詢某幣種統一帳戶保證金借貸利率 + 平台可借上限（公開端點，免簽名）。
        端點：/api/v3/market/margin-loans?coin=COIN
        回傳 annualInterest（年化小數，如 0.0117 = 1.17%）、dailyInterest、limit（平台最大可借，幣本位）。
        """
        url = "https://api.bitget.com/api/v3/market/margin-loans"
        try:
            async with session.get(url, params={"coin": coin}) as resp:
                data = await resp.json()
                if data.get("code") != "00000":
                    return None
                d = data.get("data") or {}
                annual_str = d.get("annualInterest", "")
                limit_str = d.get("limit", "")
                try:
                    annual = float(annual_str) if annual_str else None
                except (TypeError, ValueError):
                    annual = None
                try:
                    limit = float(limit_str) if limit_str else None
                except (TypeError, ValueError):
                    limit = None
                # 利率/額度都查得到視為可借
                borrowable = annual is not None and (limit is None or limit > 0)
                return {"annual_rate": annual, "limit": limit, "borrowable": borrowable}
        except Exception as e:
            logger.debug(f"Bitget margin-loans {coin} 失敗: {e}")
        return None

    async def _fetch_bitget_account_assets(self, session: aiohttp.ClientSession) -> tuple[float | None, dict[str, float]]:
        """查詢統一帳戶有效權益 + 各幣已借數量（認證 API，一次全拿）。
        端點：/api/v3/account/assets，已借在每幣 debt 欄位，帳戶可借基準用 effEquity（USDT）。
        """
        eff_equity = None
        debts = {}
        try:
            data = await self._bitget_signed_get(session, "/api/v3/account/assets")
            if not isinstance(data, dict):
                return eff_equity, debts
            try:
                eff_equity = float(data.get("effEquity", "") or 0) or None
            except (TypeError, ValueError):
                eff_equity = None
            for item in data.get("assets") or []:
                if not isinstance(item, dict):
                    continue
                coin = item.get("coin", "")
                try:
                    debt = float(item.get("debt", "0") or "0")
                except (TypeError, ValueError):
                    debt = 0
                if coin and debt > 0:
                    debts[coin] = debt
            if debts:
                logger.info(f"Bitget 已借出幣種：{len(debts)} 個")
        except Exception as e:
            logger.warning(f"Bitget 帳戶資產查詢失敗: {e}")
        return eff_equity, debts

    async def _fetch_bitget_margin_data(self, opportunities: list[dict]) -> dict[str, dict]:
        """批次查詢各幣種的借貸資訊（利率 + 即時可借額度 + 已借數量）。

        利率走公開 /api/v3/market/margin-loans；帳戶即時可借（放空可開量）走
        /api/v3/account/max-open-available（額度用完回 0，能即時反映該幣還剩多少能借）。
        只查有現貨、按費率排序前 50 個候選控制用量。
        margin_hourly_rate 統一存「每小時利率小數」沿用前端既有換算。
        """
        margin_map = {}
        for o in opportunities:
            margin_map[o["base_coin"]] = {
                "max_loan": None,
                "borrowed": 0,
                "hourly_rate": None,
                "max_borrowing_amount": None,
                "borrowable": False,
            }

        if not self._bitget_api_key or not self._bitget_api_secret or not self._bitget_passphrase:
            logger.warning("Bitget API Key 未設定，無法查詢借貸額度")
            return margin_map

        async with make_session() as session:
            # 一次拿各幣已借數量（debt）
            eff_equity, debts = await self._fetch_bitget_account_assets(session)
            for base, amt in debts.items():
                if base in margin_map:
                    margin_map[base]["borrowed"] = amt

            candidates = [o for o in opportunities if o["has_spot"]]
            candidates.sort(key=lambda x: abs(x["funding_rate"]), reverse=True)
            candidates = candidates[:50]
            if not candidates:
                return margin_map

            sem = asyncio.Semaphore(5)

            async def _check_one(o):
                base = o["base_coin"]
                price = o.get("mark_price")
                symbol = f"{base}USDT"
                async with sem:
                    ir = await self._fetch_bitget_margin_loan(session, base)
                    max_open = await self._fetch_bitget_max_open_sell(session, symbol, price)
                if ir and ir["annual_rate"] is not None:
                    margin_map[base]["hourly_rate"] = ir["annual_rate"] / (365 * 24)
                if ir:
                    margin_map[base]["max_borrowing_amount"] = ir["limit"]
                if max_open is not None:
                    margin_map[base]["max_loan"] = max_open
                # 即時還有可借量（maxOpen>0）才算可空
                margin_map[base]["borrowable"] = bool(max_open and max_open > 0)

            await asyncio.gather(*[_check_one(o) for o in candidates], return_exceptions=True)

        done = len([v for v in margin_map.values() if v.get("max_loan") is not None])
        logger.info(f"Bitget 借貸資訊查詢完成：{done}/{len(candidates)} 個幣種（effEquity={eff_equity}）")
        return margin_map

    async def _update_bitget_spot_futures(self):
        """掃描後自動抓取 Bitget 現貨價格與借幣額度，計算期現套利機會"""
        if not self.last_result:
            return

        bitget_records = [
            r for r in self.last_result.records if r.exchange == "bitget"
        ]
        if not bitget_records:
            self.bitget_spot_futures = []
            return

        exchange = ccxt_async.bitget()
        try:
            await exchange.load_markets()

            # 分類：有現貨 vs 無現貨
            spot_syms = []
            for r in bitget_records:
                spot_sym = r.symbol.replace(":USDT", "")
                if spot_sym in exchange.markets:
                    spot_syms.append(spot_sym)

            # 批次抓有現貨的 ticker
            tickers = {}
            if spot_syms:
                tickers = await exchange.fetch_tickers(spot_syms)

            opportunities = []
            for r in bitget_records:
                spot_sym = r.symbol.replace(":USDT", "")
                has_spot = spot_sym in exchange.markets
                ticker = tickers.get(spot_sym)
                base_coin = spot_sym.split("/")[0]

                interval_h = r.funding_interval_h or 8
                funding_income_pct = abs(r.funding_rate) * 100
                periods_per_year = 8760 / interval_h
                annual_income_pct = funding_income_pct * periods_per_year

                entry = {
                    "symbol": r.symbol,
                    "base_coin": base_coin,
                    "contract_name": f"{base_coin}USDT",
                    "mark_price": r.mark_price,
                    "funding_rate": r.funding_rate,
                    "funding_interval_h": interval_h,
                    "annual_rate": r.annual_rate,
                    "has_spot": has_spot,
                    "spot_bid": None,
                    "spot_ask": None,
                    "futures_bid": r.bid_price,
                    "futures_ask": r.ask_price,
                    "entry_spread_pct": None,
                    "exit_spread_pct": None,
                    "funding_income_pct": round(funding_income_pct, 6),
                    "annual_income_pct": round(annual_income_pct, 4),
                    "funding_time": r.funding_time.isoformat() if r.funding_time else None,
                    "margin_max_loan": None,
                    "margin_borrowed": 0,
                    "margin_hourly_rate": None,
                    "margin_max_borrowing_amount": None,
                    "margin_borrowable": False,
                }

                if has_spot and ticker:
                    spot_bid = ticker.get("bid")
                    spot_ask = ticker.get("ask")
                    entry["spot_bid"] = spot_bid
                    entry["spot_ask"] = spot_ask

                    if spot_bid and spot_ask and r.bid_price and r.ask_price:
                        if r.funding_rate > 0:
                            # 多現貨/空合約：建倉=現貨ask/合約bid-1，關倉=合約ask/現貨bid-1
                            entry["direction"] = "buy_spot_short_futures"
                            entry["entry_spread_pct"] = round(
                                (spot_ask / r.bid_price - 1) * 100, 6
                            )
                            entry["exit_spread_pct"] = round(
                                (r.ask_price / spot_bid - 1) * 100, 6
                            )
                        else:
                            # 空現貨/多合約：建倉=合約ask/現貨bid-1，關倉=現貨ask/合約bid-1
                            entry["direction"] = "short_spot_long_futures"
                            entry["entry_spread_pct"] = round(
                                (r.ask_price / spot_bid - 1) * 100, 6
                            )
                            entry["exit_spread_pct"] = round(
                                (spot_ask / r.bid_price - 1) * 100, 6
                            )
                else:
                    if r.funding_rate > 0:
                        entry["direction"] = "buy_spot_short_futures"
                    else:
                        entry["direction"] = "short_spot_long_futures"

                opportunities.append(entry)

            # 查詢借貸資訊（利率 + 帳戶額度）
            margin_data = await self._fetch_bitget_margin_data(opportunities)
            for o in opportunities:
                coin_data = margin_data.get(o["base_coin"])
                if coin_data:
                    if coin_data.get("max_loan") is not None:
                        o["margin_max_loan"] = round(coin_data["max_loan"], 8)
                    o["margin_borrowed"] = coin_data.get("borrowed", 0)
                    if coin_data.get("hourly_rate") is not None:
                        o["margin_hourly_rate"] = coin_data["hourly_rate"]
                    if coin_data.get("max_borrowing_amount") is not None:
                        o["margin_max_borrowing_amount"] = coin_data["max_borrowing_amount"]
                    o["margin_borrowable"] = coin_data.get("borrowable", False)

            opportunities.sort(key=lambda x: abs(x["funding_rate"]), reverse=True)
            self.bitget_spot_futures = opportunities
            has_spot_count = sum(1 for o in opportunities if o["has_spot"])
            logger.info(f"Bitget 期現更新完成：{len(opportunities)} 個幣種，{has_spot_count} 個有現貨配對")
        except Exception as e:
            logger.warning(f"Bitget 期現更新失敗: {e}")
        finally:
            await exchange.close()

    # ---------------- 通用期現建構 + 其他交易所借貸 ----------------

    async def _build_spot_futures_base(self, exchange_id: str) -> list[dict]:
        """通用：用掃描結果 + 現貨 ticker 算出某所期現機會（不含借貸欄位）。
        現貨實例沿用 _spot_exchanges（已載入 markets），未就緒則回空。
        """
        if not self.last_result:
            return []
        records = [r for r in self.last_result.records if r.exchange == exchange_id]
        if not records:
            return []
        spot_ex = self._spot_exchanges.get(exchange_id)
        markets = spot_ex.markets if spot_ex else {}
        spot_syms = [s for s in (r.symbol.replace(":USDT", "") for r in records) if s in markets]
        tickers = {}
        if spot_ex and spot_syms:
            try:
                tickers = await spot_ex.fetch_tickers(spot_syms)
            except Exception as e:
                logger.debug(f"[{exchange_id}] 現貨 ticker 失敗: {e}")

        opps = []
        for r in records:
            spot_sym = r.symbol.replace(":USDT", "")
            has_spot = spot_sym in markets
            ticker = tickers.get(spot_sym)
            base_coin = spot_sym.split("/")[0]
            interval_h = r.funding_interval_h or 8
            funding_income_pct = abs(r.funding_rate) * 100
            annual_income_pct = funding_income_pct * (8760 / interval_h)

            entry = {
                "symbol": r.symbol,
                "base_coin": base_coin,
                "contract_name": f"{base_coin}USDT",
                "mark_price": r.mark_price,
                "funding_rate": r.funding_rate,
                "funding_interval_h": interval_h,
                "annual_rate": r.annual_rate,
                "has_spot": has_spot,
                "spot_bid": None,
                "spot_ask": None,
                "futures_bid": r.bid_price,
                "futures_ask": r.ask_price,
                "entry_spread_pct": None,
                "exit_spread_pct": None,
                "funding_income_pct": round(funding_income_pct, 6),
                "annual_income_pct": round(annual_income_pct, 4),
                "funding_time": r.funding_time.isoformat() if r.funding_time else None,
                "margin_max_loan": None,
                "margin_borrowed": 0,
                "margin_hourly_rate": None,
                "margin_max_borrowing_amount": None,
                "margin_borrowable": False,
            }

            if has_spot and ticker:
                spot_bid = ticker.get("bid")
                spot_ask = ticker.get("ask")
                entry["spot_bid"] = spot_bid
                entry["spot_ask"] = spot_ask
                if spot_bid and spot_ask and r.bid_price and r.ask_price:
                    if r.funding_rate > 0:
                        entry["direction"] = "buy_spot_short_futures"
                        entry["entry_spread_pct"] = round((spot_ask / r.bid_price - 1) * 100, 6)
                        entry["exit_spread_pct"] = round((r.ask_price / spot_bid - 1) * 100, 6)
                    else:
                        entry["direction"] = "short_spot_long_futures"
                        entry["entry_spread_pct"] = round((r.ask_price / spot_bid - 1) * 100, 6)
                        entry["exit_spread_pct"] = round((spot_ask / r.bid_price - 1) * 100, 6)
            else:
                entry["direction"] = "buy_spot_short_futures" if r.funding_rate > 0 else "short_spot_long_futures"
            opps.append(entry)
        return opps

    @staticmethod
    def _margin_candidates(opps: list[dict], top_n: int = 40) -> list[dict]:
        c = [o for o in opps if o["has_spot"]]
        c.sort(key=lambda x: abs(x["funding_rate"]), reverse=True)
        return c[:top_n]

    # --- OKX ---
    def _okx_signed_headers(self, method: str, path: str, body: str = "") -> dict:
        t = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        msg = f"{t}{method}{path}{body}"
        sign = base64.b64encode(
            hmac.new(self._okx_api.get("secret", "").encode(), msg.encode(), hashlib.sha256).digest()
        ).decode()
        return {
            "OK-ACCESS-KEY": self._okx_api.get("apiKey", ""),
            "OK-ACCESS-SIGN": sign,
            "OK-ACCESS-TIMESTAMP": t,
            "OK-ACCESS-PASSPHRASE": self._okx_api.get("password", ""),
            "Content-Type": "application/json",
        }

    async def _okx_get(self, session: aiohttp.ClientSession, path: str):
        try:
            async with session.get("https://www.okx.com" + path, headers=self._okx_signed_headers("GET", path)) as r:
                d = await r.json()
                if d.get("code") == "0":
                    return d.get("data")
        except Exception as e:
            logger.debug(f"OKX {path} 失敗: {e}")
        return None

    async def _update_okx_spot_futures(self):
        opps = await self._build_spot_futures_base("okx")
        if not opps:
            self.okx_spot_futures = []
            return
        if self._okx_api.get("apiKey"):
            try:
                async with make_session() as session:
                    # 利率：一次抓全幣（interestRate 為每小時小數）
                    rate_map = {}
                    data = await self._okx_get(session, "/api/v5/account/interest-rate")
                    for d in (data or []):
                        try:
                            rate_map[d["ccy"]] = float(d["interestRate"])
                        except (KeyError, TypeError, ValueError):
                            pass
                    # 借貸配額上限（bulk，供參考；margin_max_loan 用真實 max-loan）
                    quota_map = {}
                    il = await self._okx_get(session, "/api/v5/account/interest-limits?type=2")
                    if isinstance(il, list) and il:
                        for r in (il[0].get("records") or []):
                            try:
                                quota_map[r["ccy"]] = float(r.get("loanQuota") or 0)
                            except (KeyError, TypeError, ValueError):
                                pass
                    for o in opps:
                        hr = rate_map.get(o["base_coin"])
                        if hr is not None:
                            o["margin_hourly_rate"] = hr
                        cap = quota_map.get(o["base_coin"])
                        if cap is not None:
                            o["margin_max_borrowing_amount"] = cap
                    # 帳戶即時可借（max-loan sell 側=可借該幣，與 OKX 網頁/APP 一致）
                    sem = asyncio.Semaphore(5)

                    async def one(o):
                        async with sem:
                            path = f"/api/v5/account/max-loan?instId={o['base_coin']}-USDT&mgnMode=cross"
                            rows = await self._okx_get(session, path)
                            for row in (rows or []):
                                if row.get("side") == "sell":
                                    try:
                                        ml = float(row.get("maxLoan") or 0)
                                        o["margin_max_loan"] = ml
                                        o["margin_borrowable"] = ml > 0
                                    except (TypeError, ValueError):
                                        pass
                    await asyncio.gather(*[one(o) for o in self._margin_candidates(opps)], return_exceptions=True)
            except Exception as e:
                logger.warning(f"OKX 借貸查詢失敗: {e}")
        opps.sort(key=lambda x: abs(x["funding_rate"]), reverse=True)
        self.okx_spot_futures = opps
        logger.info(f"OKX 期現更新完成：{len(opps)} 個幣種")

    # --- Binance ---
    async def _bn_get(self, session: aiohttp.ClientSession, path: str, params: dict):
        params = dict(params)
        params["timestamp"] = int(time.time() * 1000)
        q = "&".join(f"{k}={v}" for k, v in params.items())
        sig = hmac.new(self._binance_api.get("secret", "").encode(), q.encode(), hashlib.sha256).hexdigest()
        url = f"https://api.binance.com{path}?{q}&signature={sig}"
        try:
            async with session.get(url, headers={"X-MBX-APIKEY": self._binance_api.get("apiKey", "")}) as r:
                if r.status == 200:
                    return await r.json()
        except Exception as e:
            logger.debug(f"Binance {path} 失敗: {e}")
        return None

    async def _update_binance_spot_futures(self):
        opps = await self._build_spot_futures_base("binance")
        if not opps:
            self.binance_spot_futures = []
            return
        if self._binance_api.get("apiKey"):
            try:
                async with make_session() as session:
                    # 資金池剩餘可借（一次全幣）
                    pool = {}
                    inv = await self._bn_get(session, "/sapi/v1/margin/available-inventory", {"type": "MARGIN"})
                    if isinstance(inv, dict):
                        for c, v in (inv.get("assets") or {}).items():
                            try:
                                pool[c] = float(v)
                            except (TypeError, ValueError):
                                pass
                    # 借貸上限 + 利率（一次全幣，crossMarginData 依帳戶 VIP 等級回傳）
                    limits, rates = {}, {}
                    cmd = await self._bn_get(session, "/sapi/v1/margin/crossMarginData", {})
                    if isinstance(cmd, list):
                        for it in cmd:
                            c = it.get("coin")
                            if not c:
                                continue
                            try:
                                limits[c] = float(it.get("borrowLimit") or 0)
                            except (TypeError, ValueError):
                                pass
                            try:
                                rates[c] = float(it.get("dailyInterest")) / 24  # 日利率→每小時
                            except (TypeError, ValueError):
                                pass
                    for o in opps:
                        base = o["base_coin"]
                        if rates.get(base) is not None:
                            o["margin_hourly_rate"] = rates[base]
                        p, lim = pool.get(base), limits.get(base)
                        if lim is not None:
                            o["margin_max_borrowing_amount"] = lim
                        if p is not None:
                            o["margin_pool_available"] = p
                        # 可借額度 = min(資金池剩餘, 借貸上限)
                        vals = [x for x in (p, lim) if x is not None]
                        if vals:
                            o["margin_max_loan"] = min(vals)
                            o["margin_borrowable"] = min(vals) > 0
            except Exception as e:
                logger.warning(f"Binance 借貸查詢失敗: {e}")
        opps.sort(key=lambda x: abs(x["funding_rate"]), reverse=True)
        self.binance_spot_futures = opps
        logger.info(f"Binance 期現更新完成：{len(opps)} 個幣種")

    # --- Gate（利率 estimate_rate + 帳戶即時可借 cross/borrowable，皆需簽名） ---
    async def _gate_get(self, session: aiohttp.ClientSession, path: str, params: dict | None = None):
        params = params or {}
        query = "&".join(f"{k}={v}" for k, v in params.items())
        t = str(int(time.time()))
        body_hash = hashlib.sha512(b"").hexdigest()
        sign_str = f"GET\n{path}\n{query}\n{body_hash}\n{t}"
        sign = hmac.new(self._gateio_api.get("secret", "").encode(), sign_str.encode(), hashlib.sha512).hexdigest()
        headers = {"KEY": self._gateio_api.get("apiKey", ""), "Timestamp": t, "SIGN": sign, "Accept": "application/json"}
        url = "https://api.gateio.ws" + path + (f"?{query}" if query else "")
        try:
            async with session.get(url, headers=headers) as r:
                if r.status == 200:
                    return await r.json()
                logger.debug(f"Gate {path} 回傳 {r.status}")
        except Exception as e:
            logger.debug(f"Gate {path} 失敗: {e}")
        return None

    async def _update_gateio_spot_futures(self):
        opps = await self._build_spot_futures_base("gateio")
        if not opps:
            self.gateio_spot_futures = []
            return
        try:
            cands = self._margin_candidates(opps, 60)
            async with make_session() as session:
                # 逐幣抓利率 + 帳戶即時可借（estimate_rate 批次含任一不支援幣就整批 400，故逐幣）
                sem = asyncio.Semaphore(8)

                async def one(o):
                    base = o["base_coin"]
                    async with sem:
                        rate = await self._gate_get(session, "/api/v4/margin/uni/estimate_rate", {"currencies": base})
                        if isinstance(rate, dict) and base in rate:
                            try:
                                o["margin_hourly_rate"] = float(rate[base])
                            except (TypeError, ValueError):
                                pass
                        # 帳戶即時可借（cross margin，回該幣可借數量）
                        bor = await self._gate_get(session, "/api/v4/margin/cross/borrowable", {"currency": base})
                        if isinstance(bor, dict) and bor.get("amount") is not None:
                            try:
                                amt = float(bor["amount"])
                                o["margin_max_loan"] = amt
                                o["margin_borrowable"] = amt > 0
                            except (TypeError, ValueError):
                                pass
                await asyncio.gather(*[one(o) for o in cands], return_exceptions=True)
        except Exception as e:
            logger.warning(f"Gate 借貸查詢失敗: {e}")
        opps.sort(key=lambda x: abs(x["funding_rate"]), reverse=True)
        self.gateio_spot_futures = opps
        logger.info(f"Gate 期現更新完成：{len(opps)} 個幣種")

    # --- MEXC / BingX（無現貨保證金借貸 API，僅公開欄位） ---
    async def _update_mexc_spot_futures(self):
        opps = await self._build_spot_futures_base("mexc")
        opps.sort(key=lambda x: abs(x["funding_rate"]), reverse=True)
        self.mexc_spot_futures = opps
        if opps:
            logger.info(f"MEXC 期現更新完成：{len(opps)} 個幣種（無借貸）")

    async def _update_bingx_spot_futures(self):
        opps = await self._build_spot_futures_base("bingx")
        opps.sort(key=lambda x: abs(x["funding_rate"]), reverse=True)
        self.bingx_spot_futures = opps
        if opps:
            logger.info(f"BingX 期現更新完成：{len(opps)} 個幣種（無借貸）")

    async def _update_all_other_spot_futures(self):
        """統一觸發 OKX/Binance/Gate/MEXC/BingX 期現更新（各自獨立、互不阻塞）。"""
        await asyncio.gather(
            self._update_okx_spot_futures(),
            self._update_binance_spot_futures(),
            self._update_gateio_spot_futures(),
            self._update_mexc_spot_futures(),
            self._update_bingx_spot_futures(),
            return_exceptions=True,
        )

    def _load_rate_history(self):
        """從檔案載入費率歷史快取"""
        try:
            if not RATE_HISTORY_PATH.exists():
                return
            with open(RATE_HISTORY_PATH, "r", encoding="utf-8") as f:
                self._rate_history = json.load(f)
            # 清理超過 72h 的舊資料
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=72)).isoformat()
            now_iso = datetime.now(timezone.utc).isoformat()
            before = len(self._rate_history)
            seen = set()
            cleaned = []
            for r in self._rate_history:
                if r["ts"] <= cutoff:
                    continue
                try:
                    dt = datetime.fromisoformat(r["ts"])
                    # 對齊到整點（去掉秒和微秒）
                    dt = dt.replace(second=0, microsecond=0)
                    r["ts"] = dt.isoformat()
                    # 清理非整點（掃描快照）+ 未來時間戳（未結算）
                    if dt.minute != 0 or r["ts"] > now_iso:
                        continue
                    # 去重（同一時間+交易所+幣種只保留一筆）
                    key = (r["ts"], r["ex"], r["sym"])
                    if key in seen:
                        continue
                    seen.add(key)
                except Exception:
                    continue
                cleaned.append(r)
            self._rate_history = cleaned
            purged = before - len(cleaned)
            logger.info(f"載入費率歷史：{len(self._rate_history)} 筆" + (f"（清除 {purged} 筆非整點記錄）" if purged else ""))
            if purged:
                self._save_rate_history()
        except Exception as e:
            logger.warning(f"載入費率歷史失敗: {e}")
            self._rate_history = []

    def _save_rate_history(self):
        """將費率歷史存入檔案"""
        try:
            with open(RATE_HISTORY_PATH, "w", encoding="utf-8") as f:
                json.dump(self._rate_history, f)
        except Exception as e:
            logger.warning(f"儲存費率歷史失敗: {e}")

    @staticmethod
    def _index_day_file(day: str) -> Path:
        """某日的指數偏離 JSONL 檔路徑（day = 'YYYYMMDD'，UTC）"""
        return INDEX_HISTORY_DIR / f"{day}.jsonl"

    @staticmethod
    def _index_keep_days() -> list[str]:
        """近 INDEX_HISTORY_RETAIN_DAYS 天的日期字串（舊→新，UTC）"""
        today = datetime.now(timezone.utc)
        return sorted((today - timedelta(days=d)).strftime("%Y%m%d")
                      for d in range(INDEX_HISTORY_RETAIN_DAYS))

    @staticmethod
    def _dev_push_to(devs: dict, key, dev):
        """把一個 dev 追加到 {key: array('f')} 並裁到最近 INDEX_DEV_WINDOW 個（float32，每點 4 bytes）。"""
        arr = devs.get(key)
        if arr is None:
            arr = array("f")
            devs[key] = arr
        arr.append(dev)
        if len(arr) > INDEX_DEV_WINDOW:
            del arr[0]

    def _rebuild_index_devs_from_disk(self) -> dict:
        """串流讀近 3 天按日 JSONL，直接建 {(幣,所): float32 陣列(最近 window 個 dev)}，不整檔載入
        （即使檔案到 GB 級，RAM 也維持低檔）。同步、不碰 self，供 thread 執行。"""
        devs: dict[tuple[str, str], array] = {}
        if not INDEX_HISTORY_DIR.exists():
            return devs
        for day in self._index_keep_days():
            fp = self._index_day_file(day)
            if not fp.exists():
                continue
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            h = json.loads(line)
                            key = (h["sym"], h["ex"])
                            dev = h["dev"]
                        except (json.JSONDecodeError, KeyError, TypeError):
                            continue  # 壞行/缺欄只略過該行
                        self._dev_push_to(devs, key, dev)
            except Exception as e:
                logger.warning(f"重建指數偏離序列讀 {fp.name} 失敗: {e}")
        return devs

    def _append_index_batch(self, batch: list[dict]):
        """把本輪批次以 JSONL 追加到當日檔（只追加、永不重寫整檔，杜絕整檔損毀）。"""
        if not batch:
            return
        try:
            INDEX_HISTORY_DIR.mkdir(parents=True, exist_ok=True)
            day = datetime.now(timezone.utc).strftime("%Y%m%d")
            with open(self._index_day_file(day), "a", encoding="utf-8") as f:
                for rec in batch:
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.warning(f"寫入指數偏離 JSONL 失敗: {e}")

    def _prune_index_day_files(self):
        """刪除超過保留天數的按日 JSONL 檔。"""
        try:
            if not INDEX_HISTORY_DIR.exists():
                return
            keep = set(self._index_keep_days())
            for fp in INDEX_HISTORY_DIR.glob("*.jsonl"):
                if fp.stem not in keep:
                    fp.unlink(missing_ok=True)
        except Exception as e:
            logger.warning(f"清理指數偏離舊檔失敗: {e}")

    def read_index_history_api(self, symbol: str, exchange: str = "") -> list[dict]:
        """供 API：串流讀近 3 天按日 JSONL、只留符合的單幣記錄（不整檔載入，RAM 低）。
        先用便宜的字串包含快篩，再對命中行 json 解析，避免對整個大檔逐行解析。低頻、單一 symbol。"""
        ex = exchange.lower() if exchange else ""
        out: list[dict] = []
        if not INDEX_HISTORY_DIR.exists():
            return out
        for day in self._index_keep_days():
            fp = self._index_day_file(day)
            if not fp.exists():
                continue
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    for line in f:
                        if symbol not in line:  # 便宜快篩：不含 symbol 字串就跳過，不解析
                            continue
                        try:
                            h = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if h.get("sym") != symbol:
                            continue
                        if ex and h.get("ex") != ex:
                            continue
                        out.append(h)
            except Exception as e:
                logger.warning(f"讀取指數偏離 JSONL {fp.name} 失敗: {e}")
        return out

    async def _load_index_history_async(self):
        """背景載入：從按日 JSONL 重建 RAM 的 _index_devs（每 (幣,所) 最近 window 個 dev）。
        JSONL 只追加、壞行自動略過，故不再有整檔損毀/自癒問題。"""
        try:
            rebuilt = await asyncio.to_thread(self._rebuild_index_devs_from_disk)
            # 載入期間掃描可能已往 _index_devs append（較新）；接在磁碟歷史之後，不覆蓋
            for key, arr in self._index_devs.items():
                for v in arr:
                    self._dev_push_to(rebuilt, key, v)
            self._index_devs = rebuilt
            self._prune_index_day_files()
            logger.info(f"背景載入指數偏離歷史完成：{len(self._index_devs)} 個 (幣,所) 序列")
            self._index_history_ready = True
            # 一次性：把舊的單檔格式移到旁邊（新格式改用 index_history/ 目錄的按日 JSONL）
            try:
                if INDEX_HISTORY_PATH.exists():
                    INDEX_HISTORY_PATH.replace(
                        INDEX_HISTORY_PATH.with_name(INDEX_HISTORY_PATH.name + ".pre-jsonl"))
            except Exception:
                pass
        except Exception as e:
            logger.warning(f"載入指數偏離歷史失敗，本 session 暫停寫 JSONL: {e}")

    # ── 指數成分追蹤 ──────────────────────────────────────────

    def _load_constituent_snapshot(self):
        """從檔案載入指數成分快照，並合併因新增別名而分散的同幣 key"""
        try:
            if not CONSTITUENT_SNAPSHOT_PATH.exists():
                return
            with open(CONSTITUENT_SNAPSHOT_PATH, "r", encoding="utf-8") as f:
                self._constituent_snapshot = json.load(f)

            # 合併別名分散的舊 key（例 alias 加入後 TSTBSC/USDT:USDT 應併入 TST/USDT:USDT）
            merged = 0
            for key in list(self._constituent_snapshot.keys()):
                normalized = _normalize_symbol(key)
                if normalized == key:
                    continue
                src_data = self._constituent_snapshot.pop(key)
                dst = self._constituent_snapshot.setdefault(normalized, {})
                for k, v in src_data.items():
                    dst.setdefault(k, v)  # 已有 normalized 資料時不覆蓋
                merged += 1
            if merged:
                logger.info(f"指數成分快照合併別名 key：{merged} 個")
                self._save_constituent_snapshot()

            logger.info(f"載入指數成分快照：{len(self._constituent_snapshot)} 個幣種")
        except Exception as e:
            logger.warning(f"載入指數成分快照失敗: {e}")
            self._constituent_snapshot = {}

    def _save_constituent_snapshot(self):
        """將指數成分快照存入檔案"""
        try:
            with open(CONSTITUENT_SNAPSHOT_PATH, "w", encoding="utf-8") as f:
                json.dump(self._constituent_snapshot, f)
        except Exception as e:
            logger.warning(f"儲存指數成分快照失敗: {e}")

    def _load_constituent_changes(self):
        """從檔案載入成分變更歷史（保留 7 天）"""
        try:
            if not CONSTITUENT_CHANGES_PATH.exists():
                return
            with open(CONSTITUENT_CHANGES_PATH, "r", encoding="utf-8") as f:
                self._constituent_changes = json.load(f)
            cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
            self._constituent_changes = [c for c in self._constituent_changes if c["ts"] > cutoff]
            logger.info(f"載入成分變更歷史：{len(self._constituent_changes)} 筆")
        except Exception as e:
            logger.warning(f"載入成分變更歷史失敗: {e}")
            self._constituent_changes = []

    def _save_constituent_changes(self):
        """將成分變更歷史存入檔案"""
        try:
            with open(CONSTITUENT_CHANGES_PATH, "w", encoding="utf-8") as f:
                json.dump(self._constituent_changes, f)
        except Exception as e:
            logger.warning(f"儲存成分變更歷史失敗: {e}")

    # 成分交易所名稱別名對照（只合併明確是同一交易所改名/縮寫的）
    _CONSTITUENT_EXCHANGE_ALIASES = {
        "mxc": "mexc",
        "gate": "gateio",
        "okex": "okx",
        "pancakev3": "pancakeswapv3",
        "binance_future": "binance_futures",
        "binancealpha": "binance_alpha",
        "binanceticker": "binance",
        "binance_linear_perpetual": "binance_futures",
    }

    @staticmethod
    def _norm_constituent_exchange(name: str) -> str:
        raw = str(name).lower()
        return FundingScanner._CONSTITUENT_EXCHANGE_ALIASES.get(raw, raw)

    @staticmethod
    def _normalize_constituents(raw_list: list[dict], source: str) -> list[dict]:
        """正規化不同交易所的成分格式為統一結構"""
        result = []
        _norm = FundingScanner._norm_constituent_exchange
        # Gate 分支僅為「v4 退回路徑」：官方 v4 weight 為空字串，退回時假設等權重 1/N。
        # 真實權重（非等權，如 SKHYNIX Gate 96%）由 _fetch_gate_constituents_apiw 直接提供。
        n_constituents = len(raw_list)
        gate_equal_weight = (1.0 / n_constituents) if n_constituents else None
        for item in raw_list:
            if source == "binance":
                result.append({
                    "exchange": _norm(item.get("exchange", "")),
                    "weight": float(item.get("weight", 0)),
                    "price": float(item.get("price", 0)),
                })
            elif source == "bybit":
                result.append({
                    "exchange": _norm(item.get("exchange", "")),
                    "weight": float(item.get("weight", 0)),
                    "price": float(item.get("price", 0)),
                })
            elif source == "okx":
                result.append({
                    "exchange": _norm(item.get("exch", "")),
                    "weight": float(item.get("wgt", 0)),
                    "price": float(item.get("symPx", 0)),
                })
            elif source == "kucoinfutures":
                result.append({
                    "exchange": _norm(item.get("exchange", "")),
                    "weight": float(item.get("weight", 0)),
                    "price": float(item.get("price", 0)),
                })
            elif source == "gateio":
                result.append({
                    "exchange": _norm(item.get("exchange", "")),
                    "weight": gate_equal_weight,
                    "price": None,
                })
            elif source == "bitget":
                result.append({
                    "exchange": _norm(item.get("exchange", "")),
                    "weight": float(item.get("weight", 0)),
                    "price": float(item.get("equivalentPrice", 0)),
                })
            elif source == "mexc":
                result.append({
                    "exchange": _norm(item.get("exchange", "")),
                    "weight": None,
                    "price": None,
                })
        return result

    @staticmethod
    def _diff_constituents(old_list: list[dict], new_list: list[dict]) -> list[dict]:
        """比對兩次快照的差異，回傳變更列表"""
        old_map = {c["exchange"]: c for c in old_list}
        new_map = {c["exchange"]: c for c in new_list}
        changes = []

        # 被移除的成分交易所
        for ex in old_map:
            if ex not in new_map:
                changes.append({
                    "type": "removed",
                    "constituent_exchange": ex,
                    "old_weight": old_map[ex].get("weight"),
                    "new_weight": None,
                })

        # 新增的成分交易所
        for ex in new_map:
            if ex not in old_map:
                changes.append({
                    "type": "added",
                    "constituent_exchange": ex,
                    "old_weight": None,
                    "new_weight": new_map[ex].get("weight"),
                })

        # 權重變化（只比較兩者都有權重的情況）
        for ex in old_map:
            if ex in new_map:
                ow = old_map[ex].get("weight")
                nw = new_map[ex].get("weight")
                if ow is not None and nw is not None and abs(ow - nw) > 0.05:
                    changes.append({
                        "type": "weight_changed",
                        "constituent_exchange": ex,
                        "old_weight": round(ow, 6),
                        "new_weight": round(nw, 6),
                    })

        return changes

    async def _fetch_kucoin_index_symbol_map(self, session: aiohttp.ClientSession) -> dict:
        """批次取得 KuCoin 所有合約的 base→indexSymbol 映射，避免逐一查詢"""
        try:
            url = "https://api-futures.kucoin.com/api/v1/contracts/active"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    return {}
                data = await resp.json()
                mapping = {}
                for c in data.get("data", []):
                    base = c.get("baseCurrency", "")
                    idx_sym = c.get("indexSymbol", "")
                    if base and idx_sym:
                        mapping[base.upper()] = idx_sym
                return mapping
        except Exception as e:
            logger.debug(f"KuCoin 合約列表抓取失敗: {e}")
            return {}

    async def _fetch_constituents_batch(self, session: aiohttp.ClientSession,
                                         source: str, symbols: list[str]) -> dict:
        """從單一交易所批次抓取指數成分

        Returns: {symbol: [normalized_constituent, ...]}
        """
        result = {}
        batch_size = 10
        delay = 2.0
        if source == "okx":
            batch_size = 15
            delay = 2.5  # 20次/2秒限制

        # KuCoin：預先取得 base→indexSymbol 映射（一次 API call 取所有合約）
        kucoin_index_map = {}
        if source == "kucoinfutures":
            kucoin_index_map = await self._fetch_kucoin_index_symbol_map(session)

        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            tasks = []
            for sym in batch:
                tasks.append(self._fetch_one_constituent(session, source, sym, kucoin_index_map=kucoin_index_map))
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            for sym, resp in zip(batch, responses):
                if isinstance(resp, Exception):
                    continue
                if resp:
                    result[sym] = resp
            if i + batch_size < len(symbols):
                await asyncio.sleep(delay)

        return result

    async def _fetch_one_constituent(self, session: aiohttp.ClientSession,
                                      source: str, symbol: str,
                                      kucoin_index_map: dict | None = None) -> list[dict] | None:
        """從單一交易所抓取單一幣種的指數成分"""
        try:
            # 取得 base coin（如 BTC/USDT:USDT -> BTC）
            base = symbol.split("/")[0] if "/" in symbol else symbol

            if source == "binance":
                # 嘗試原始名稱與 1000 前綴
                symbols_to_try = [f"{base}USDT", f"1000{base}USDT"]
                for b_sym in symbols_to_try:
                    url = f"https://fapi.binance.com/fapi/v1/constituents?symbol={b_sym}"
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            raw = data.get("constituents", [])
                            return self._normalize_constituents(raw, "binance")
                return None

            elif source == "bybit":
                url = f"https://api.bybit.com/v5/market/index-price-components?indexName={base}USDT"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    raw = data.get("result", {}).get("components", [])
                    return self._normalize_constituents(raw, "bybit")

            elif source == "okx":
                url = f"https://www.okx.com/api/v5/market/index-components?index={base}-USDT"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    components = data.get("data", {})
                    if isinstance(components, list) and components:
                        components = components[0]
                    raw = components.get("components", [])
                    return self._normalize_constituents(raw, "okx")

            elif source == "kucoinfutures":
                # 從映射表取 indexSymbol（e.g. .KBTUSDT / .KMEGAUSDT）
                # KuCoin 用 XBT 代表 BTC
                _KU_BASE_ALIAS = {"BTC": "XBT"}
                lookup = _KU_BASE_ALIAS.get(base.upper(), base.upper())
                index_sym = (kucoin_index_map or {}).get(lookup)
                if not index_sym:
                    return None
                # 注意：KuCoin 的成分 API 是 /api/v1/index/query
                url = f"https://api-futures.kucoin.com/api/v1/index/query?symbol={index_sym}&startAt=0&maxCount=1"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    data_list = data.get("data", {}).get("dataList", [])
                    if not data_list:
                        return None
                    # KuCoin API 有個 typo：decomposionList（少一個 i）
                    # 或是 decompositionList (正確拼寫)
                    latest_data = data_list[0]
                    raw = latest_data.get("decompositionList") or latest_data.get("decomposionList", [])
                    return self._normalize_constituents(raw, "kucoinfutures")

            elif source == "gateio":
                # 官方 v4 index_constituents 的 weight 一律空字串 → 真實權重只在網頁 API
                # （Akamai 保護，需瀏覽器 TLS 指紋，用 curl_cffi 繞過）。失敗才退回 v4 等權重。
                apiw = await self._fetch_gate_constituents_apiw(base)
                if apiw is not None:
                    return apiw
                url = f"https://api.gateio.ws/api/v4/futures/usdt/index_constituents/{base}_USDT"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    raw = data.get("constituents", [])
                    return self._normalize_constituents(raw, "gateio")

            elif source == "bitget":
                url = f"https://api.bitget.com/api/v3/market/index-components?symbol={base}USDT"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    if data.get("code") != "00000":
                        return None
                    raw = data.get("data", {}).get("componentList", [])
                    return self._normalize_constituents(raw, "bitget")

            elif source == "mexc":
                url = f"https://contract.mexc.com/api/v1/contract/detail?symbol={base}_USDT"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    if not data.get("success"):
                        return None
                    origins = data.get("data", {}).get("indexOrigin", [])
                    # MEXC 只回傳名稱列表，沒有權重和價格，正規化時處理
                    raw = [{"exchange": o} for o in origins]
                    return self._normalize_constituents(raw, "mexc")

        except Exception as e:
            logger.debug(f"抓取 {source} {symbol} 成分失敗: {e}")
            return None

    async def _fetch_gate_constituents_apiw(self, base: str) -> list[dict] | None:
        """用 curl_cffi（瀏覽器 TLS 指紋）打 Gate 網頁 API 取指數成分「真實權重＋價格」。
        官方 v4 API 的 weight 一律空字串，此為唯一權重來源（如 SKHYNIX：Gate 自壓 96%）。
        Akamai 會擋一般 server-side 請求，故用 impersonate。失敗回 None（呼叫端退回 v4 等權重）。"""
        try:
            from curl_cffi.requests import AsyncSession
        except ImportError:
            return None
        url = (
            "https://www.gate.com/apiw/v2/futures/common/index/breakdown"
            f"?sub_website_id=0&index={base}_USDT"
        )
        try:
            async with AsyncSession() as s:
                r = await s.get(url, impersonate="chrome", timeout=12)
            if r.status_code != 200:
                return None
            d = r.json()
            if d.get("code") != 200:
                return None
            cons = (d.get("data") or {}).get("constituents") or []
            if not cons:
                return None
            result = []
            for c in cons:
                try:
                    w = float(c.get("weight") or 0)
                except (TypeError, ValueError):
                    w = 0.0
                try:
                    p = float(c.get("price") or 0) or None
                except (TypeError, ValueError):
                    p = None
                result.append({
                    "exchange": FundingScanner._norm_constituent_exchange(c.get("exchange", "")),
                    "weight": w,
                    "price": p,
                })
            return result or None
        except Exception as e:
            logger.debug(f"Gate apiw 成分抓取失敗 {base}: {e}")
            return None

    async def _update_constituents(self, index_tracking_snapshot: list = None, index_all_deviations_snapshot: list = None):
        """抓取所有交易所的指數成分，比對差異，更新快照"""
        if not self.last_result:
            return

        now_iso = datetime.now(timezone.utc).isoformat()

        # 收集需要查詢的幣種（從掃描結果中有 index_price 的）
        _CONSTITUENT_SOURCES = ("binance", "bybit", "okx", "kucoinfutures", "gateio", "bitget", "mexc")
        _MAX_PER_SOURCE = 60   # 每個交易所每輪最多查 60 個幣
        _REFRESH_HOURS = 24    # 快照超過幾小時才重查

        now_dt = datetime.now(timezone.utc)
        cutoff_iso = (now_dt - timedelta(hours=_REFRESH_HOURS)).isoformat()

        # all_by_source: {ex: [(fetch_sym, storage_sym), ...]}
        # fetch_sym = 該交易所原始 symbol（API 查詢用）；storage_sym = normalized（快照 key）
        all_by_source: dict[str, list[tuple[str, str]]] = {}
        for r in self.last_result.records:
            ex = r.exchange
            if ex not in _CONSTITUENT_SOURCES:
                continue
            storage_sym = r.normalized_symbol or r.symbol
            all_by_source.setdefault(ex, []).append((r.symbol, storage_sym))

        if not all_by_source:
            return

        # 剪枝：移除「本輪該來源交易所已不再上架此幣」的陳舊成分
        # （成分快照只增不減，交易所下架/改名後舊成分會永遠殘留，導致成分數 > 實際資費數）
        # 掃描端每個交易所是原子式：fetch 成功回全量、失敗整所 raise 被排除（scan_once），故無逐幣 partial fetch。
        # 守門：來源本輪整所失敗（不在 current）或掃到數量異常少（無聲半量回應）→ 跳過該所剪枝，避免誤刪。
        #   這 7 所每所都有數百永續，正常遠高於下限，閾值零誤判；只有掃描崩潰時才擋下。
        _PRUNE_MIN_SCAN = 30
        current_syms_by_source = {ex: {s for _, s in pairs} for ex, pairs in all_by_source.items()}
        suspect_sources = {
            s for s, syms in current_syms_by_source.items() if len(syms) < _PRUNE_MIN_SCAN
        }
        if suspect_sources:
            logger.warning(f"成分剪枝：本輪掃描幣數過少，跳過剪枝來源 {sorted(suspect_sources)}")
        prune_changes = []
        for sym in list(self._constituent_snapshot.keys()):
            sym_data = self._constituent_snapshot[sym]
            for source in [k for k in sym_data.keys() if not k.startswith("_")]:
                live_syms = current_syms_by_source.get(source)
                # 該來源本輪整所失敗 / 掃描異常 → 不動，避免誤刪
                if live_syms is None or source in suspect_sources:
                    continue
                if sym in live_syms:
                    continue  # 該來源本輪仍上架此幣 → 保留
                # 該來源本輪有上架其他幣、唯獨沒這個幣 → 已下架，剪掉整個來源
                sym_data.pop(source, None)
                sym_data.pop(f"_updated_{source}", None)
                prune_changes.append({
                    "type": "source_removed",
                    "constituent_exchange": "",  # 整個指數來源下架，非單一成分移除
                    "old_weight": None,
                    "new_weight": None,
                    "ts": now_iso,
                    "symbol": sym,
                    "source": source,
                })
            # 該幣已無任何來源 → 整個 key 清掉（含殘留的 _updated_* / _* 欄位）
            if not any(not k.startswith("_") for k in sym_data.keys()):
                del self._constituent_snapshot[sym]

        if prune_changes:
            self._save_constituent_snapshot()
            self._constituent_changes.extend(prune_changes)
            self._save_constituent_changes()
            logger.info(f"成分剪枝：移除 {len(prune_changes)} 個已下架來源")

        # 優先查快照中沒有的幣，其次是超過 24h 未更新的，每個 source 最多 _MAX_PER_SOURCE 個
        symbols_by_source: dict[str, list[tuple[str, str]]] = {}
        for source, pairs in all_by_source.items():
            new_pairs = [(f, s) for f, s in pairs if s not in self._constituent_snapshot or source not in self._constituent_snapshot.get(s, {})]
            stale_pairs = [(f, s) for f, s in pairs if s in self._constituent_snapshot and source in self._constituent_snapshot.get(s, {})
                           and self._constituent_snapshot[s].get(f"_updated_{source}", "0") < cutoff_iso]
            priority = new_pairs + stale_pairs
            if priority:
                # 依 storage_sym 去重
                seen = set()
                deduped = []
                for f, s in priority:
                    if s in seen:
                        continue
                    seen.add(s)
                    deduped.append((f, s))
                symbols_by_source[source] = deduped[:_MAX_PER_SOURCE]

        if not symbols_by_source:
            return

        total_symbols = sum(len(p) for p in symbols_by_source.values())
        new_count = sum(
            sum(1 for _, s in pairs if s not in self._constituent_snapshot or source not in self._constituent_snapshot.get(s, {}))
            for source, pairs in symbols_by_source.items()
        )
        logger.info(f"開始抓取指數成分：{len(symbols_by_source)} 個交易所，{total_symbols} 個幣種（{new_count} 個新幣）")

        new_snapshot = {}
        changes = []

        async with make_session() as session:
            for source, pairs in symbols_by_source.items():
                try:
                    fetch_syms = [f for f, _ in pairs]
                    storage_map = {f: s for f, s in pairs}
                    fetched = await self._fetch_constituents_batch(
                        session, source, fetch_syms
                    )
                    for fetch_sym, constituents in fetched.items():
                        storage_sym = storage_map.get(fetch_sym, fetch_sym)
                        if storage_sym not in new_snapshot:
                            new_snapshot[storage_sym] = {}
                        new_snapshot[storage_sym][source] = constituents

                        # 與舊快照比對
                        old_constituents = self._constituent_snapshot.get(storage_sym, {}).get(source, [])
                        if old_constituents:
                            diffs = self._diff_constituents(old_constituents, constituents)
                            for d in diffs:
                                d["ts"] = now_iso
                                d["symbol"] = storage_sym
                                d["source"] = source  # 哪個交易所的指數
                                changes.append(d)

                    logger.debug(f"成分抓取完成：{source} ({len(fetched)}/{len(pairs)})")
                except Exception as e:
                    logger.warning(f"成分抓取失敗 {source}: {e}")

        # 合併快照（保留舊的未更新幣種）
        for sym in new_snapshot:
            if sym not in self._constituent_snapshot:
                self._constituent_snapshot[sym] = {}
            for source, constituents in new_snapshot[sym].items():
                self._constituent_snapshot[sym][source] = constituents
                self._constituent_snapshot[sym][f"_updated_{source}"] = now_iso
        self._save_constituent_snapshot()

        # 追加變更到歷史
        if changes:
            self._constituent_changes.extend(changes)
            logger.info(f"指數成分變更：{len(changes)} 筆（{len(set(c['symbol'] for c in changes))} 個幣種）")
            # 只有結構性變更（新增/移除成分交易所）才發 TG 通知
            structural = [c for c in changes if c.get("type") in ("added", "removed")]
            if structural:
                asyncio.create_task(self._notify_constituent_changes(
                    structural,
                    index_tracking_snapshot or self.index_tracking,
                    index_all_deviations_snapshot or self.index_all_deviations,
                ))

        # 清理 7 天前
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        self._constituent_changes = [c for c in self._constituent_changes if c["ts"] > cutoff]
        self._save_constituent_changes()

    def _detect_price_contamination(self, source_data: list[dict], threshold_pct: float = 1.0) -> list[dict]:
        """在指數成分中找出價格離群的來源（中位數基準）。
        source_data: [{exchange, weight, price}, ...]
        回傳: [{exchange, price, weight, deviation_pct, median_price}]
        """
        prices = [item["price"] for item in source_data if (item.get("price") or 0) > 0]
        if len(prices) < 3:
            return []
        median_price = statistics.median(prices)
        if median_price <= 0:
            return []
        contaminated = []
        for item in source_data:
            price = item.get("price") or 0  # Gate/MEXC 成分無 price（None），視為 0 排除
            if price <= 0:
                continue
            dev = (price / median_price - 1) * 100
            if abs(dev) >= threshold_pct:
                contaminated.append({
                    "exchange": item.get("exchange", "?"),
                    "price": price,
                    "weight": item.get("weight"),
                    "deviation_pct": round(dev, 4),
                    "median_price": median_price,
                })
        return contaminated


    # 指數成分異動通知閾值：只有偏離夠大才值得通知（避免微小變動騷擾）
    _CONSTITUENT_NOTIFY_DEV_THRESHOLD = 0.5   # |dev| >= 0.5% 才通知
    _CONSTITUENT_NOTIFY_SPIKE_THRESHOLD = 0.5  # |spike| >= 0.5% 才通知

    async def _notify_constituent_changes(
        self,
        structural_changes: list[dict],
        index_tracking_snapshot: list = None,
        index_all_deviations_snapshot: list = None,
    ):
        """發送「指數成分結構異動」的 TG 通知

        只通知偏離超過閾值的異動，過濾掉微小變動和無偏離資料的項目。
        使用掃描時的快照，避免 async task 延遲導致資料被下一次掃描覆蓋。
        """
        tracking = index_tracking_snapshot if index_tracking_snapshot is not None else self.index_tracking
        all_devs = index_all_deviations_snapshot if index_all_deviations_snapshot is not None else self.index_all_deviations

        # 建立偏離查找表 {(symbol, exchange): entry}
        dev_map: dict[tuple, dict] = {}
        for a in all_devs:
            key = (a["symbol"], a["exchange"])
            dev_map[key] = a
        # anomaly 優先覆蓋（資料更完整）
        for a in tracking:
            key = (a["symbol"], a["exchange"])
            dev_map[key] = a

        if not structural_changes:
            return

        # 依 symbol+source 分組
        grouped: dict[tuple, list] = {}
        for c in structural_changes:
            key = (c["symbol"], c["source"])
            grouped.setdefault(key, []).append(c)

        lines = []
        now_str = datetime.now(timezone.utc).strftime("%m-%d %H:%M UTC")
        for (sym, source), clist in grouped.items():
            base = sym.split("/")[0]
            dev_entry = dev_map.get((sym, source))

            # --- 篩選：沒有偏離資料 或 偏離太小的不通知 ---
            if not dev_entry:
                continue
            
            dev = dev_entry.get("deviation_pct", 0)
            spike = dev_entry.get("spike_pct")
            
            # 如果偏離值太小，不通知（使用者不想看 0.01% 的變動）
            if abs(dev) < self._CONSTITUENT_NOTIFY_DEV_THRESHOLD and \
               (spike is None or abs(spike) < self._CONSTITUENT_NOTIFY_SPIKE_THRESHOLD):
                continue

            # dev 是相對於 Market Median 的
            dev_str = f" dev={dev:+.2f}%"
            spike_str = f" spike={spike:+.2f}%" if spike is not None else ""
            
            for c in clist:
                icon = "✅" if c["type"] == "added" else "❌"
                ex = c.get("constituent_exchange", "?")
                wt = c.get("new_weight")
                wt_str = f" ({wt:.2%})" if wt is not None else ""
                # 這裡顯示 source 所對應的 dev/spike
                lines.append(f"{icon} {base} [{source}]{dev_str}{spike_str} {c['type']}: {ex}{wt_str}")

            # 附上當前成分價格污染資訊（如有）
            source_data = self._constituent_snapshot.get(sym, {}).get(source, [])
            contaminated = self._detect_price_contamination(source_data)
            if contaminated:
                for c in contaminated:
                    wt_str = f" 權重{c['weight']:.2%}" if c.get("weight") is not None else ""
                    lines.append(
                        f"  ⚠ 污染來源 {c['exchange']}: {c['price']:.6g} "
                        f"(中位數={c['median_price']:.6g}, 偏差={c['deviation_pct']:+.2f}%{wt_str})"
                    )

        if not lines:
            return

        msg = f"🔔 指數成分異動 (對比中位數) {now_str}\n" + "\n".join(lines)

        # 直接 POST 到 TG 服務，避免 subprocess 在 Windows 的編碼問題
        if not TG_SEND_URL:
            return  # 沒設 TG_SEND_URL 就不發通知
        try:
            async with make_session() as session:
                await session.post(
                    TG_SEND_URL,
                    json={"content": msg, "topic_id": int(os.environ.get("TG_TOPIC_ID") or 0)},
                    timeout=aiohttp.ClientTimeout(total=5),
                )
            logger.info(f"指數成分異動通知已發送：{len(structural_changes)} 筆（{len(grouped)} 個幣種來源）")
        except Exception as e:
            logger.warning(f"指數成分異動通知發送失敗: {e}")

    async def scan_constituents_for_symbol(self, symbol: str, sources: tuple | None = None) -> dict:
        """針對單一幣種，立即向所有支援的交易所抓取指數成分並更新快照

        前端傳入 normalized symbol（例 TST/USDT:USDT），此函式會從 last_result
        反查每個 source 的原始 symbol 用於 API 查詢，但快照仍以 normalized 為 key。
        回傳本輪成功抓到的 {source: [constituents]}（供持續監控比對用）。
        """
        SUPPORTED = sources or ("binance", "bybit", "okx", "kucoinfutures", "gateio", "bitget")
        now_iso = datetime.now(timezone.utc).isoformat()
        changes = []
        fetched: dict[str, list] = {}

        # 反查每個 source 對應的原始 symbol（例 bybit 上 TST 叫 TSTBSC）
        fetch_sym_by_source: dict[str, str] = {}
        if self.last_result:
            for r in self.last_result.records:
                if r.exchange not in SUPPORTED:
                    continue
                if (r.normalized_symbol or r.symbol) == symbol:
                    fetch_sym_by_source.setdefault(r.exchange, r.symbol)
        # 沒掃描資料的 source 就用 normalized symbol 當 fallback
        for src in SUPPORTED:
            fetch_sym_by_source.setdefault(src, symbol)

        async with make_session() as session:
            # KuCoin 需要先取得 indexSymbol 映射
            kucoin_map = await self._fetch_kucoin_index_symbol_map(session)
            tasks = {
                src: self._fetch_one_constituent(
                    session, src, fetch_sym_by_source[src],
                    kucoin_index_map=kucoin_map if src == "kucoinfutures" else None
                )
                for src in SUPPORTED
            }
            results = await asyncio.gather(*tasks.values(), return_exceptions=True)

            for source, constituents in zip(tasks.keys(), results):
                if isinstance(constituents, Exception) or not constituents:
                    continue
                if symbol not in self._constituent_snapshot:
                    self._constituent_snapshot[symbol] = {}

                old = self._constituent_snapshot.get(symbol, {}).get(source, [])
                if old:
                    diffs = self._diff_constituents(old, constituents)
                    for d in diffs:
                        d["ts"] = now_iso
                        d["symbol"] = symbol
                        d["source"] = source
                        changes.append(d)

                self._constituent_snapshot[symbol][source] = constituents
                fetched[source] = constituents

        self._save_constituent_snapshot()
        if changes:
            self._constituent_changes.extend(changes)
            self._save_constituent_changes()
        logger.info(f"按需成分掃描完成：{symbol}，{len(self._constituent_snapshot.get(symbol, {}))} 個交易所")
        return fetched

    # ---------------- 指數成分持續監控（沿用 LAB 規則） ----------------

    _IW_SRC_LABEL = {
        "binance": "Binance", "bybit": "Bybit", "okx": "OKX",
        "kucoinfutures": "KuCoin", "gateio": "Gate", "bitget": "Bitget", "mexc": "MEXC",
    }

    @staticmethod
    def _iw_fmt_w(w) -> str:
        return "-" if w is None else f"{w * 100:.2f}%"

    @staticmethod
    def _iw_diff_source(old: dict, new: dict, threshold: float) -> list[tuple[str, str]]:
        """比對單一來源成分變化，回傳 (成分交易所, 描述)。threshold>0 時權重變動 <門檻不列入。"""
        items = []
        old_ex, new_ex = set(old), set(new)
        for ex in sorted(new_ex - old_ex):
            items.append((ex, f"  ＋新增 {ex}（{FundingScanner._iw_fmt_w(new[ex])}）"))
        for ex in sorted(old_ex - new_ex):
            items.append((ex, f"  －移除 {ex}（原 {FundingScanner._iw_fmt_w(old[ex])}）"))
        for ex in sorted(old_ex & new_ex):
            ow, nw = old[ex], new[ex]
            if ow is None or nw is None:
                continue
            if threshold > 0:
                if abs(nw - ow) < threshold:
                    continue
            elif round(nw, INDEX_WATCH_ROUND) == round(ow, INDEX_WATCH_ROUND):
                continue
            items.append((ex, f"  ～{ex} 權重 {FundingScanner._iw_fmt_w(ow)} → {FundingScanner._iw_fmt_w(nw)}"))
        return items

    async def _send_index_watch_tg(self, msg: str):
        # 直接 POST 到 TG 推送服務，指定目標頻道 + topic
        if not TG_SEND_URL:
            return  # 沒設 TG_SEND_URL 就不發通知
        try:
            async with make_session() as session:
                await session.post(
                    TG_SEND_URL,
                    json={
                        "content": msg,
                        "chat_id": INDEX_WATCH_TG_CHAT_ID,
                        "topic_id": INDEX_WATCH_TG_TOPIC_ID,
                    },
                    timeout=aiohttp.ClientTimeout(total=5),
                )
        except Exception as e:
            logger.warning(f"[index-watch] TG 通知失敗: {e}")

    async def _index_watch_check(self, symbol: str):
        """抓取一次某幣種成分，套用 LAB 規則比對，變動就發 TG；順帶檢查現貨充提狀態。"""
        # 先檢查充提（獨立基準，不受下方成分首輪 return 影響）
        await self._index_watch_wallet_check(symbol)

        fetched = await self.scan_constituents_for_symbol(symbol, sources=INDEX_WATCH_SOURCES)
        cur = {
            src: {c["exchange"]: c.get("weight") for c in lst}
            for src, lst in fetched.items()
        }
        prev = self._index_watch_state.setdefault(symbol, {})
        first = not prev

        blocks = []
        for src in cur:
            thr = INDEX_WATCH_SOURCE_THRESHOLD.get(src, 0.0)
            items = self._iw_diff_source(prev.get(src, {}), cur[src], thr)
            # 只通知與 TARGET(bitget) 有關：自家指數全收，其他指數只收 bitget 那筆
            if src == INDEX_WATCH_TARGET:
                lines = [t for _, t in items]
            else:
                lines = [t for ex, t in items if ex == INDEX_WATCH_TARGET]
            if lines:
                blocks.append(f"【{self._IW_SRC_LABEL.get(src, src)} 指數】\n" + "\n".join(lines))
                prev[src] = cur[src]        # 有通知才更新基準（避免小幅連續漂移被無視）
            elif src not in prev:
                prev[src] = cur[src]        # 首次見到的來源先建基準

        if first:
            return  # 首輪僅建立基準，不通知
        if blocks:
            base = symbol.split("/")[0] if "/" in symbol else symbol
            await self._send_index_watch_tg(
                f"⚠️ {base} 指數變動（Bitget 相關）\n" + "\n\n".join(blocks)
            )

    async def _fetch_coin_wallet_status(self, base: str) -> dict:
        """抓取某幣現貨充值/提幣狀態（Gate + Bitget 公開單一幣端點，輕量）。
        回傳 {exchange: {chain: {"deposit": bool, "withdraw": bool}}}，抓不到的所不放入（不誤判）。
        """
        result = {}
        async with make_session() as session:
            # Gate（公開）：chains[].deposit_disabled / withdraw_disabled
            try:
                async with session.get(
                    f"https://api.gateio.ws/api/v4/spot/currencies/{base}",
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as r:
                    if r.status == 200:
                        d = await r.json()
                        chains = {}
                        for c in d.get("chains", []) or []:
                            name = c.get("name", "")
                            if not name:
                                continue
                            chains[name] = {
                                "deposit": not c.get("deposit_disabled", False),
                                "withdraw": not c.get("withdraw_disabled", False),
                            }
                        if chains:
                            result["gateio"] = chains
            except Exception as e:
                logger.debug(f"[index-watch] Gate 充提查詢失敗 {base}: {e}")

            # Bitget（公開）：data[0].chains[].rechargeable / withdrawable（字串）
            try:
                async with session.get(
                    "https://api.bitget.com/api/v2/spot/public/coins",
                    params={"coin": base}, timeout=aiohttp.ClientTimeout(total=8),
                ) as r:
                    if r.status == 200:
                        d = await r.json()
                        if d.get("code") == "00000" and d.get("data"):
                            chains = {}
                            for c in d["data"][0].get("chains", []) or []:
                                name = c.get("chain", "")
                                if not name:
                                    continue
                                chains[name] = {
                                    "deposit": str(c.get("rechargeable")).lower() == "true",
                                    "withdraw": str(c.get("withdrawable")).lower() == "true",
                                }
                            if chains:
                                result["bitget"] = chains
            except Exception as e:
                logger.debug(f"[index-watch] Bitget 充提查詢失敗 {base}: {e}")
        return result

    @staticmethod
    def _iw_wallet_snapshot_lines(state: dict) -> list[str]:
        """把充提現況攤成可讀列表（所有所/鏈），供監控啟動時的初始訊息用。"""
        _sw = lambda b: "開" if b else "關"
        lines = []
        for ex in sorted(state):
            label = FundingScanner._IW_SRC_LABEL.get(ex, ex)
            for chain in sorted(state[ex]):
                c = state[ex][chain]
                lines.append(f"  {label} {chain}：充{_sw(c['deposit'])}/提{_sw(c['withdraw'])}")
        return lines

    @staticmethod
    def _iw_diff_wallet(old: dict, new: dict) -> list[str]:
        """比對充提狀態變化，回傳描述列表（每條鏈分開）。
        只比對兩邊都抓到的交易所，避免某所抓取忽好忽壞造成假「新增/移除」；
        整所首次出現會由呼叫端靜默併入基準。
        """
        _sw = lambda b: "開" if b else "關"
        lines = []
        for ex in sorted(set(old) & set(new)):
            o, n = old[ex], new[ex]
            label = FundingScanner._IW_SRC_LABEL.get(ex, ex)
            for chain in sorted(set(o) | set(n)):
                oc, nc = o.get(chain), n.get(chain)
                if oc is None and nc is not None:
                    lines.append(f"  ＋{label} {chain} 新增（充:{_sw(nc['deposit'])}/提:{_sw(nc['withdraw'])}）")
                elif oc is not None and nc is None:
                    lines.append(f"  －{label} {chain} 下架")
                elif oc and nc:
                    if oc["deposit"] != nc["deposit"]:
                        lines.append(f"  {label} {chain} 充值 {_sw(oc['deposit'])}→{_sw(nc['deposit'])}")
                    if oc["withdraw"] != nc["withdraw"]:
                        lines.append(f"  {label} {chain} 提幣 {_sw(oc['withdraw'])}→{_sw(nc['withdraw'])}")
        return lines

    async def _index_watch_wallet_check(self, symbol: str):
        """檢查該幣現貨充提狀態，變動就發 TG（首輪只建基準）。"""
        base = symbol.split("/")[0] if "/" in symbol else symbol
        cur = await self._fetch_coin_wallet_status(base)
        if not cur:
            return
        prev = self._index_watch_wallet.get(symbol)
        if prev is None:
            self._index_watch_wallet[symbol] = cur  # 首輪建基準
            # 首輪送一次完整現況，讓使用者看到起始基準（含所有所/鏈）
            snap = self._iw_wallet_snapshot_lines(cur)
            if snap:
                await self._send_index_watch_tg(f"💰 {base} 現貨充提監控啟動\n" + "\n".join(snap))
            return
        lines = self._iw_diff_wallet(prev, cur)
        # 更新基準（只併入這次有抓到的所；抓不到的沿用舊值，整所首次出現也在此靜默建基準）
        prev.update(cur)
        if lines:
            await self._send_index_watch_tg(f"💰 {base} 現貨充提狀態變動\n" + "\n".join(lines))

    async def _index_watch_loop(self):
        logger.info(f"指數成分持續監控啟動：{sorted(self._index_watch)}")
        # 只要程式在跑就常駐（清單為空時空轉睡眠），避免「清空→退出」與「重新加入」
        # 之間的競態導致監控靜默停擺；也保證同時只有一個 loop。
        while self.is_running:
            if not self._index_watch:
                await asyncio.sleep(INDEX_WATCH_INTERVAL)
                continue
            for symbol in list(self._index_watch):
                try:
                    await self._index_watch_check(symbol)
                except Exception as e:
                    logger.warning(f"[index-watch] {symbol} 檢查失敗: {e}")
            await asyncio.sleep(INDEX_WATCH_INTERVAL)
        self._index_watch_task = None
        logger.info("指數成分持續監控迴圈結束")

    def _ensure_index_watch_task(self):
        if self._index_watch and (self._index_watch_task is None or self._index_watch_task.done()):
            self._index_watch_task = asyncio.create_task(self._index_watch_loop())

    def is_index_watched(self, symbol: str) -> bool:
        return symbol in self._index_watch

    def add_index_watch(self, symbol: str) -> bool:
        self._index_watch.add(symbol)
        self._index_watch_state.pop(symbol, None)  # 重新建立基準（避免用舊值誤報）
        self._index_watch_wallet.pop(symbol, None)
        self._save_index_watch()
        self._ensure_index_watch_task()
        logger.info(f"[index-watch] 開始監控 {symbol}")
        return True

    def remove_index_watch(self, symbol: str) -> bool:
        self._index_watch.discard(symbol)
        self._index_watch_state.pop(symbol, None)
        self._index_watch_wallet.pop(symbol, None)
        self._save_index_watch()
        logger.info(f"[index-watch] 取消監控 {symbol}")
        return False

    def _load_index_watch(self):
        try:
            if INDEX_WATCH_PATH.exists():
                with open(INDEX_WATCH_PATH, "r", encoding="utf-8") as f:
                    self._index_watch = set(json.load(f))
                if self._index_watch:
                    logger.info(f"載入持續監控清單：{len(self._index_watch)} 個幣種")
        except Exception as e:
            logger.warning(f"載入持續監控清單失敗: {e}")
            self._index_watch = set()

    def _save_index_watch(self):
        try:
            with open(INDEX_WATCH_PATH, "w", encoding="utf-8") as f:
                json.dump(sorted(self._index_watch), f)
        except Exception as e:
            logger.warning(f"儲存持續監控清單失敗: {e}")

    def _compute_index_tracking(self):
        """計算指數追蹤：以市場中位數 (Market Median) 為基準，偵測各交易所的偏離與突發異動"""
        if not self.last_result:
            return

        now_iso = datetime.now(timezone.utc).isoformat()

        # 按正規化幣種分組，收集各交易所的 index_price
        sym_index = {}  # {sym: {exchange: index_price}}
        for r in self.last_result.records:
            if r.index_price is None or r.index_price <= 0:
                continue
            key = r.normalized_symbol or r.symbol
            if key not in sym_index:
                sym_index[key] = {}
            # 1000X 慣例：1000TURBO 的 index_price 是「每1000顆」的價格，需 ÷1000 換算成每顆
            raw_base = r.symbol.split("/")[0].upper()
            _, multiplier = _strip_1000x(raw_base)
            sym_index[key][r.exchange] = r.index_price / multiplier

        anomalies = []
        all_deviations = []
        history_batch = []

        for sym, ex_prices in sym_index.items():
            if not ex_prices:
                continue
            
            # 使用所有可用交易所價格的中位數作為「市場真實價格」基準
            prices = list(ex_prices.values())
            if len(prices) < 2:
                # 只有一家交易所，無法計算偏離
                continue
            market_median = statistics.median(prices)
            if market_median <= 0:
                continue

            base_coin = sym.split("/")[0] if "/" in sym else sym

            for ex, price in ex_prices.items():
                deviation_pct = (price / market_median - 1) * 100  # 正=該所偏高，負=偏低
                dev_rounded = round(deviation_pct, 4)

                # 偏離超出合理範圍視為計算異常（如 1000X 幣種未正確換算），跳過
                if not (-90 <= dev_rounded <= 1000):
                    continue

                # 記錄到歷史（所有幣種所有交易所都記，用於趨勢查詢）
                history_batch.append({
                    "ts": now_iso,
                    "sym": sym,
                    "ex": ex,
                    "dev": dev_rounded,
                    "market_price": market_median,
                    "ex_price": price,
                })

                # 從 RAM float32 序列取該幣種+交易所的近期偏離趨勢（array → list）
                hist_devs = list(self._index_devs.get((sym, ex), ()))
                recent_devs = hist_devs[-24:]  # 最近 24 筆（趨勢圖用）

                # 突發偏離：當前偏離 - 歷史中位數（基線）
                if len(hist_devs) >= 3:
                    sorted_hist = sorted(hist_devs)
                    mid = len(sorted_hist) // 2
                    if len(sorted_hist) % 2 == 0:
                        baseline = (sorted_hist[mid - 1] + sorted_hist[mid]) / 2
                    else:
                        baseline = sorted_hist[mid]
                    spike = round(dev_rounded - baseline, 4)
                else:
                    # 歷史不足，無法判斷突發，spike = None
                    baseline = None
                    spike = None

                entry = {
                    "symbol": sym,
                    "base_coin": base_coin,
                    "exchange": ex,
                    "deviation_pct": dev_rounded,
                    "baseline_pct": round(baseline, 4) if baseline is not None else None,
                    "spike_pct": spike,
                    "market_price": market_median,
                    "ex_price": price,
                    "recent_devs": recent_devs,
                }

                all_deviations.append(entry)

                # 異常判定標準調高：突發偏離 ≥ 0.8%，或絕對偏離 ≥ 1.5%（無歷史時）
                # 這樣可以過濾掉大部分微小成分異動引起的波動
                if spike is not None:
                    if abs(spike) >= 0.8:
                        anomalies.append(entry)
                elif abs(deviation_pct) >= 1.5:
                    anomalies.append(entry)

                # 把本輪新 dev 追加到 RAM float32 序列（供下輪算基線/趨勢；hist_devs 已在上面讀取、不含本輪）
                self._dev_push_to(self._index_devs, (sym, ex), dev_rounded)

        # 完整明細只追加寫按日 JSONL（不進 RAM、永不重寫整檔）；RAM 序列已在迴圈內即時更新。
        # 背景重建完成前不寫，避免與載入 race（少數啟動批次僅存在 RAM，可接受）。
        if self._index_history_ready:
            self._append_index_batch(history_batch)
            self._prune_index_day_files()

        # 按突發偏離排序
        anomalies.sort(key=lambda x: abs(x["spike_pct"]) if x["spike_pct"] is not None else abs(x["deviation_pct"]), reverse=True)
        all_deviations.sort(key=lambda x: abs(x["spike_pct"]) if x["spike_pct"] is not None else abs(x["deviation_pct"]), reverse=True)
        self.index_tracking = anomalies
        self.index_all_deviations = all_deviations

        if anomalies:
            logger.info(f"指數追蹤：{len(anomalies)} 個異常（偏離≥1.5%），最大偏離 {anomalies[0]['base_coin']} {anomalies[0]['exchange']} {anomalies[0]['deviation_pct']:+.2f}%")
        else:
            logger.debug("指數追蹤：無異常")

    async def _fetch_history_one(self, ex, variants, since_ms):
        """嘗試從交易所抓某幣種的歷史費率"""
        for sym in variants:
            try:
                data = await ex.fetch_funding_history(sym, since=since_ms, limit=100)
                if data:
                    return data
            except Exception:
                continue
        return []

    async def _fetch_all_histories(self, target_syms, since_ms):
        """抓歷史費率（只對掃描中確認有該幣的交易所抓）"""
        all_entries = []

        for ex in self._exchanges:
            # 只抓該交易所實際有的幣種
            sym_list = [sym for sym, recs in target_syms.items() if ex.name in recs]
            if not sym_list:
                continue
            count = 0
            batch_size = 10
            for i in range(0, len(sym_list), batch_size):
                batch = sym_list[i:i + batch_size]
                tasks = []
                for sym in batch:
                    variants = self._get_symbol_variants(sym, ex.name)
                    tasks.append(self._fetch_history_one(ex, variants, since_ms))
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                for sym, br in zip(batch, batch_results):
                    if isinstance(br, BaseException) or not br:
                        continue
                    now_ts = datetime.now(timezone.utc)
                    for h in br:
                        dt = datetime.fromtimestamp(h["timestamp"] / 1000, tz=timezone.utc)
                        # 對齊到整點（去掉秒和微秒，避免重複）
                        dt = dt.replace(second=0, microsecond=0)
                        # 排除非整點（掃描快照）+ 未來時間戳（未結算）
                        if dt.minute != 0 or dt > now_ts:
                            continue
                        all_entries.append({
                            "ts": dt.isoformat(),
                            "ex": ex.name,
                            "sym": sym,
                            "rate": h["rate"],
                        })
                        count += 1
                if i + batch_size < len(sym_list):
                    await asyncio.sleep(0.3)
            logger.info(f"  {ex.name}: {count} 筆結算記錄")

        return all_entries

    async def _bootstrap_and_compute_meat(self):
        """啟動時：先用磁碟快取立即計算，再背景補齊"""
        self._meat_bootstrapping = True
        # 先用快取資料立即算一次（讓前端有東西看，同時顯示「補齊中」）
        if self._rate_history and self.last_result:
            self._compute_meat_flow()
            logger.info(f"碎肉流：從快取立即計算完成（{len(self.meat_flow)} 個幣種）")
        # 補齊缺失的歷史
        await self._update_meat_flow()

    def _is_spike_driven(self, sym, high_ex, low_ex, rate_entries, threshold=0.005):
        """檢查日資費差 > threshold 是否由單一次異常資費導致。
        條件（全部成立才排除）：
        1. 移除該筆後差值跌破閾值
        2. 該筆佔該交易所總資費差的 90% 以上
        3. 該筆不是該交易所最新一筆結算"""
        high_entries = sorted(
            [e for e in rate_entries if e["sym"] == sym and e["ex"] == high_ex],
            key=lambda e: e["ts"]
        )
        low_entries = sorted(
            [e for e in rate_entries if e["sym"] == sym and e["ex"] == low_ex],
            key=lambda e: e["ts"]
        )

        if not high_entries or not low_entries:
            return False

        high_rates = [e["rate"] for e in high_entries]
        low_rates = [e["rate"] for e in low_entries]
        high_sum = sum(high_rates)
        low_sum = sum(low_rates)
        diff = high_sum - low_sum

        if diff < threshold:
            return False

        # 檢查高費率所（排除最新一筆）
        for i, r in enumerate(high_rates):
            if i == len(high_rates) - 1:
                continue  # 最新一筆不排除
            if diff - r < threshold:
                # 該筆對 diff 的貢獻：r（正值拉高 high_sum）
                # 佔比 = r / diff
                if abs(r) / diff >= 0.9:
                    return True

        # 檢查低費率所（排除最新一筆）
        for i, r in enumerate(low_rates):
            if i == len(low_rates) - 1:
                continue  # 最新一筆不排除
            if diff + r < threshold:
                # 該筆對 diff 的貢獻：|r|（負值拉低 low_sum）
                # 佔比 = |r| / diff
                if abs(r) / diff >= 0.9:
                    return True

        return False

    def _build_opportunity(self, sym, ex_map, rate_sums, high_ex, high_data, low_ex, low_data, recent_entries):
        """根據高低費率所建構機會 dict"""
        daily_diff_pct = (high_data["sum"] - low_data["sum"]) * 100
        ref_rec = ex_map.get("binance") or next(iter(ex_map.values()))
        high_rec = ex_map.get(high_ex)
        low_rec = ex_map.get(low_ex)
        spread_pct = None
        if high_rec and low_rec and low_rec.ask_price and high_rec.bid_price and high_rec.bid_price > 0:
            _, high_mult = _strip_1000x(high_rec.symbol.split("/")[0].upper())
            _, low_mult = _strip_1000x(low_rec.symbol.split("/")[0].upper())
            adj_low_ask = low_rec.ask_price / low_mult
            adj_high_bid = high_rec.bid_price / high_mult
            if adj_high_bid > 0:
                spread_pct = round((adj_low_ask / adj_high_bid - 1) * 100, 4)
        exchanges_detail = {}
        for ex, ex_data in rate_sums[sym].items():
            # 只顯示目前掃描中存在的交易所（排除已下架/SETTLING）
            if ex not in ex_map:
                continue
            exchanges_detail[ex] = {
                "daily_rate_pct": round(ex_data["sum"] * 100, 4),
                "periods": ex_data["count"],
                "diff_pct": round((ex_data["sum"] - low_data["sum"]) * 100, 4),
            }

        high_max_notional = get_max_notional(high_ex, high_rec.symbol) if high_rec else None
        low_max_notional = get_max_notional(low_ex, low_rec.symbol) if low_rec else None

        # CoinW 檔位 1 最大持倉張數（若某一側是 CoinW）
        coinw_info = None
        if high_ex == "coinw" and high_rec:
            coinw_info = get_coinw_position_info(high_rec.symbol)
        elif low_ex == "coinw" and low_rec:
            coinw_info = get_coinw_position_info(low_rec.symbol)

        # 預期收益（日）= 倉位上限 (USDT) × 每日資費差 %
        # 若某一側是 CoinW，直接用檔位 1 張數 × 每張 USDT 價值 作為倉位上限
        # （用戶要求：張數 × 幣價 × 每日資費差 = 預期收益/日，不套 50k cap）
        effective_limit = None
        if coinw_info and ref_rec and ref_rec.mark_price:
            coinw_notional = (
                coinw_info["max_piece"] * coinw_info["lot_size"] * ref_rec.mark_price
            )
            if coinw_notional > 0:
                effective_limit = coinw_notional
        if effective_limit is None:
            limit = min(high_max_notional or 0, low_max_notional or 0)
            if limit > 0:
                effective_limit = min(limit, 50000)
        expected_profit = (
            round(daily_diff_pct * effective_limit / 100, 2) if effective_limit else None
        )

        return {
            "symbol": sym,
            "base_coin": sym.split("/")[0],
            "high_exchange": high_ex,
            "high_daily_rate_pct": round(high_data["sum"] * 100, 4),
            "high_periods": high_data["count"],
            "high_interval_h": high_rec.funding_interval_h if high_rec else None,
            "high_current_rate": high_rec.funding_rate if high_rec else None,
            "high_max_notional": high_max_notional,
            "low_exchange": low_ex,
            "low_daily_rate_pct": round(low_data["sum"] * 100, 4),
            "low_periods": low_data["count"],
            "low_interval_h": low_rec.funding_interval_h if low_rec else None,
            "low_current_rate": low_rec.funding_rate if low_rec else None,
            "low_max_notional": low_max_notional,
            "daily_diff_pct": round(daily_diff_pct, 4),
            "annual_diff_pct": round(daily_diff_pct * 365, 2),
            "spread_pct": spread_pct,
            "mark_price": ref_rec.mark_price,
            "expected_profit": expected_profit,
            "exchanges": exchanges_detail,
            "coinw_max_piece": coinw_info["max_piece"] if coinw_info else None,
            "coinw_lot_size": coinw_info["lot_size"] if coinw_info else None,
        }

    def _compute_meat_flow(self):
        """從 _rate_history 快取計算碎肉流（3 種模式）"""
        if not self.last_result:
            return

        start_time = time.time()

        # 收集所有幣種及其所在交易所
        sym_exchanges = {}
        for r in self.last_result.records:
            key = r.normalized_symbol or r.symbol
            if key not in sym_exchanges:
                sym_exchanges[key] = {}
            sym_exchanges[key][r.exchange] = r

        multi_ex_syms = {sym: recs for sym, recs in sym_exchanges.items() if len(recs) >= 2}
        if not multi_ex_syms:
            self.meat_flow = []
            self.meat_flow_coinw = []
            self.meat_flow_same_interval = []
            return

        # 從快取中取 24h 內的資料（只保留整點結算記錄）
        cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        recent_entries = []
        for e in self._rate_history:
            if e["ts"] <= cutoff_24h:
                continue
            try:
                dt = datetime.fromisoformat(e["ts"])
                if dt.minute != 0:
                    continue
            except Exception:
                continue
            recent_entries.append(e)

        # 按 (sym, exchange) 分組加總，同一趟順便算出每個 (幣,所) 的時間覆蓋範圍。
        #
        # ⚠ ex_ts_range 一定要在這裡一次算完，不可以放進下面的幣種迴圈裡重算：
        #   原本的寫法是在「每個幣種」的迴圈內再把整個 recent_entries 從頭掃一遍
        #   （for e in recent_entries: if e["sym"] != sym: continue），
        #   複雜度是 O(幣種 × 全量歷史)。用真實資料實測：
        #     rate_history 174,153 筆 → recent_entries 58,110 筆、外圈 992 個幣種
        #     = 5,760 萬次元素訪問，單這段就 2.38 秒；改成這裡一次 group-by 只要 0.006 秒（快 400 倍）。
        #   py-spy 取樣顯示那行 `if e["sym"] != sym` 曾是全專案第 2 熱的單行（3.09%）。
        rate_sums = {}
        ex_ts_ranges: dict[str, dict[str, dict]] = {}
        for e in recent_entries:
            sym = e["sym"]
            if sym not in multi_ex_syms:
                continue
            ex, ts = e["ex"], e["ts"]
            if sym not in rate_sums:
                rate_sums[sym] = {}
                ex_ts_ranges[sym] = {}
            if ex not in rate_sums[sym]:
                rate_sums[sym][ex] = {"sum": 0.0, "count": 0}
                ex_ts_ranges[sym][ex] = {"min": ts, "max": ts, "count": 0}
            rate_sums[sym][ex]["sum"] += e["rate"]
            rate_sums[sym][ex]["count"] += 1
            r = ex_ts_ranges[sym][ex]
            if ts < r["min"]:
                r["min"] = ts
            if ts > r["max"]:
                r["max"] = ts
            r["count"] += 1

        # 計算每個幣種的時間覆蓋
        sym_ts_range = {}
        for e in recent_entries:
            sym = e["sym"]
            if sym not in sym_ts_range:
                sym_ts_range[sym] = {"min": e["ts"], "max": e["ts"]}
            else:
                if e["ts"] < sym_ts_range[sym]["min"]:
                    sym_ts_range[sym]["min"] = e["ts"]
                if e["ts"] > sym_ts_range[sym]["max"]:
                    sym_ts_range[sym]["max"] = e["ts"]

        opp_all = []
        opp_coinw = []
        opp_same_interval = []
        spike_excluded = 0
        coverage_excluded = 0

        for sym, ex_map in multi_ex_syms.items():
            if sym not in rate_sums or len(rate_sums[sym]) < 2:
                continue

            # 時間覆蓋不足 16h 的幣種不納入
            if sym in sym_ts_range:
                try:
                    t_min = datetime.fromisoformat(sym_ts_range[sym]["min"])
                    t_max = datetime.fromisoformat(sym_ts_range[sym]["max"])
                    if (t_max - t_min).total_seconds() / 3600 < 16:
                        coverage_excluded += 1
                        continue
                except Exception:
                    pass

            # 過濾資料不足的交易所（範圍已於上方 group-by 一次算完，這裡直接取用）
            ex_ts_range = ex_ts_ranges.get(sym, {})

            qualified_exs = {}
            for ex, d in rate_sums[sym].items():
                # 只保留目前掃描中存在的交易所（排除已下架/SETTLING 的）
                if ex not in ex_map:
                    continue
                info = ex_ts_range.get(ex)
                if not info or info["count"] < 2:
                    continue
                try:
                    t_min = datetime.fromisoformat(info["min"])
                    t_max = datetime.fromisoformat(info["max"])
                    if (t_max - t_min).total_seconds() / 3600 >= 16:
                        qualified_exs[ex] = d
                except Exception:
                    continue
            if len(qualified_exs) < 2:
                continue

            # === Mode all：全局最高 vs 最低 ===
            sorted_exs = sorted(qualified_exs.items(), key=lambda x: x[1]["sum"])
            low_ex, low_data = sorted_exs[0]
            high_ex, high_data = sorted_exs[-1]
            daily_diff_pct_all = (high_data["sum"] - low_data["sum"]) * 100
            if abs(daily_diff_pct_all) >= 0.5:
                if self._is_spike_driven(sym, high_ex, low_ex, recent_entries):
                    spike_excluded += 1
                else:
                    opp_all.append(self._build_opportunity(sym, ex_map, rate_sums, high_ex, high_data, low_ex, low_data, recent_entries))
            elif daily_diff_pct_all != 0:
                opp_all.append(self._build_opportunity(sym, ex_map, rate_sums, high_ex, high_data, low_ex, low_data, recent_entries))

            # === Mode coinw：以 CoinW 為一側，找差距最大的對手所 ===
            # CoinW 不套用 16h 覆蓋限制（其結算週期短，資料天然較少），
            # 只要 rate_sums 裡有 CoinW 的任何記錄即可；對手所需通過 qualified_exs 篩選
            coinw_raw = rate_sums[sym].get("coinw")
            if coinw_raw and qualified_exs:
                coinw_data = coinw_raw
                best_other_ex = None
                best_diff = 0.0
                # 對手所必須在 qualified_exs（資料充足）
                for ex, d in qualified_exs.items():
                    if ex == "coinw":
                        continue
                    diff = abs(d["sum"] - coinw_data["sum"])
                    if diff > best_diff:
                        best_diff = diff
                        best_other_ex = ex
                if best_other_ex:
                    other_data = qualified_exs[best_other_ex]
                    if other_data["sum"] > coinw_data["sum"]:
                        c_high_ex, c_high_data = best_other_ex, other_data
                        c_low_ex, c_low_data = "coinw", coinw_data
                    else:
                        c_high_ex, c_high_data = "coinw", coinw_data
                        c_low_ex, c_low_data = best_other_ex, other_data
                    daily_diff_pct_coinw = (c_high_data["sum"] - c_low_data["sum"]) * 100
                    spike_ok = True
                    if abs(daily_diff_pct_coinw) >= 0.5:
                        if self._is_spike_driven(sym, c_high_ex, c_low_ex, recent_entries):
                            spike_ok = False
                    if spike_ok:
                        opp_coinw.append(self._build_opportunity(sym, ex_map, rate_sums, c_high_ex, c_high_data, c_low_ex, c_low_data, recent_entries))

            # === Mode same_interval：同結算週期內最高 vs 最低 ===
            interval_groups = {}
            for ex, d in qualified_exs.items():
                rec = ex_map.get(ex)
                interval = rec.funding_interval_h if rec else None
                if interval is None:
                    continue
                interval_groups.setdefault(interval, []).append((ex, d))
            best_si_opp = None
            best_si_diff = 0.0
            for interval, group in interval_groups.items():
                if len(group) < 2:
                    continue
                sorted_group = sorted(group, key=lambda x: x[1]["sum"])
                g_low_ex, g_low_data = sorted_group[0]
                g_high_ex, g_high_data = sorted_group[-1]
                diff = abs(g_high_data["sum"] - g_low_data["sum"])
                if diff > best_si_diff:
                    best_si_diff = diff
                    best_si_opp = (g_high_ex, g_high_data, g_low_ex, g_low_data)
            if best_si_opp:
                g_high_ex, g_high_data, g_low_ex, g_low_data = best_si_opp
                daily_diff_pct_si = (g_high_data["sum"] - g_low_data["sum"]) * 100
                spike_ok = True
                if abs(daily_diff_pct_si) >= 0.5:
                    if self._is_spike_driven(sym, g_high_ex, g_low_ex, recent_entries):
                        spike_ok = False
                if spike_ok:
                    opp_same_interval.append(self._build_opportunity(sym, ex_map, rate_sums, g_high_ex, g_high_data, g_low_ex, g_low_data, recent_entries))

        opp_all.sort(key=lambda x: abs(x["daily_diff_pct"]), reverse=True)
        opp_coinw.sort(key=lambda x: abs(x["daily_diff_pct"]), reverse=True)
        opp_same_interval.sort(key=lambda x: abs(x["daily_diff_pct"]), reverse=True)
        self.meat_flow = opp_all
        self.meat_flow_coinw = opp_coinw
        self.meat_flow_same_interval = opp_same_interval
        qualified = sum(1 for o in opp_all if abs(o["daily_diff_pct"]) >= 0.5)
        duration = time.time() - start_time
        logger.info(f"碎肉流計算：全局{len(opp_all)}個 CoinW{len(opp_coinw)}個 同週期{len(opp_same_interval)}個，{qualified}個日差≥0.5%，排除{spike_excluded}異常+{coverage_excluded}覆蓋不足，耗時{duration:.1f}s")

    def compute_custom_pair_meat_flow(self, ex_a: str, ex_b: str) -> list[dict]:
        """自選兩交易所：過去 24h 每日資費差（從 _rate_history 即時計算）

        規則與 _compute_meat_flow 一致：
        - 幣種需同時存在 ex_a / ex_b（當前掃描）
        - 各交易所 24h 內至少 2 筆整點結算、時間跨度 ≥ 16h
        - 排除單次異常 (_is_spike_driven)
        - 回傳用 _build_opportunity 產生的結構，依 |daily_diff_pct| 由大到小排序
        """
        if not self.last_result:
            return []
        if ex_a == ex_b:
            return []

        sym_exchanges: dict[str, dict] = {}
        for r in self.last_result.records:
            key = r.normalized_symbol or r.symbol
            sym_exchanges.setdefault(key, {})[r.exchange] = r

        # 只看兩個交易所都有的幣種
        target_syms = {
            sym: ex_map for sym, ex_map in sym_exchanges.items()
            if ex_a in ex_map and ex_b in ex_map
        }
        if not target_syms:
            return []

        cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        recent_entries = []
        for e in self._rate_history:
            if e["ts"] <= cutoff_24h:
                continue
            if e["ex"] not in (ex_a, ex_b):
                continue
            if e["sym"] not in target_syms:
                continue
            try:
                dt = datetime.fromisoformat(e["ts"])
                if dt.minute != 0:
                    continue
            except Exception:
                continue
            recent_entries.append(e)

        # 分組加總
        rate_sums: dict[str, dict] = {}
        for e in recent_entries:
            sym = e["sym"]
            ex = e["ex"]
            rate_sums.setdefault(sym, {}).setdefault(ex, {"sum": 0.0, "count": 0})
            rate_sums[sym][ex]["sum"] += e["rate"]
            rate_sums[sym][ex]["count"] += 1

        opportunities = []
        for sym, ex_map in target_syms.items():
            sums = rate_sums.get(sym)
            if not sums or ex_a not in sums or ex_b not in sums:
                continue

            # 時間覆蓋檢查（與 _compute_meat_flow 一致：≥ 16h 且 ≥ 2 筆）
            ex_ts_range: dict[str, dict] = {}
            for e in recent_entries:
                if e["sym"] != sym:
                    continue
                ex = e["ex"]
                info = ex_ts_range.setdefault(ex, {"min": e["ts"], "max": e["ts"], "count": 0})
                if e["ts"] < info["min"]:
                    info["min"] = e["ts"]
                if e["ts"] > info["max"]:
                    info["max"] = e["ts"]
                info["count"] += 1

            # 至少 2 筆即可。原本還檢查 16h 跨度，但對「累積期不足的新交易所」
            # （deepcoin 剛接入時、_rate_history 才幾小時資料）會大量誤殺。
            # 現在 _append_scan_to_history 每次掃描都會補當期 ts，跨度會自然成長
            ok = True
            for ex in (ex_a, ex_b):
                info = ex_ts_range.get(ex)
                if not info or info["count"] < 2:
                    ok = False
                    break
            if not ok:
                continue

            a_data = sums[ex_a]
            b_data = sums[ex_b]
            if a_data["sum"] >= b_data["sum"]:
                high_ex, high_data = ex_a, a_data
                low_ex, low_data = ex_b, b_data
            else:
                high_ex, high_data = ex_b, b_data
                low_ex, low_data = ex_a, a_data

            daily_diff_pct = (high_data["sum"] - low_data["sum"]) * 100
            if abs(daily_diff_pct) >= 0.5:
                if self._is_spike_driven(sym, high_ex, low_ex, recent_entries):
                    continue

            opportunities.append(
                self._build_opportunity(sym, ex_map, rate_sums, high_ex, high_data, low_ex, low_data, recent_entries)
            )

        opportunities.sort(key=lambda x: abs(x["daily_diff_pct"]), reverse=True)
        return opportunities

    async def _update_meat_flow(self):
        """增量抓取歷史費率 + 重新計算碎肉流"""
        if self._meat_updating:
            logger.debug("碎肉流：已有更新任務在執行，跳過")
            return
        self._meat_updating = True
        try:
            await self._do_update_meat_flow()
        finally:
            self._meat_updating = False
            self._meat_bootstrapping = False

    def _needs_settled_fetch(self, record, now_dt) -> bool:
        """本輪是否需要對這個 (交易所,幣) 抓實際結算費率。

        最佳化只跳過「能算出結算整點、且該整點已是舊結算（落在 lookback 窗外）」的合約——
        結算費率產生後不變，早抓過的重抓會被 _merge_entries 去重丟掉。
        無法判斷結算時間者（funding_time/interval 為 None、或算出的結算非整點）一律照抓，
        維持改動前「全量丟給 _fetch_all_histories」的行為，避免這類合約被靜默漏抓。
        """
        if not record.funding_time or not record.funding_interval_h:
            return True
        last_settle = record.funding_time - timedelta(hours=record.funding_interval_h)
        last_settle = last_settle.replace(second=0, microsecond=0)
        # 算出的結算非整點：無法安全套用整點窗，照抓（維持改動前行為）
        if last_settle.minute != 0:
            return True
        age = now_dt - last_settle
        # 未來結算：尚無可抓（與 _append_scan_to_history 略過 last_settle > now 一致）
        if age < timedelta(0):
            return False
        # 落在窗內＝剛結算、需抓真值覆蓋 placeholder；窗外＝早已抓過，跳過
        return age <= timedelta(minutes=_MEAT_SETTLE_LOOKBACK_MINUTES)

    async def _fetch_settled_and_update_meat(self, all_records):
        """掃描後從各交易所抓最新結算費率，更新碎肉流快取並重算"""
        if self._meat_updating:
            return
        self._meat_updating = True
        try:
            # 1. 清理已下架的幣種
            before = len(self._rate_history)
            self._rate_history = _purge_delisted(self._rate_history, all_records)
            removed = before - len(self._rate_history)
            if removed:
                logger.info(f"碎肉流：清理 {removed} 筆無效歷史")

            # 2. 建立多所幣種列表
            sym_exchanges = {}
            for r in all_records:
                sym = r.normalized_symbol or r.symbol
                if sym not in sym_exchanges:
                    sym_exchanges[sym] = {}
                sym_exchanges[sym][r.exchange] = r
            multi_ex_syms = {s: recs for s, recs in sym_exchanges.items() if len(recs) >= 2}

            # 2b. 只保留「剛結算」的 (交易所,幣)。結算費率一旦產生就不變，已抓過的重抓會被
            #     _merge_entries 去重丟掉；改成只打剛結算的合約，把每 5 分鐘全量重抓
            #     （~數千請求）降為結算整點附近一小波，其餘輪次趨近 0。窗外幣種的結算值靠
            #     先前輪次已抓入 + _append_scan_to_history 的 placeholder 兜底。
            now_dt = datetime.now(timezone.utc)
            due_syms = {}
            for s, recs in multi_ex_syms.items():
                due = {ex: r for ex, r in recs.items() if self._needs_settled_fetch(r, now_dt)}
                if due:
                    due_syms[s] = due

            # 3. 對剛結算的合約抓最近 9 小時結算費率（涵蓋最新一期，結算後 ~30 分鐘內必有資料）
            if due_syms:
                since_ms = int((now_dt - timedelta(hours=9)).timestamp() * 1000)
                new_entries = await self._fetch_all_histories(due_syms, since_ms)
                added = self._merge_entries(new_entries)
                logger.info(f"碎肉流：{len(due_syms)} 個剛結算幣種，補充 {added} 筆結算費率")
            else:
                logger.debug("碎肉流：本輪無剛結算幣種，跳過歷史抓取")

            # 4. 重算碎肉流
            self._compute_meat_flow()
        except Exception as e:
            logger.warning(f"碎肉流掃描更新失敗: {e}")
        finally:
            self._meat_updating = False

    def _append_scan_to_history(self, records):
        """將掃描結果的當期費率直接存入 _rate_history（取代慢速的逐幣歷史 API）"""
        # 清理已下架合約的歷史（只針對本輪有回資料的交易所，見 _purge_delisted）
        before = len(self._rate_history)
        self._rate_history = _purge_delisted(self._rate_history, records)
        removed = before - len(self._rate_history)
        if removed:
            logger.info(f"碎肉流：清理 {removed} 筆無效歷史（交易所已無該合約）")

        existing = set((r["ts"], r["ex"], r["sym"]) for r in self._rate_history)
        added = 0
        for r in records:
            if not r.funding_time or not r.funding_interval_h:
                continue
            # funding_time 是下次結算時間，減去 interval 得到上次結算時間
            last_settle = r.funding_time - timedelta(hours=r.funding_interval_h)
            # 對齊到整點（去掉秒和微秒）
            last_settle = last_settle.replace(second=0, microsecond=0)
            # 只保留整點 + 已過去的結算
            if last_settle.minute != 0:
                continue
            if last_settle > datetime.now(timezone.utc):
                continue
            sym = r.normalized_symbol or r.symbol
            ts = last_settle.isoformat()
            key = (ts, r.exchange, sym)
            if key not in existing:
                self._rate_history.append({
                    "ts": ts,
                    "ex": r.exchange,
                    "sym": sym,
                    "rate": r.funding_rate,
                })
                existing.add(key)
                added += 1
        if added:
            # 清理超過 72h 的舊資料
            cutoff_72h = (datetime.now(timezone.utc) - timedelta(hours=72)).isoformat()
            self._rate_history = [e for e in self._rate_history if e["ts"] > cutoff_72h]
            self._save_rate_history()
            logger.info(f"碎肉流：掃描追加 {added} 筆結算記錄到快取")

    def _merge_entries(self, all_entries):
        """去重並合併新資料到 _rate_history，API 資料覆蓋掃描預測值"""
        seen = set()
        new_entries = []
        for e in all_entries:
            key = (e["ts"], e["ex"], e["sym"])
            if key not in seen:
                seen.add(key)
                new_entries.append(e)

        existing = {(r["ts"], r["ex"], r["sym"]): r for r in self._rate_history}
        added = 0
        updated = 0
        for e in new_entries:
            key = (e["ts"], e["ex"], e["sym"])
            if key in existing:
                # API 回傳的是實際結算值，覆蓋掃描時的預測值
                if existing[key]["rate"] != e["rate"]:
                    existing[key]["rate"] = e["rate"]
                    updated += 1
            else:
                self._rate_history.append(e)
                existing[key] = e
                added += 1

        cutoff_72h = (datetime.now(timezone.utc) - timedelta(hours=72)).isoformat()
        self._rate_history = [r for r in self._rate_history if r["ts"] > cutoff_72h]
        if added or updated:
            self._save_rate_history()
        return added + updated

    def _find_undercovered_syms(self, multi_ex_syms):
        """找出快取中有交易所覆蓋不足的幣種（逐交易所檢查）"""
        cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

        # 統計每個 (sym, ex) 的資料筆數
        sym_ex_count = {}
        for e in self._rate_history:
            if e["ts"] <= cutoff_24h:
                continue
            key = (e["sym"], e["ex"])
            sym_ex_count[key] = sym_ex_count.get(key, 0) + 1

        undercovered = {}
        for sym, recs in multi_ex_syms.items():
            # 檢查每個交易所是否有至少 2 筆 24h 內的資料
            for ex in recs:
                count = sym_ex_count.get((sym, ex), 0)
                if count < 2:
                    undercovered[sym] = recs
                    break

        return undercovered

    async def _do_update_meat_flow(self):
        """只回補覆蓋不足的幣種（日常更新由 scan-append 處理）"""
        if not self.last_result:
            return

        # 收集所有幣種及其所在交易所
        sym_exchanges = {}
        for r in self.last_result.records:
            key = r.normalized_symbol or r.symbol
            if key not in sym_exchanges:
                sym_exchanges[key] = {}
            sym_exchanges[key][r.exchange] = r

        multi_ex_syms = {sym: recs for sym, recs in sym_exchanges.items() if len(recs) >= 2}
        if not multi_ex_syms:
            self.meat_flow = []
            return

        # 只回補覆蓋不足的幣種（< 16h）
        backfill_syms = self._find_undercovered_syms(multi_ex_syms)
        if not backfill_syms:
            logger.info("碎肉流：所有幣種覆蓋充足，無需回補")
            self._compute_meat_flow()
            return

        start_time = time.time()
        logger.info(f"碎肉流：回補 {len(backfill_syms)} 個覆蓋不足的幣種...")
        full_since = int((datetime.now(timezone.utc) - timedelta(hours=24)).timestamp() * 1000)
        backfill_entries = await self._fetch_all_histories(backfill_syms, full_since)
        added = self._merge_entries(backfill_entries)
        duration = time.time() - start_time
        logger.info(f"碎肉流：回補完成，新增 {added} 筆，耗時 {duration:.1f}s")

        # 重新計算
        self._compute_meat_flow()

    def fetch_symbol_from_cache(self, symbol: str) -> list:
        """從最近一次掃描快取讀取某幣種在各交易所的費率（毫秒級）"""
        if not self.last_result:
            return []
        base = symbol.split("/")[0].upper()
        return [
            r for r in self.last_result.records
            if (r.normalized_symbol or r.symbol).split("/")[0].upper() == base
        ]

    async def fetch_symbol_realtime(self, symbol: str) -> list:
        """即時查詢某幣種在各交易所的最新費率，並更新快取"""
        from models import FundingRecord
        from services.normalizer import normalize_rate_to_8h, calc_annual_rate

        results = []
        base_coin = symbol.split("/")[0].upper()

        def _get_exchange_actual_symbol(exchange_name: str) -> str | None:
            """從掃描快取中取出某交易所對此幣種的實際合約名（base 部分）。
            例如 CL 在 CoinW 上叫 XTI，在 MEXC 上叫 USOIL。
            回傳 None 表示該交易所沒有此幣種。"""
            if not self.last_result:
                return None
            for r in self.last_result.records:
                if r.exchange == exchange_name and (r.normalized_symbol or r.symbol).split("/")[0].upper() == base_coin:
                    return r.symbol.split("/")[0].upper()
            return None

        async def _fetch_ccxt(ex, sym):
            """ccxt 交易所 - 費率+深度並行查詢"""
            from exchanges.ccxt_exchange import _extract_interval_h, _parse_interval_str, _guess_interval_from_next_funding

            if not ex._exchange.markets:
                await ex._exchange.load_markets()
            if sym not in ex._exchange.markets:
                return None

            # 費率 + 深度並行
            ob_limit = 20 if ex.exchange_id == "kucoinfutures" else 5
            fr_task = ex._exchange.fetch_funding_rate(sym)
            ob_task = ex._exchange.fetch_order_book(sym, limit=ob_limit)
            fr, ob = await asyncio.gather(fr_task, ob_task, return_exceptions=True)

            if isinstance(fr, Exception) or not fr or fr.get("fundingRate") is None:
                return None

            rate = fr["fundingRate"]
            market = ex._exchange.markets.get(sym, {})
            info = market.get("info", {})

            interval_h = _extract_interval_h(info)
            ccxt_interval = fr.get("interval")
            if ccxt_interval:
                interval_h = _parse_interval_str(ccxt_interval)
            elif interval_h == 8 and self.last_result:
                for cached in self.last_result.records:
                    if cached.exchange == ex.name and cached.symbol == sym:
                        interval_h = cached.funding_interval_h or 8
                        break

            # 交叉校驗：用下次收費時間反推結算週期，偵測 ccxt interval 缺漏/快取過時
            # （與全量掃描 _fetch_via_ccxt 同邏輯：啟發式只能推出「至多」週期，僅在推算值更短時覆寫）
            nft_for_check = fr.get("nextFundingTimestamp") or fr.get("fundingTimestamp")
            if nft_for_check and interval_h > 1:
                guessed_h = _guess_interval_from_next_funding(nft_for_check)
                if guessed_h < interval_h:
                    if ex.exchange_id == "bingx":
                        # BingX 啟發式分不清偶數小時的 1h/2h，做即時 API 查詢取精確值
                        exact_h = await ex._verify_bingx_interval(sym)
                        if exact_h and exact_h < interval_h:
                            interval_h = exact_h
                        elif not exact_h:
                            interval_h = guessed_h
                    else:
                        interval_h = guessed_h

            rate_8h = normalize_rate_to_8h(rate, interval_h)

            bid, ask = None, None
            if not isinstance(ob, Exception) and ob:
                if ob.get("bids") and ob.get("asks"):
                    bid = ob["bids"][0][0]
                    ask = ob["asks"][0][0]

            funding_time = None
            nft = fr.get("nextFundingTimestamp")
            ft = fr.get("fundingTimestamp")
            if nft:
                try:
                    funding_time = datetime.fromtimestamp(int(nft) / 1000, tz=timezone.utc)
                except Exception:
                    pass
            elif ft:
                try:
                    ft_time = datetime.fromtimestamp(int(ft) / 1000, tz=timezone.utc)
                    now = datetime.now(timezone.utc)
                    if ft_time > now:
                        funding_time = ft_time
                    else:
                        funding_time = ft_time + timedelta(hours=interval_h)
                except Exception:
                    pass
            elif fr.get("fundingDatetime"):
                try:
                    funding_time = datetime.fromisoformat(fr["fundingDatetime"].replace("Z", "+00:00"))
                except Exception:
                    pass

            # 偵測即將下架
            import time as _rt
            is_dl = False
            if ex.exchange_id == "gateio":
                is_dl = bool(info.get("in_delisting"))
            elif ex.exchange_id == "binance":
                is_dl = info.get("status", "TRADING") not in ("TRADING", "")
                if not is_dl:
                    delivery_ms = info.get("deliveryDate", 0)
                    if delivery_ms and delivery_ms > 0:
                        days_left = (delivery_ms - _rt.time() * 1000) / 86400000
                        if days_left <= 30:
                            is_dl = True
            elif ex.exchange_id == "bybit":
                is_dl = info.get("status", "Trading") not in ("Trading", "")
                if not is_dl:
                    delivery_ts = info.get("deliveryTime")
                    if delivery_ts:
                        try:
                            delivery_ms = int(delivery_ts)
                            if delivery_ms > 0 and (delivery_ms - _rt.time() * 1000) / 86400000 <= 7:
                                is_dl = True
                        except (ValueError, TypeError):
                            pass
            elif ex.exchange_id == "bitget":
                is_dl = info.get("symbolStatus", "normal") not in ("normal", "")
            elif ex.exchange_id == "okx":
                is_dl = info.get("state", "live") not in ("live", "")

            # 補充 delisting_time（BN deliveryDate）
            rt_delisting_time = None
            if is_dl and ex.exchange_id == "binance":
                delivery_ms_val = info.get("deliveryDate", 0)
                if delivery_ms_val:
                    try:
                        rt_delisting_time = datetime.fromtimestamp(delivery_ms_val / 1000, tz=timezone.utc)
                    except Exception:
                        pass

            record = FundingRecord(
                exchange=ex.name, symbol=sym,
                funding_rate=rate_8h, annual_rate=calc_annual_rate(rate, interval_h),
                funding_interval_h=interval_h, bid_price=bid, ask_price=ask,
                mark_price=fr.get("markPrice"), index_price=fr.get("indexPrice"),
                funding_time=funding_time,
                is_delisting=is_dl,
                delisting_time=rt_delisting_time,
            )
            record.normalized_symbol = _normalize_symbol(sym, ex.name)
            return record

        async def _fetch_coinw(ex):
            """CoinW - REST API 即時費率 + 深度並行（不開 WebSocket）"""
            session = await ex._get_session()

            # 從掃描快取取 interval 和實際合約名（避免額外 API 呼叫）
            # CoinW 可能用不同幣名（如 XTI 對應 CL），需用實際合約名查 API
            interval_h = 8
            instrument = base_coin  # 預設用 canonical name
            sym = f"{base_coin}/USDT:USDT"
            found_in_scan = False
            if self.last_result:
                for r in self.last_result.records:
                    if r.exchange == "coinw" and (r.normalized_symbol or r.symbol).split("/")[0].upper() == base_coin:
                        interval_h = r.funding_interval_h or 8
                        # 使用 CoinW 上的實際合約名（可能是別名如 XTI）
                        instrument = r.symbol.split("/")[0].upper()
                        sym = r.symbol
                        found_in_scan = True
                        break
            if not found_in_scan:
                # 合約不在上次掃描的 instruments(online) 列表中，可能已下架
                return None

            # 費率：WS 即時訂閱（REST /v1/perpum/fundingRate 嚴重滯後，不使用）
            async def _get_rate():
                try:
                    import websockets, json as _json
                    ws_url = "wss://ws.futurescw.com/perpum"
                    async with websockets.connect(ws_url, ping_interval=None, close_timeout=5) as ws:
                        await ws.send(_json.dumps({
                            "event": "sub",
                            "params": {"biz": "futures", "type": "funding_rate", "pairCode": instrument.upper()}
                        }))
                        for _ in range(5):
                            raw = await asyncio.wait_for(ws.recv(), timeout=5)
                            msg = _json.loads(raw)
                            data = msg.get("data", {})
                            if "r" in data:
                                return float(data["r"])
                except Exception:
                    pass
                return None

            async def _get_depth():
                async with session.get(
                    "https://api.coinw.com/v1/perpumPublic/depth",
                    params={"base": instrument},
                ) as resp:
                    if resp.status == 200:
                        ddata = await resp.json()
                        if ddata.get("code") == 0:
                            d = ddata.get("data", {})
                            bids = d.get("bids", [])
                            asks = d.get("asks", [])
                            if bids and asks:
                                return float(bids[0]["p"]), float(asks[0]["p"])
                return None, None

            rate_result, depth_result = await asyncio.gather(
                _get_rate(), _get_depth(), return_exceptions=True
            )

            rate = rate_result if not isinstance(rate_result, Exception) else None
            if rate is None:
                return None

            bid, ask = (None, None)
            if not isinstance(depth_result, Exception) and depth_result:
                bid, ask = depth_result

            rate_8h = normalize_rate_to_8h(rate, "coinw")

            now = datetime.now(timezone.utc)
            interval_secs = int(interval_h * 3600)
            midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
            elapsed = int((now - midnight).total_seconds())
            next_secs = ((elapsed // interval_secs) + 1) * interval_secs
            funding_time = midnight + timedelta(seconds=next_secs)

            record = FundingRecord(
                exchange="coinw", symbol=sym,
                funding_rate=rate_8h, annual_rate=calc_annual_rate(rate_8h, interval_h),
                funding_interval_h=interval_h, bid_price=bid, ask_price=ask,
                funding_time=funding_time,
            )
            record.normalized_symbol = _normalize_symbol(sym, "coinw")
            return record

        async def _fetch_hyperliquid(ex):
            """Hyperliquid - REST API"""
            actual_base_hl = _get_exchange_actual_symbol("hyperliquid") or base_coin
            async with make_session() as session:
                payload = {"type": "metaAndAssetCtxs"}
                async with session.post("https://api.hyperliquid.xyz/info", json=payload) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()

            meta = data[0] if isinstance(data, list) and len(data) >= 2 else {}
            asset_ctxs = data[1] if isinstance(data, list) and len(data) >= 2 else []
            universes = meta.get("universe", [])

            for i, asset in enumerate(universes):
                if asset.get("isDelisted", False):
                    continue
                if asset.get("name", "").upper() == actual_base_hl:
                    ctx = asset_ctxs[i] if i < len(asset_ctxs) else {}
                    rate = float(ctx.get("funding", 0))
                    mark = float(ctx.get("markPx", 0))
                    sym = f"{actual_base_hl}/USDT:USDT"

                    # 取 bid/ask（L2 orderbook）
                    bid, ask = None, None
                    try:
                        async with make_session() as s2:
                            payload2 = {"type": "l2Book", "coin": actual_base_hl}
                            async with s2.post("https://api.hyperliquid.xyz/info", json=payload2) as resp2:
                                if resp2.status == 200:
                                    book = await resp2.json()
                                    levels = book.get("levels", [])
                                    if len(levels) >= 2 and levels[0] and levels[1]:
                                        bid = float(levels[0][0].get("px", 0))
                                        ask = float(levels[1][0].get("px", 0))
                    except Exception:
                        pass

                    record = FundingRecord(
                        exchange="hyperliquid", symbol=sym,
                        funding_rate=rate, annual_rate=calc_annual_rate(rate, 1),
                        funding_interval_h=1, mark_price=mark,
                        bid_price=bid, ask_price=ask,
                    )
                    record.normalized_symbol = _normalize_symbol(sym, "hyperliquid")
                    return record
            return None

        async def _fetch_aster(ex):
            """Aster - REST API"""
            actual_base = _get_exchange_actual_symbol("aster") or base_coin
            async with make_session() as session:
                sym_query = f"{actual_base}USDT"
                async with session.get(
                    "https://fapi.asterdex.com/fapi/v1/premiumIndex",
                    params={"symbol": sym_query},
                ) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()

            if isinstance(data, list):
                data = data[0] if data else {}

            rate = data.get("lastFundingRate")
            if rate is None:
                return None
            rate = float(rate)

            # 查詢實際結算週期
            interval_h = 8
            try:
                async with make_session() as s2:
                    async with s2.get("https://fapi.asterdex.com/fapi/v1/fundingInfo") as resp2:
                        if resp2.status == 200:
                            fi_data = await resp2.json()
                            for item in fi_data:
                                if item.get("symbol") == sym_query:
                                    h = item.get("fundingIntervalHours")
                                    if h:
                                        interval_h = float(h)
                                    break
            except Exception:
                pass

            rate_8h = normalize_rate_to_8h(rate, interval_h)
            sym = f"{actual_base}/USDT:USDT"

            funding_time = None
            nft = data.get("nextFundingTime")
            if nft:
                try:
                    funding_time = datetime.fromtimestamp(int(nft) / 1000, tz=timezone.utc)
                except Exception:
                    pass

            # 取 bid/ask（orderbook）
            bid, ask = None, None
            try:
                async with make_session() as s2:
                    async with s2.get(
                        "https://fapi.asterdex.com/fapi/v1/depth",
                        params={"symbol": sym_query, "limit": 5},
                    ) as resp2:
                        if resp2.status == 200:
                            ob = await resp2.json()
                            bids = ob.get("bids", [])
                            asks = ob.get("asks", [])
                            if bids and asks:
                                bid = float(bids[0][0])
                                ask = float(asks[0][0])
            except Exception:
                pass

            record = FundingRecord(
                exchange="aster", symbol=sym,
                funding_rate=rate_8h, annual_rate=calc_annual_rate(rate_8h, interval_h),
                funding_interval_h=interval_h, mark_price=float(data.get("markPrice", 0)) or None,
                index_price=float(data.get("indexPrice", 0)) or None,
                funding_time=funding_time,
                bid_price=bid, ask_price=ask,
            )
            record.normalized_symbol = _normalize_symbol(sym, "aster")
            return record

        async def _fetch_tradexyz(ex):
            """Trade.xyz - Hyperliquid HIP-3 perpDEX"""
            actual_base_xyz = _get_exchange_actual_symbol("tradexyz") or base_coin
            async with make_session() as session:
                payload = {"type": "metaAndAssetCtxs", "dex": "xyz"}
                async with session.post("https://api.hyperliquid.xyz/info", json=payload) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()

            meta = data[0] if isinstance(data, list) and len(data) >= 2 else {}
            asset_ctxs = data[1] if isinstance(data, list) and len(data) >= 2 else []
            universes = meta.get("universe", [])

            # trade.xyz 的 coin 名稱帶 xyz: 前綴，需要匹配去掉前綴後的 base
            for i, asset in enumerate(universes):
                coin_name = asset.get("name", "")
                # 去掉 xyz: 前綴來比對
                coin_base = coin_name.split(":", 1)[1] if ":" in coin_name else coin_name
                if coin_base.upper() == actual_base_xyz:
                    ctx = asset_ctxs[i] if i < len(asset_ctxs) else {}
                    rate = float(ctx.get("funding", 0))
                    mark = float(ctx.get("markPx", 0))
                    sym = f"{actual_base_xyz}/USDT:USDT"

                    # 取 bid/ask
                    bid, ask = None, None
                    try:
                        async with make_session() as s2:
                            payload2 = {"type": "l2Book", "coin": coin_name}
                            async with s2.post("https://api.hyperliquid.xyz/info", json=payload2) as resp2:
                                if resp2.status == 200:
                                    book = await resp2.json()
                                    levels = book.get("levels", [])
                                    if len(levels) >= 2 and levels[0] and levels[1]:
                                        bid = float(levels[0][0].get("px", 0))
                                        ask = float(levels[1][0].get("px", 0))
                    except Exception:
                        pass

                    record = FundingRecord(
                        exchange="tradexyz", symbol=sym,
                        funding_rate=rate, annual_rate=calc_annual_rate(rate, 1),
                        funding_interval_h=1, mark_price=mark,
                        bid_price=bid, ask_price=ask,
                    )
                    record.normalized_symbol = _normalize_symbol(sym, "tradexyz")
                    return record
            return None

        async def _fetch_bingx(ex):
            """BingX - REST premiumIndex(費率+週期+下次結算) + ticker(買賣一)，per-symbol 查詢。

            BingX 的 WS markPrice 串流不帶資費、且 ccxt 即時路徑不涵蓋 BingX，
            導致詳情頁的 BingX 資費只能靠 5 分鐘整批掃描，結算前快速衝向上限時會顯示舊值。
            這裡打開詳情就即時抓一次。與 BingxExchange.fetch_funding_rates 一致：
            funding_rate 存「原始 per-period 值」（不做 8h 正規化），避免顯示值跳成 8 倍。
            """
            actual_base = _get_exchange_actual_symbol("bingx") or base_coin
            bx_sym = f"{actual_base}-USDT"
            base_url = "https://open-api.bingx.com/openApi/swap/v2/quote"
            session = await ex._get_session()

            async def _prem():
                async with session.get(f"{base_url}/premiumIndex", params={"symbol": bx_sym}) as resp:
                    return await resp.json()

            async def _tick():
                async with session.get(f"{base_url}/ticker", params={"symbol": bx_sym}) as resp:
                    return await resp.json()

            prem_r, tick_r = await asyncio.gather(_prem(), _tick(), return_exceptions=True)
            if isinstance(prem_r, Exception):
                return None
            pdata = (prem_r or {}).get("data") or {}
            if isinstance(pdata, list):
                pdata = pdata[0] if pdata else {}
            rate = pdata.get("lastFundingRate")
            if rate is None:
                return None
            rate = float(rate)

            try:
                interval_h = float(pdata.get("fundingIntervalHours") or 8)
            except (TypeError, ValueError):
                interval_h = 8

            mark = pdata.get("markPrice")
            index = pdata.get("indexPrice")
            mark = float(mark) if mark not in (None, "") else None
            index = float(index) if index not in (None, "") else None

            funding_time = None
            nft = pdata.get("nextFundingTime")
            if nft:
                try:
                    funding_time = datetime.fromtimestamp(int(nft) / 1000, tz=timezone.utc)
                except Exception:
                    pass

            bid, ask = None, None
            if not isinstance(tick_r, Exception):
                tdata = (tick_r or {}).get("data") or {}
                if isinstance(tdata, list):
                    tdata = tdata[0] if tdata else {}
                try:
                    bid = float(tdata["bidPrice"]) if tdata.get("bidPrice") not in (None, "") else None
                    ask = float(tdata["askPrice"]) if tdata.get("askPrice") not in (None, "") else None
                except (TypeError, ValueError, KeyError):
                    pass

            sym = f"{actual_base}/USDT:USDT"
            record = FundingRecord(
                exchange="bingx", symbol=sym,
                funding_rate=rate, annual_rate=calc_annual_rate(rate, interval_h),
                funding_interval_h=interval_h, bid_price=bid, ask_price=ask,
                mark_price=mark, index_price=index, funding_time=funding_time,
            )
            record.normalized_symbol = _normalize_symbol(sym, "bingx")
            return record

        async def _fetch_one(ex):
            variants = self._get_symbol_variants(symbol, ex.name)
            try:
                if ex.name == "coinw":
                    return await _fetch_coinw(ex)
                elif ex.name == "hyperliquid":
                    return await _fetch_hyperliquid(ex)
                elif ex.name == "tradexyz":
                    return await _fetch_tradexyz(ex)
                elif ex.name == "aster":
                    return await _fetch_aster(ex)
                elif ex.name == "bingx":
                    return await _fetch_bingx(ex)
                elif ex.name == "lbank":
                    # 批次費率無 bid/ask，按需用單合約訂單簿 marketOrder 補上
                    for r in await ex.fetch_funding_rates():
                        if r.symbol in variants:
                            bid, ask = await ex.fetch_bbo(r.symbol.split("/")[0])
                            r.bid_price = bid
                            r.ask_price = ask
                            r.normalized_symbol = _normalize_symbol(r.symbol, ex.name)
                            return r
                    return None
                elif hasattr(ex, '_exchange'):
                    for sym in variants:
                        result = await _fetch_ccxt(ex, sym)
                        if result:
                            return result
                else:
                    # WS-first / REST-bulk 自製交易所（bybit、binance、okx 等）：
                    # bulk 讀的是 WS 暖快取（或少量 REST），直接取再挑出該幣
                    for r in await ex.fetch_funding_rates():
                        if r.symbol in variants:
                            r.normalized_symbol = _normalize_symbol(r.symbol, ex.name)
                            return r
            except Exception as e:
                logger.debug(f"[{ex.name}] realtime {symbol} 失敗: {e}")
            return None

        # 只查上次掃描中有此幣的交易所（避免對無此幣的交易所等 timeout）
        # DEX（hyperliquid、tradexyz）用全量 API，快且不浪費，一律查
        target_exchanges = self._exchanges
        if self.last_result:
            scan_exchanges = set()
            for r in self.last_result.records:
                norm = r.normalized_symbol or r.symbol
                if norm.split("/")[0].upper() == base_coin:
                    scan_exchanges.add(r.exchange)
            if scan_exchanges:
                always_query = {"hyperliquid", "tradexyz", "lighter", "lighter_rh"}  # 全量 API，不需過濾
                target_exchanges = [
                    ex for ex in self._exchanges
                    if ex.name in scan_exchanges or ex.name in always_query
                ]

        tasks_results = await asyncio.gather(
            *[asyncio.wait_for(_fetch_one(ex), timeout=15) for ex in target_exchanges],
            return_exceptions=True,
        )

        for r in tasks_results:
            if isinstance(r, Exception) or r is None:
                continue
            results.append(r)

        # 同步更新所有快取
        if self.last_result and results:
            by_ex = {r.exchange: r for r in results}

            # 1. 更新掃描快取 (費率總覽)
            for new_r in results:
                for i, old_r in enumerate(self.last_result.records):
                    if old_r.exchange == new_r.exchange and (
                        old_r.normalized_symbol == new_r.normalized_symbol
                        or old_r.symbol == new_r.symbol
                    ):
                        self.last_result.records[i] = new_r
                        break

            # 2. 更新套利偵測的價差 + 預計利潤（做多所ask / 做空所bid - 1）
            if self.last_result.arbitrage:
                for opp in self.last_result.arbitrage:
                    if opp.symbol != symbol:
                        continue
                    short_r = by_ex.get(opp.short_exchange)
                    long_r = by_ex.get(opp.long_exchange)
                    if short_r and long_r and long_r.ask_price and short_r.bid_price and short_r.bid_price > 0:
                        opp.spread_pct = round((long_r.ask_price / short_r.bid_price - 1) * 100, 4)
                        opp.estimated_profit = round(opp.rate_diff * 100 - opp.spread_pct, 4)

            # 3. 更新碎肉流的價差（做多所ask / 做空所bid - 1）
            if self.meat_flow:
                for opp in self.meat_flow:
                    if opp["symbol"] != symbol:
                        continue
                    high_rec = by_ex.get(opp["high_exchange"])  # 做空所
                    low_rec = by_ex.get(opp["low_exchange"])    # 做多所
                    if low_rec and high_rec and low_rec.ask_price and high_rec.bid_price and high_rec.bid_price > 0:
                        opp["spread_pct"] = round((low_rec.ask_price / high_rec.bid_price - 1) * 100, 4)
                    break

            # 4. 存檔（重啟保留）
            self._save_cache()

        logger.info(f"即時查詢 {symbol}：{len(results)} 個交易所回傳")
        return results

    @staticmethod
    def _ordered_unique(items) -> list[str]:
        """保序去重（dict 保留插入序 —— 這正是 set 沒有、而呼叫端都需要的東西）"""
        return list(dict.fromkeys(x for x in items if x))

    def _get_symbol_variants(self, symbol: str, exchange: str = None) -> list[str]:
        """收集某幣種在指定交易所上所有可能的 symbol 名稱，【依可信度由高到低排序】。

        ⚠ 回傳型別從 set 改成 list 是根因層修正，不是風格調整（2026-08-07 溢價圖空白事件）：
          這個清單裝的是「一堆互斥的猜測」，其中至少一半保證不存在，語意上需要優先序；
          但原本宣告成 set —— 沒有順序，且 Python 字串 hash 受 PYTHONHASHSEED 隨機化，
          於是 `next(iter(variants))` 這種「取一個當答案」的呼叫端變成後端開機時抽籤：
          ACE 的變體是 {ACE, 1000ACE}，抽中 1000ACE 就送出 1000ACEUSDT，
          Binance 回 400 Invalid symbol → 溢價圖整片空白，重啟後壞的幣種還會換一批。
          實測 Binance 526 個 USDT 永續【全部】有這個風險，沒有一個安全。

        優先序：
          1. 掃描結果裡該交易所回報的【真實 raw symbol】—— 交易所自己講的名字，
             唯一 100% 正確的來源，也是唯一能正確處理 1000PEPE / ONSTOCK / AAPLX /
             BingX NC 系列這些命名的來源
          2. 呼叫端傳進來的原始 symbol
          3. base / stripped 的標準寫法
          4. 人工維護的別名表（交易所專屬優先於全域 —— 專屬的比較精準）
          5. 猜測型變體（1000{base}、{base}STOCK）—— 永遠排最後。
             它們約一半機率根本不存在，更糟的是可能撞到別的資產：
             Binance 的 CATUSDT 是 contractType=TRADIFI_PERPETUAL 的 Caterpillar
             股票永續，不是迷因幣 1000CAT，取錯不會報錯、會靜默畫出完全錯的曲線。
             這種候選絕不可以贏過權威來源。

        既有 5 個 `for sym in variants` 的呼叫端一行都不用改，就自動從
        「隨機第一個成功」升級成「最可信的先試」。
        """
        base = symbol.split("/")[0].upper()
        stripped, _ = _strip_1000x(base)  # 若 base=1000PEPE 則 stripped=PEPE，否則同 base

        # --- 1. 掃描結果反查真實 raw symbol（權威來源，排最前）---
        raw_exact, raw_norm = [], []
        if exchange and self.last_result:
            for r in self.last_result.records:
                if r.exchange != exchange:
                    continue
                if r.symbol == symbol:                      # 直接傳 raw symbol 進來
                    raw_exact.append(r.symbol)
                elif (r.normalized_symbol or r.symbol) == symbol:
                    raw_norm.append(r.symbol)
            if len(raw_exact) + len(raw_norm) > 1:
                # 同一所對同一個正規化名字有多筆 raw symbol → 真的有同名不同標的的疑慮
                # （例：Binance 的 1000CAT 迷因幣 vs CAT=Caterpillar 股票永續）。
                # 這種要人去補 symbol_aliases.json，不能靠程式猜，所以記 warning。
                logger.warning(f"[{exchange}] {symbol} 反查到多個 raw symbol："
                               f"{raw_exact + raw_norm}，請檢查 symbol_aliases.json")

        # --- 4. 人工別名（交易所專屬比全域精準，各自排序確保跨 process 一致）---
        alias_ex, alias_global = [], []
        if exchange:
            for alias, canonical in _exchange_alias_map.get(exchange.lower(), {}).items():
                if canonical in (base, stripped):
                    alias_ex.append(f"{alias}/USDT:USDT")
        for alias, canonical in _alias_map.items():
            if canonical in (base, stripped):
                alias_global.append(f"{alias}/USDT:USDT")
        alias_ex.sort()
        alias_global.sort()

        # --- 5. 猜測型變體（最後）---
        guesses = []
        if stripped == base:
            guesses.append(f"1000{base}/USDT:USDT")          # ACE → 1000ACE（盲猜）
        if exchange and exchange.lower() == "mexc":
            guesses.append(f"{base}STOCK/USDT:USDT")          # MEXC 股票代幣後綴（盲猜）

        return self._ordered_unique(
            raw_exact + raw_norm                                   # 1
            + [symbol]                                             # 2
            + [f"{base}/USDT:USDT", f"{stripped}/USDT:USDT"]       # 3
            + alias_ex + alias_global                              # 4
            + guesses                                              # 5
        )

    async def fetch_symbol_history(self, symbol: str, limit: int = 100) -> dict:
        """取得某幣種在各交易所的歷史費率（快取 + API 合併）"""
        base = symbol.split("/")[0].upper()

        # 1. 從 _rate_history 快取讀取（毫秒級，提供大部分資料）
        history = {}
        for e in self._rate_history:
            sym_base = e["sym"].split("/")[0].upper()
            if sym_base != base:
                continue
            ex = e["ex"]
            if ex not in history:
                history[ex] = []
            history[ex].append({
                "timestamp": int(datetime.fromisoformat(e["ts"]).timestamp() * 1000),
                "rate": e["rate"],
            })

        # 2. 從交易所 API 抓最新資料補齊（並行，填補快取的空窗期）
        since_ms = int((datetime.now(timezone.utc) - timedelta(days=3)).timestamp() * 1000)

        async def _fetch_one(ex):
            variants = self._get_symbol_variants(symbol, ex.name)
            for sym in variants:
                try:
                    data = await ex.fetch_funding_history(sym, since=since_ms, limit=limit)
                    if data:
                        return (ex.name, data)
                except Exception:
                    continue
            return None

        api_results = await asyncio.gather(
            *[_fetch_one(ex) for ex in self._exchanges],
            return_exceptions=True,
        )

        # 3. 合併：API 資料優先（API 回傳的是實際結算費率，快取可能是掃描時的預測值）
        for r in api_results:
            if isinstance(r, Exception) or r is None:
                continue
            name, data = r
            if not data:
                continue
            if name not in history:
                history[name] = []
            existing_ts_map = {e["timestamp"]: e for e in history[name]}
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            for entry in data:
                # 對齊到整點（去掉秒和微秒）
                dt = datetime.fromtimestamp(entry["timestamp"] / 1000, tz=timezone.utc)
                dt = dt.replace(second=0, microsecond=0)
                ts = int(dt.timestamp() * 1000)
                # 過濾：只保留整點 + 已過去的結算
                if ts > now_ms:
                    continue
                if dt.minute != 0:
                    continue
                if ts in existing_ts_map:
                    # API 回傳的是實際結算值，覆蓋快取的預測值
                    existing_ts_map[ts]["rate"] = entry["rate"]
                else:
                    new_entry = {"timestamp": ts, "rate": entry["rate"]}
                    history[name].append(new_entry)
                    existing_ts_map[ts] = new_entry

        # 4. 將 API 抓到的新資料存進 _rate_history（下次不用再抓）
        existing_cache = {(r["ts"], r["ex"], r["sym"]): r for r in self._rate_history}
        sym_name = f"{base}/USDT:USDT"
        added = 0
        updated = 0
        for ex_name, entries in history.items():
            for entry in entries:
                dt = datetime.fromtimestamp(entry["timestamp"] / 1000, tz=timezone.utc)
                ts_iso = dt.replace(second=0, microsecond=0).isoformat()
                key = (ts_iso, ex_name, sym_name)
                if key in existing_cache:
                    # 用 API 的實際結算值覆蓋快取的預測值
                    if existing_cache[key]["rate"] != entry["rate"]:
                        existing_cache[key]["rate"] = entry["rate"]
                        updated += 1
                else:
                    new_rec = {
                        "ts": ts_iso, "ex": ex_name, "sym": sym_name, "rate": entry["rate"],
                    }
                    self._rate_history.append(new_rec)
                    existing_cache[key] = new_rec
                    added += 1
        if added or updated:
            self._save_rate_history()
            logger.info(f"幣種詳情補充 {sym_name}：新增 {added} 筆，更正 {updated} 筆")

        # 5. 排序 + 限制筆數
        for ex in history:
            history[ex].sort(key=lambda x: x["timestamp"])
            history[ex] = history[ex][-limit:]

        return history

    async def fetch_futures_spot_premium(
        self, symbol: str, exchange_id: str, interval: str = "1m", hours: int = 12
    ) -> list[dict]:
        """取得某幣種在指定交易所的溢價指數時序資料

        優先使用交易所原生溢價指數 API，效能更好且不需要現貨市場存在。
        策略：
        1. Binance/Bybit/Gate.io：原生 premium index klines
        2. OKX/MEXC：mark price + index price klines 自行計算
        3. 其他：回退到合約 vs 現貨 OHLCV 計算
        """
        futures_ex = next((e for e in self._exchanges if e.name == exchange_id), None)
        if not futures_ex:
            return []

        since_ms = int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp() * 1000)
        variants = self._get_symbol_variants(symbol, exchange_id)

        # --- 策略 1：原生溢價指數 K 線（Binance, Bybit, Gate.io）---
        if exchange_id in ("binance", "bybit", "gateio"):
            data = await self._fetch_premium_index_klines(exchange_id, variants, interval, since_ms, hours)
            if data:
                return data

        # --- 策略 2：mark + index K 線自行計算（OKX, MEXC, Aster）---
        if exchange_id in ("okx", "mexc", "aster"):
            data = await self._fetch_mark_index_premium(exchange_id, variants, interval, since_ms)
            if data:
                return data

        # --- 策略 2b：Hyperliquid/Trade.xyz fundingHistory premium 欄位（每小時一筆）---
        if exchange_id in ("hyperliquid", "tradexyz"):
            data = await self._fetch_hl_premium_history(exchange_id, variants, since_ms)
            if data:
                return data

        # --- 策略 3：回退到合約 vs 現貨 OHLCV ---
        spot_ex = self._spot_exchanges.get(exchange_id)
        if not spot_ex or not hasattr(futures_ex, '_exchange'):
            return []

        async def _fetch_futures():
            if not futures_ex._exchange.markets:
                await futures_ex._exchange.load_markets()
            for sym in variants:
                if sym in futures_ex._exchange.markets:
                    try:
                        return await futures_ex._exchange.fetch_ohlcv(sym, interval, since=since_ms, limit=1000)
                    except Exception:
                        continue
            return []

        async def _fetch_spot():
            if not spot_ex.markets:
                await spot_ex.load_markets()
            for sym in variants:
                spot_sym = sym.replace(":USDT", "")
                if spot_sym in spot_ex.markets:
                    try:
                        return await spot_ex.fetch_ohlcv(spot_sym, interval, since=since_ms, limit=1000)
                    except Exception:
                        continue
            return []

        results = await asyncio.gather(_fetch_futures(), _fetch_spot(), return_exceptions=True)
        futures_candles = results[0] if not isinstance(results[0], Exception) else []
        spot_candles = results[1] if not isinstance(results[1], Exception) else []

        if not futures_candles or not spot_candles:
            return []

        spot_map = {c[0]: c[4] for c in spot_candles}

        data = []
        for c in futures_candles:
            ts, fc = c[0], c[4]
            sc = spot_map.get(ts)
            if sc and sc > 0 and fc:
                data.append({
                    "timestamp": ts,
                    "futures_price": fc,
                    "spot_price": sc,
                    "premium": round((fc / sc - 1) * 100, 6),
                })

        return data

    # 溢價查詢最多試幾個 symbol 變體。第 1 順位是掃描結果的真實 raw symbol，
    # 正常情況第一次就命中；留 3 個名額只是為了涵蓋「冷啟動、該幣剛上架、
    # 掃描結果還沒有它」的回退。不設更大：每多試一次就多打一發註定 4xx 的請求，
    # 而 exchanges/_session.py 的 limit_per_host=10 是【全程序共用額度】，
    # 重試風暴會直接排擠正在跑的掃描迴圈。
    PREMIUM_MAX_ATTEMPTS = 3

    @classmethod
    def _premium_bases(cls, variants: list[str]) -> list[str]:
        """把有序變體清單轉成有序去重的 base 清單（依可信度，最多試 N 個）。

        ⚠ 刻意【不做 .upper()】：原本 next(iter(variants)).split("/")[0].upper() 的
          .upper() 會把 Hyperliquid 的 kPEPE 打成 KPEPE。本程式組出來的候選本來就是
          大寫，只有交易所回報的 raw symbol 才帶原始大小寫 —— 那正是要保留的東西。
          萬一某所真的要大寫而 raw 是小寫，也會由後面的標準寫法候選接住。
        """
        return cls._ordered_unique(v.split("/")[0] for v in variants)[: cls.PREMIUM_MAX_ATTEMPTS]

    async def _try_premium_bases(self, exchange_id: str, bases: list[str], fetch_one):
        """依序嘗試每個候選 base，第一個【真的解析出資料】的才採用。

        設計理由：
        1. 序列不並發 —— 候選之間互斥（只有一個是對的），並發等於故意多打註定失敗的
           請求去搶 limit_per_host=10 的全程序額度，對限流是純損失。
        2. 成功判準是「解析後 data 非空」而不是 HTTP 200 —— Bybit 對無效 symbol
           回的是 200 + retCode=10001，只看 status 完全攔不到。這正是原程式把
           「symbol 打錯」誤判成「這個幣沒有溢價資料」的直接原因。
        3. 全部失敗記 warning 並列出試過什麼、各自為什麼失敗。原本失敗只有
           logger.debug（預設 log level 是 INFO）→ 圖是空的、log 一片乾淨，
           這個 bug 才能躲這麼久。
        """
        attempts: list[str] = []
        for b in bases:
            try:
                data, note = await fetch_one(b)
            except Exception as e:
                data, note = [], f"例外 {type(e).__name__}: {e}"
            attempts.append(f"{b}→{note}")
            if data:
                if len(attempts) > 1:
                    # 第 1 順位（掃描結果 raw symbol）沒中，代表反查失效，值得注意
                    logger.info(f"[{exchange_id}] 溢價命中 {b}"
                                f"（前 {len(attempts) - 1} 個失敗：{attempts[:-1]}）")
                return data
        logger.warning(f"[{exchange_id}] 溢價全部候選失敗：{' | '.join(attempts) or '無候選'}")
        return []

    async def _fetch_premium_index_klines(
        self, exchange_id: str, variants: list[str], interval: str, since_ms: int, hours: int
    ) -> list[dict]:
        """Binance / Bybit / Gate.io 原生溢價指數 K 線（依序試候選，不再抽籤）"""
        bases = self._premium_bases(variants)
        if not bases:
            return []

        # 整個重試迴圈共用一個 session：只佔 1 個 per-host 名額，重試走 keep-alive
        # 不重新 TLS 握手（必須用 make_session，見 exchanges/_session.py 的鐵則）
        async with make_session() as session:

            async def _binance(base):
                # 官方文檔 GET /fapi/v1/premiumIndexKlines；無效 symbol → 400 -1121
                params = {"symbol": f"{base}USDT", "interval": interval,
                          "startTime": since_ms, "limit": 1500}
                async with session.get("https://fapi.binance.com/fapi/v1/premiumIndexKlines",
                                       params=params) as resp:
                    if resp.status != 200:
                        return [], f"HTTP {resp.status} {(await resp.text())[:80]}"
                    raw = await resp.json()
                # Binance kline: [openTime, open, high, low, close, ...]
                # 值是小數比例（如 -0.00067），乘 100 轉百分比
                data = [{"timestamp": int(c[0]), "premium": round(float(c[4]) * 100, 6)} for c in raw]
                return data, f"{len(data)} 筆"

            async def _bybit(base):
                bb_interval = {"1m": "1", "5m": "5", "15m": "15", "1h": "60"}.get(interval, "1")
                params = {"category": "linear", "symbol": f"{base}USDT",
                          "interval": bb_interval, "start": since_ms, "limit": 1000}
                async with session.get("https://api.bybit.com/v5/market/premium-index-price-kline",
                                       params=params) as resp:
                    if resp.status != 200:
                        return [], f"HTTP {resp.status}"
                    raw = await resp.json()
                # ⚠ Bybit 對無效 symbol 回的是 HTTP 200 + retCode=10001，必須看 retCode
                if raw.get("retCode") != 0:
                    return [], f"retCode {raw.get('retCode')} {raw.get('retMsg')}"
                items = raw.get("result", {}).get("list", [])
                # Bybit: [timestamp, open, high, low, close]，值是小數比例，降序
                data = [{"timestamp": int(c[0]), "premium": round(float(c[4]) * 100, 6)} for c in items]
                data.sort(key=lambda d: d["timestamp"])
                return data, f"{len(data)} 筆"

            async def _gateio(base):
                gt_interval = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h"}.get(interval, "1m")
                params = {"contract": f"{base}_USDT", "interval": gt_interval,
                          "from": since_ms // 1000,
                          "to": int(datetime.now(timezone.utc).timestamp()), "limit": 1000}
                async with session.get("https://api.gateio.ws/api/v4/futures/usdt/premium_index",
                                       params=params) as resp:
                    if resp.status != 200:
                        return [], f"HTTP {resp.status} {(await resp.text())[:80]}"
                    raw = await resp.json()
                # Gate.io: [{t, o, h, l, c}]，值是小數比例
                data = [{"timestamp": int(c["t"]) * 1000,
                         "premium": round(float(c["c"]) * 100, 6)} for c in raw]
                return data, f"{len(data)} 筆"

            fetch_one = {"binance": _binance, "bybit": _bybit, "gateio": _gateio}.get(exchange_id)
            if not fetch_one:
                return []
            return await self._try_premium_bases(exchange_id, bases, fetch_one)

    async def _fetch_hl_premium_history(
        self, exchange_id: str, variants: list[str], since_ms: int
    ) -> list[dict]:
        """Hyperliquid / Trade.xyz：fundingHistory 內含 premium 欄位（每小時一筆）

        HL 的 coin 命名沒有 1000x 慣例（它用 kPEPE 這種 k 前綴），所以
        _get_symbol_variants 補的 1000{base} 對 HL 而言 100% 是垃圾候選 ——
        原本用 next(iter()) 抽到它就是空圖，現在會自動往下試下一個。
        """
        bases = self._premium_bases(variants)
        if not bases:
            return []

        async with make_session() as session:

            async def _one(base):
                coin = f"xyz:{base}" if exchange_id == "tradexyz" else base
                payload = {"type": "fundingHistory", "coin": coin, "startTime": since_ms}
                async with session.post("https://api.hyperliquid.xyz/info", json=payload) as resp:
                    if resp.status != 200:
                        return [], f"HTTP {resp.status}"
                    raw = await resp.json()
                data = []
                for item in raw or []:
                    ts, premium = item.get("time"), item.get("premium")
                    if ts and premium is not None:
                        data.append({"timestamp": int(ts),
                                     "premium": round(float(premium) * 100, 6)})
                data.sort(key=lambda d: d["timestamp"])
                return data, f"{len(data)} 筆"

            return await self._try_premium_bases(exchange_id, bases, _one)

    @staticmethod
    def _mark_index_to_premium(mark_rows, index_map, ts_scale: int = 1,
                               close_idx: int = 4) -> list[dict]:
        """把 mark K 線與 index 收盤對照表合成溢價序列：(mark/index - 1) * 100%

        close_idx 因交易所格式而異：OKX / Aster 是 [ts, o, h, l, c, ...] → 4；
        MEXC 回的是平行陣列，呼叫端 zip 成 (time, close) 兩元組 → 1。
        """
        data = []
        for c in mark_rows:
            ts = int(c[0])
            idx_close = index_map.get(ts)
            if idx_close and idx_close > 0:
                data.append({"timestamp": ts * ts_scale,
                             "premium": round((float(c[close_idx]) / idx_close - 1) * 100, 6)})
        data.sort(key=lambda d: d["timestamp"])
        return data

    async def _fetch_mark_index_premium(
        self, exchange_id: str, variants: list[str], interval: str, since_ms: int
    ) -> list[dict]:
        """OKX / MEXC / Aster：用 mark price + index price K 線計算溢價（依序試候選）

        MEXC 最容易踩雷：_get_symbol_variants 對 mexc 會多補一個 {base}STOCK 猜測變體，
        原本 next(iter()) 三選一、正確率只有 1/3（實測 ACE 會抽到 ACESTOCK）。
        """
        bases = self._premium_bases(variants)
        if not bases:
            return []

        if exchange_id == "okx":
            okx_interval = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1H"}.get(interval, "1m")
            async with make_session() as session:

                async def _get(url, params):
                    async with session.get(url, params=params) as resp:
                        if resp.status != 200:
                            return []
                        return (await resp.json()).get("data", [])

                async def _one(base):
                    after_ts = str(since_ms)
                    mark_raw, index_raw = await asyncio.gather(
                        _get("https://www.okx.com/api/v5/market/mark-price-candles",
                             {"instId": f"{base}-USDT-SWAP", "bar": okx_interval,
                              "after": after_ts, "limit": "300"}),
                        _get("https://www.okx.com/api/v5/market/index-candles",
                             {"instId": f"{base}-USDT", "bar": okx_interval,
                              "after": after_ts, "limit": "300"}),
                    )
                    if not mark_raw or not index_raw:
                        return [], f"mark {len(mark_raw)} / index {len(index_raw)} 筆"
                    # OKX: [ts, o, h, l, c, confirm]
                    data = self._mark_index_to_premium(
                        mark_raw, {int(c[0]): float(c[4]) for c in index_raw})
                    return data, f"{len(data)} 筆"

                return await self._try_premium_bases(exchange_id, bases, _one)

        if exchange_id == "mexc":
            mx_interval = {"1m": "Min1", "5m": "Min5", "15m": "Min15", "1h": "Min60"}.get(interval, "Min1")
            start_ts, end_ts = since_ms // 1000, int(datetime.now(timezone.utc).timestamp())
            async with make_session() as session:

                async def _one(base):
                    async def _get_mx(kind):
                        url = f"https://contract.mexc.com/api/v1/contract/kline/{kind}/{base}_USDT"
                        async with session.get(url, params={"interval": mx_interval,
                                                            "start": start_ts, "end": end_ts}) as resp:
                            if resp.status != 200:
                                return {}
                            return (await resp.json()).get("data", {}) or {}

                    mark_raw, index_raw = await asyncio.gather(_get_mx("fair_price"),
                                                               _get_mx("index_price"))
                    # MEXC: {time[], open[], close[], high[], low[]}（欄位平行陣列）
                    index_map = {int(t): float(c) for t, c in
                                 zip(index_raw.get("time", []), index_raw.get("close", []))}
                    rows = list(zip(mark_raw.get("time", []), mark_raw.get("close", [])))
                    if not rows or not index_map:
                        return [], f"mark {len(rows)} / index {len(index_map)} 筆"
                    # rows 是 (time, close) 兩元組 → close 在索引 1；MEXC 時戳是秒
                    data = self._mark_index_to_premium(rows, index_map, ts_scale=1000, close_idx=1)
                    return data, f"{len(data)} 筆"

                return await self._try_premium_bases(exchange_id, bases, _one)

        if exchange_id == "aster":
            # Aster（Binance-fork）：ccxt 底層呼叫 markPriceKlines + indexPriceKlines。
            # 實例在重試迴圈【外面】建一次就好——原本每次呼叫都重建 + load_markets()，
            # 為註定失敗的猜測變體付出整輪成本。ccxt_to_close 與「能不能用」分開追蹤，
            # 確保任何路徑都會釋放 connector（見 CLAUDE.md 的常駐程式資源鐵則）。
            import ccxt.async_support as ccxt_async
            aster_interval = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h"}.get(interval, "1m")
            ccxt_to_close = ccxt_async.aster({"enableRateLimit": True})
            try:
                await ccxt_to_close.load_markets()

                async def _one(base):
                    mark_raw, index_raw = await asyncio.gather(
                        ccxt_to_close.fapipublic_get_v1_markpriceklines({
                            "symbol": f"{base}USDT", "interval": aster_interval,
                            "startTime": since_ms, "limit": 1000}),
                        ccxt_to_close.fapipublic_get_v1_indexpriceklines({
                            "pair": f"{base}USDT", "interval": aster_interval,
                            "startTime": since_ms, "limit": 1000}),
                    )
                    if not mark_raw or not index_raw:
                        return [], f"mark {len(mark_raw or [])} / index {len(index_raw or [])} 筆"
                    # 格式: [openTime, open, high, low, close, ...]
                    data = self._mark_index_to_premium(
                        mark_raw, {int(c[0]): float(c[4]) for c in index_raw})
                    return data, f"{len(data)} 筆"

                return await self._try_premium_bases(exchange_id, bases, _one)
            except Exception as e:
                logger.warning(f"[{exchange_id}] mark+index premium 失敗: {e}")
                return []
            finally:
                await asyncio.shield(ccxt_to_close.close())

        return []

    async def fetch_price_premium(
        self, symbol: str, exchange_a: str, exchange_b: str, days: int = 3
    ) -> list[dict]:
        """取得兩交易所的 5 分 K 線價格溢價：(A收盤 / B收盤 - 1) * 100%
        支援現貨：exchange id 以 '_spot' 結尾（如 'bitget_spot'），從 _spot_exchanges 取實例。"""

        def _resolve(name: str):
            # 現貨：'{base}_spot' → 從 _spot_exchanges[base] 取 ccxt spot 實例
            if name.endswith("_spot"):
                base = name[:-5]
                return "spot", self._spot_exchanges.get(base)
            # 合約：從 _exchanges 取 wrapper
            return "futures", next((e for e in self._exchanges if e.name == name), None)

        kind_a, ex_a = _resolve(exchange_a)
        kind_b, ex_b = _resolve(exchange_b)
        if ex_a is None or ex_b is None:
            return []

        since_ms = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)

        async def _fetch_ohlcv(ex, ex_name, kind):
            """依類型取 K 線：現貨直接呼叫 ccxt spot 實例；合約走原有 variants 邏輯"""
            if kind == "spot":
                base = symbol.split("/")[0]
                spot_sym = f"{base}/USDT"
                try:
                    if getattr(ex, "markets", None) and spot_sym not in ex.markets:
                        return []
                    candles = await ex.fetch_ohlcv(
                        spot_sym, timeframe='5m', since=since_ms, limit=1000
                    )
                    if candles:
                        return candles
                except Exception as e:
                    logger.debug(f"[{ex_name}] spot OHLCV {spot_sym} 失敗: {e}")

                # MEXC 現貨 fallback：api.mexc.com 會被 Akamai 間歇性 403
                # 改走 www.mexc.com 平台 API（相同資料、不同域名、不被封鎖）
                if ex_name == "mexc_spot":
                    return await self._fetch_mexc_spot_ohlcv_fallback(base, since_ms)
                return []

            # 合約：嘗試所有 symbol 變體
            variants = self._get_symbol_variants(symbol, ex_name)
            # Deribit 反向永續：我們的 record 是 {base}/USD:USD（quote_currency 給的），
            # 但 ccxt 用結算幣當第三段 → BTC/USD:BTC。實測 SOL/USDC:USDC 直接命中、
            # 只有 BTC/ETH 這兩檔 inverse 需要補這個變體，否則價差圖抓不到 K 線。
            if ex_name == "deribit" and symbol.endswith("/USD:USD"):
                variants = list(variants) + [f"{symbol.split('/')[0]}/USD:{symbol.split('/')[0]}"]

            # 取得 ccxt async 實例：舊式 CcxtExchange 用常駐 _exchange；新式 WS-first
            # wrapper（無 _exchange）用「常駐 OHLCV ccxt 快取」——建一次、load_markets 一次、
            # 重複使用、永不 per-call close，逾時取消也不會漏 session（洩漏根治）。
            ccxt_ex = getattr(ex, '_exchange', None)
            if ccxt_ex is None:
                ccxt_ex = await self._get_ohlcv_ccxt(ex.name)

            # 常駐快取已 load 過 markets；此處不再 load、不再 close（共用實例，取消時不關）。
            if ccxt_ex is not None:
                for sym in variants:
                    try:
                        if sym not in ccxt_ex.markets:
                            continue
                        candles = await ccxt_ex.fetch_ohlcv(
                            sym, timeframe='5m', since=since_ms, limit=1000
                        )
                        if candles:
                            return candles
                    except Exception as e:
                        logger.debug(f"[{ex_name}] ccxt OHLCV {sym} 失敗: {e}")
                        continue

            # 直接 REST 交易所（CoinW、Hyperliquid、Aster、Ourbit、TradeXyz…）
            for sym in variants:
                try:
                    candles = await self._fetch_ohlcv_direct(ex, sym, since_ms)
                    if candles:
                        return candles
                except Exception as e:
                    logger.debug(f"[{ex_name}] direct OHLCV {sym} 失敗: {e}")
                    continue
            return []

        results = await asyncio.gather(
            _fetch_ohlcv(ex_a, exchange_a, kind_a),
            _fetch_ohlcv(ex_b, exchange_b, kind_b),
            return_exceptions=True,
        )

        candles_a = results[0] if not isinstance(results[0], Exception) else []
        candles_b = results[1] if not isinstance(results[1], Exception) else []

        if not candles_a or not candles_b:
            return []

        # 建立 B 的 timestamp -> close 映射
        b_map = {c[0]: c[4] for c in candles_b}  # [ts, o, h, l, c, v]

        # 對齊計算溢價
        premium_data = []
        for c in candles_a:
            ts = c[0]
            close_a = c[4]
            close_b = b_map.get(ts)
            if close_b and close_b > 0 and close_a:
                premium_pct = (close_a / close_b - 1) * 100
                premium_data.append({
                    "timestamp": ts,
                    "price_a": close_a,
                    "price_b": close_b,
                    "premium": round(premium_pct, 6),
                })

        return premium_data

    async def _lighter_market_id(self, rest_base: str, base: str) -> int | None:
        """Lighter 的 K 線是用 market_id（整數）定址，不是 symbol；快取住避免每次重抓。

        主站與 RH Chain 的 market_id 各自編號、互不相通，所以快取要用 rest_base 分開存。
        """
        if not hasattr(self, "_lighter_mid_cache"):
            self._lighter_mid_cache = {}
        if rest_base not in self._lighter_mid_cache:
            try:
                async with make_session(20) as session:
                    async with session.get(f"{rest_base}/orderBookDetails") as resp:
                        if resp.status != 200:
                            return None
                        data = await resp.json()
                self._lighter_mid_cache[rest_base] = {
                    str(d["symbol"]).upper(): d["market_id"]
                    for d in (data.get("order_book_details") or [])
                    if d.get("symbol") and d.get("market_id") is not None
                }
            except Exception as e:
                logger.debug(f"[lighter] market_id 對照表取得失敗 {rest_base}: {e}")
                return None
        return self._lighter_mid_cache[rest_base].get(base.upper())

    async def _fetch_ohlcv_direct(self, ex, symbol: str, since_ms: int) -> list:
        """非 ccxt 交易所的 K 線取得（Aster、CoinW、Hyperliquid、Lighter）"""
        base = symbol.split("/")[0].upper()
        try:
            if ex.name == "aster":
                # 用常駐 OHLCV ccxt 快取（建一次重複用、不 per-call close → 不漏 session）
                exchange = await self._get_ohlcv_ccxt("aster")
                if exchange is not None:
                    try:
                        if symbol in exchange.markets:
                            candles = await exchange.fetch_ohlcv(
                                symbol, timeframe="5m", since=since_ms, limit=1000
                            )
                            if candles:
                                return candles
                    except Exception as e:
                        logger.debug(f"[aster] ccxt OHLCV 失敗: {e}")
                return []
            elif ex.name == "okx":
                # OKX 官方 K 線：/api/v5/market/candles（繞過 ccxt load_markets，
                # 後者因 OKX 偶有空 instId 的 FUTURES 合約而排序崩潰）
                # 回傳降序 [ts, o, h, l, c, vol, ...]，單次最多 300 根；分頁用 after（取更舊）
                inst_id = f"{base}-USDT-SWAP"
                rows: list = []
                after = None
                async with make_session(15) as session:
                    for _ in range(6):  # 6 × 300 = 1800 根，足以涵蓋 3 天 5m（864 根）
                        params = {"instId": inst_id, "bar": "5m", "limit": "300"}
                        if after:
                            params["after"] = str(after)
                        async with session.get(
                            "https://www.okx.com/api/v5/market/candles", params=params
                        ) as resp:
                            if resp.status != 200:
                                break
                            data = (await resp.json()).get("data") or []
                        if not data:
                            break
                        rows.extend(data)
                        oldest = int(data[-1][0])
                        after = oldest
                        if oldest <= since_ms or len(data) < 300:
                            break
                # OKX 回傳為降序（新→舊），轉成 ccxt 慣例的升序以對齊其他分支
                candles = [
                    [int(k[0]), float(k[1]), float(k[2]),
                     float(k[3]), float(k[4]), float(k[5])]
                    for k in rows if int(k[0]) >= since_ms
                ]
                candles.sort(key=lambda c: c[0])
                return candles
            elif ex.name == "coinw":
                # CoinW API：currencyCode + granularity（'1'=5min, '0'=1m, '3'=1h...）
                # response.data 為 array of [ts, open, high, low, close, volume]，已是 ccxt 標準順序
                session = await ex._get_session()
                async with session.get(
                    f"https://api.coinw.com/v1/perpumPublic/klines",
                    params={"currencyCode": base, "granularity": "1", "limit": 864},
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        klines = data.get("data") or []
                        return [
                            [int(k[0]), float(k[1]), float(k[2]),
                             float(k[3]), float(k[4]), float(k[5])]
                            for k in klines if int(k[0]) >= since_ms
                        ]
            elif ex.name in ("hyperliquid", "tradexyz"):
                # trade.xyz 的 coin 名稱帶 xyz: 前綴
                coin = f"xyz:{base}" if ex.name == "tradexyz" else base
                async with make_session() as session:
                    payload = {
                        "type": "candleSnapshot",
                        "req": {"coin": coin, "interval": "5m",
                                "startTime": since_ms, "endTime": int(datetime.now(timezone.utc).timestamp() * 1000)},
                    }
                    async with session.post(
                        "https://api.hyperliquid.xyz/info", json=payload
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            return [
                                [int(c["t"]), float(c["o"]), float(c["h"]),
                                 float(c["l"]), float(c["c"]), float(c.get("v", 0))]
                                for c in data
                            ]
            elif ex.name == "ourbit":
                # MEXC 白標 K 線 API：回傳 {time:[], open:[], close:[], ...}
                ourbit_sym = f"{base}_USDT"
                start_s = since_ms // 1000
                end_s = int(datetime.now(timezone.utc).timestamp())
                async with make_session() as session:
                    async with session.get(
                        f"https://futures.ourbit.com/api/v1/contract/kline/{ourbit_sym}",
                        params={"interval": "Min5", "start": start_s, "end": end_s},
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            d = data.get("data", {})
                            times = d.get("time", [])
                            opens = d.get("open", [])
                            highs = d.get("high", [])
                            lows = d.get("low", [])
                            closes = d.get("close", [])
                            vols = d.get("vol", [])
                            return [
                                [t * 1000, float(o), float(h), float(l), float(c), float(v)]
                                for t, o, h, l, c, v in zip(times, opens, highs, lows, closes, vols)
                            ]
            elif ex.name in ("lighter", "lighter_rh"):
                # Lighter 官方 K 線 endpoint（主站與 RH Chain 共用同一套規格，只差網域）。
                # ⚠ 2026-08-15 實測本機打過去一律 CloudFront 403（mainnet 與 api.rh 兩個 host、
                #   urllib 與 aiohttp、補 Origin/Referer 全試過），而 ccxt 4.5.32 也沒有 lighter
                #   → 目前這兩間所的價差圖會是空的。
                # 這段照官方規格先寫好：哪天不擋了就自動生效，不用再回來補。
                rest_base = getattr(ex, "REST_BASE", None)
                if not rest_base:
                    return []
                market_id = await self._lighter_market_id(rest_base, base)
                if market_id is None:
                    return []
                async with make_session() as session:
                    async with session.get(
                        f"{rest_base}/candlesticks",
                        params={
                            "market_id": market_id,
                            "resolution": "5m",
                            "start_timestamp": since_ms,
                            "end_timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
                            "count_back": 0,
                            "set_timestamp_to_end": "false",
                        },
                    ) as resp:
                        if resp.status != 200:
                            logger.debug(f"[lighter] candlesticks HTTP {resp.status}（已知會被 CloudFront 擋）")
                            return []
                        data = await resp.json()
                        candles = [
                            [int(c["timestamp"]) * 1000, float(c["open"]), float(c["high"]),
                             float(c["low"]), float(c["close"]), float(c.get("volume0") or 0)]
                            for c in (data.get("candlesticks") or [])
                        ]
                        candles.sort(key=lambda x: x[0])
                        return candles
        except Exception as e:
            logger.debug(f"[{ex.name}] direct OHLCV 失敗: {e}")
        return []

    async def _get_ohlcv_ccxt(self, name: str):
        """重查 OHLCV 專用的常駐 ccxt 實例：建一次、load_markets 一次、重複使用，
        只在關機（stop）時 close。徹底消除每次重查現建現關 → 逾時取消也不會漏 session。
        建立流程用 shield 包住：即使觸發它的那次重查逾時被取消，實例仍會建好並快取給
        下次用，不留半成品。"""
        ex = self._ohlcv_ccxt.get(name)
        if ex is not None:
            return ex
        if name in _CCXT_OHLCV_BLOCKLIST:
            return None                    # 永久排除，連建都不要建（見常數說明）
        # 負向快取：建過但 load_markets 失敗的所，1 小時內不再重建，
        # 直接回 None → 走 direct REST fallback，避免每輪重查重建洩漏 session。
        failed_at = self._ohlcv_ccxt_failed.get(name)
        if failed_at is not None and (time.monotonic() - failed_at) < 3600:
            return None
        if getattr(ccxt_async, _CCXT_OHLCV_ID.get(name, name), None) is None:
            return None
        return await asyncio.shield(self._create_ohlcv_ccxt(name))

    async def _create_ohlcv_ccxt(self, name: str):
        """實際建立並 load_markets，受 _ohlcv_ccxt_lock 保護避免並發重複建。"""
        async with self._ohlcv_ccxt_lock:
            ex = self._ohlcv_ccxt.get(name)
            if ex is not None:  # 等鎖時別人已建好
                return ex
            failed_at = self._ohlcv_ccxt_failed.get(name)  # 等鎖時別人剛標記失敗
            if failed_at is not None and (time.monotonic() - failed_at) < 3600:
                return None
            # 快取鍵仍用本專案的 name，但實際建的是對應的 ccxt id（見 _CCXT_OHLCV_ID）
            cls = getattr(ccxt_async, _CCXT_OHLCV_ID.get(name, name), None)
            if cls is None:
                return None
            ex = cls({"enableRateLimit": True})
            try:
                await ex.load_markets()
            except BaseException as e:
                # 記入負向快取，並「一定」關掉半成品——用 BaseException 涵蓋 CancelledError
                # （非 Exception，舊版 except Exception 接不到 → 就是 hyperliquid 漏 session 的主因）。
                self._ohlcv_ccxt_failed[name] = time.monotonic()
                try:
                    await asyncio.shield(ex.close())
                except Exception:
                    pass
                if isinstance(e, Exception):
                    logger.debug(f"[{name}] 常駐 OHLCV ccxt 建立失敗（負向快取 1 小時）: {e}")
                    return None
                raise  # CancelledError 等非 Exception 照常上拋
            self._ohlcv_ccxt[name] = ex
            self._ohlcv_ccxt_failed.pop(name, None)
            logger.info(f"[重查] 建立常駐 OHLCV ccxt 實例：{name}（之後重複使用，不再現建）")
            return ex

    async def _fetch_mexc_spot_ohlcv_fallback(self, base: str, since_ms: int) -> list:
        """MEXC 現貨 K 線 fallback：走 www.mexc.com 平台 API（避開 api.mexc.com 的 Akamai 403）

        回傳 column 格式 {t:[], o:[], h:[], l:[], c:[], v:[]}，轉換為 ccxt 標準 row 格式。
        """
        sym = f"{base.upper()}_USDT"
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        try:
            async with make_session(10) as session:
                async with session.get(
                    "https://www.mexc.com/api/platform/spot/market/kline",
                    params={"symbol": sym, "interval": "Min5",
                            "start": since_ms, "end": now_ms},
                    headers={"User-Agent": "Mozilla/5.0"},
                ) as resp:
                    if resp.status != 200:
                        return []
                    data = await resp.json()
                    d = data.get("data", {})
                    times = d.get("t", [])
                    if not times:
                        return []
                    opens = d.get("o", [])
                    highs = d.get("h", [])
                    lows = d.get("l", [])
                    closes = d.get("c", [])
                    vols = d.get("v", [])
                    return [
                        [int(t) * 1000, float(o), float(h), float(l), float(c), float(v)]
                        for t, o, h, l, c, v in zip(times, opens, highs, lows, closes, vols)
                    ]
        except Exception as e:
            logger.debug(f"[mexc_spot] fallback OHLCV 失敗: {e}")
            return []

    async def _fetch_with_name(self, ex: BaseExchange):
        """包裝交易所查詢，確保例外正確傳播"""
        return await ex.fetch_funding_rates()

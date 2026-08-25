"""RWA 期現套利：買現貨 + 空永續（Delta Neutral 收 Funding），限跨所統一帳戶可交易的標的。

策略（同「Trade.xyz × Backpack 美股期現套利」的思路，但兩腿都在 CrossEx 同一帳戶）：
  買 RWA 現貨 + 空等量永續 → Delta≈0，漲跌互抵
  收益 = Funding Fee（資費，空方收）+ 進場價差收斂
CrossEx 的優勢：跨所共用保證金池，兩腿盈虧互抵，比兩個獨立帳戶更不易被單邊波動強平。

標的範圍完全由 API 推導、不寫死：
  CrossEx /rule/symbols（帳戶實際可交易）∩ Gate /spot/currencies 的官方 name（判斷是否非加密貨幣）
  例：NVDAX="NVIDIA xStock"、XAUT="Tether Gold"、SPCX="SpaceX" → 是 RWA
      AVAX="Avalanche"、STX="Stacks" → 是加密貨幣，排除
"""

import asyncio
import hashlib
import hmac
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from exchanges._session import make_session

logger = logging.getLogger(__name__)

UNIVERSE_PATH = Path(__file__).resolve().parent.parent / "data" / "rwa_universe.json"
UNIVERSE_TTL = 24 * 3600          # 標的清單一天刷新一次（交易所上新標的才會變）
CROSSEX_BASE = "https://api.gateio.ws/api/v4"
GATE_CURRENCIES = "https://api.gateio.ws/api/v4/spot/currencies"
BINANCE_ASSETS = "https://www.binance.com/bapi/asset/v2/public/asset/asset/get-all-asset"

# CrossEx exchange_type → 5011 內部 exchange id
CROSSEX_TO_5011 = {
    "BINANCE": "binance", "OKX": "okx", "GATE": "gateio", "BYBIT": "bybit",
    "KRAKEN": "kraken", "HYPERLIQUID": "hyperliquid", "DERIBIT": "deribit",
}

# Gate 官方 name 判定為「非加密貨幣」的關鍵字（代幣化股票/商品/盤前）
RWA_NAME_KEYWORDS = ("xstock", "ondo tokenized", "tokenized", "spacex", "pre-ipo")
# name 查不到、但已知為非加密貨幣的標的（盤前股權代幣等）
RWA_KNOWN_BASES = {"SPCX", "SPCXX", "OPENAI", "OPENAIX", "ANTHROPIC", "ANTHROPICX"}
# 商品類（24 小時交易，不受美股盤中/盤外影響）
COMMODITY_NAME_KEYWORDS = ("gold", "silver", "oil", "platinum", "palladium", "copper",
                           "nickel", "lead", "aluminum", "zinc", "tin", "natural gas",
                           "crude", "brent", "wheat", "corn", "soybean")

US_EASTERN = ZoneInfo("America/New_York")

# 現貨與合約價差超過此值 → 判定「兩腿基準不一致」，不是可對沖的期現組合。
# 實證：TSLAX/NVDAX 這類真正對沖的組合價差 < 1%；但 SPCX(Gate 現貨 94.75 vs Gate 合約 108.49)、
# OPENAI(728.5 vs 1102.35)、TQQQX(127.4 vs 63.97) 在「同一家交易所內」就差 14%~2 倍，
# 且合約 multiplier 相同 → 合約指數追蹤的標的與現貨代幣並非同一個東西（盤前估值 vs 代幣包裝）。
# 這種組合買現貨+空合約【不是 Delta Neutral】，價差不會收斂，必須標示不可對沖。
HEDGEABLE_MAX_SPREAD_PCT = 3.0

# 跨所價差套利：同一標的、兩腿都是永續，做多便宜所 + 做空貴的所，賺價差收斂。
# 成因是各所【指數取價時點不同】（實例：Bybit 的美股指數要到 9:30 才更新，
# 其他所連續更新 → 美股收盤後到隔日開盤前，兩邊指數會拉開）。
XS_PRICE_CLUSTER_PCT = 20.0    # 同群組內價格偏離中位數超過此% → 視為同名不同標的，不可配對
XS_MIN_PROFIT_PCT = 0.3        # 預計利潤（價差−資費）低於此值不列出
XS_NOTIFY_PROFIT_PCT = 1.0     # 預計利潤 >= 此值才發 TG（群友實務門檻：價差−資費 > 1%）


def _is_rwa(base: str, name: str) -> bool:
    if base.upper() in RWA_KNOWN_BASES:
        return True
    low = (name or "").lower()
    if not low:
        return False
    # 「gold」等商品字眼會誤中加密貨幣（AGLD=Adventure Gold），故商品必須是代幣化商品的
    # 標準命名（PAX Gold / Tether Gold / iShares Silver Trust Ondo Tokenized）
    if any(k in low for k in RWA_NAME_KEYWORDS):
        return True
    return low in ("pax gold", "tether gold")


def _is_commodity(name: str) -> bool:
    return any(k in (name or "").lower() for k in COMMODITY_NAME_KEYWORDS)


def us_market_state(now: datetime | None = None) -> str:
    """美股盤別：regular（正常時段）/ extended（盤前盤後）/ closed（休市，含週末）。
    文章明確提醒：非正常時段流動性下降、Bid/Ask 價差會明顯變大侵蝕利潤。"""
    now = (now or datetime.now(timezone.utc)).astimezone(US_EASTERN)
    if now.weekday() >= 5:                       # 週六日
        return "closed"
    mins = now.hour * 60 + now.minute
    if 9 * 60 + 30 <= mins < 16 * 60:            # 09:30–16:00 ET
        return "regular"
    if 4 * 60 <= mins < 9 * 60 + 30 or 16 * 60 <= mins < 20 * 60:   # 04:00–09:30、16:00–20:00 ET
        return "extended"
    return "closed"


# ── CrossEx 標的清單 ─────────────────────────────────────────

def _crossex_headers(key: str, secret: str, method: str, path: str, query: str) -> dict:
    """Gate APIv4 標準簽名（CrossEx 所有端點都強制要 KEY/Timestamp/SIGN，含唯讀的 symbols）"""
    ts = str(int(time.time()))
    body_hash = hashlib.sha512(b"").hexdigest()
    payload = f"{method}\n{path}\n{query}\n{body_hash}\n{ts}"
    sign = hmac.new(secret.encode(), payload.encode(), hashlib.sha512).hexdigest()
    return {"KEY": key, "Timestamp": ts, "SIGN": sign, "Accept": "application/json"}


async def _fetch_crossex_symbols(key: str, secret: str) -> list[dict]:
    """GET /crossex/rule/symbols（唯讀）：帳戶可交易的全部標的"""
    path = "/api/v4/crossex/rule/symbols"
    headers = _crossex_headers(key, secret, "GET", path, "")
    async with make_session(20) as s:
        async with s.get(f"{CROSSEX_BASE}/crossex/rule/symbols", headers=headers) as r:
            if r.status != 200:
                raise Exception(f"CrossEx symbols HTTP {r.status}: {(await r.text())[:120]}")
            return await r.json()


# 「去後綴代號」本身的官方名稱（判斷撞號用），由 refresh_universe 在推導前填入
_STEM_NAMES: dict[str, str] = {}


def _same_underlying(tokenized_name: str, stem_name: str) -> bool:
    """代幣化資產的名稱與「去後綴代號」本身的名稱是否指同一家公司。

    防撞號：QNTB="Quantinuum"(股票) 去 B 後是 QNT，但 QNT 本身是加密貨幣 "Quant"
    → 名稱不符 → 不可合併，否則會配出「買 Quant 幣 × 空 Quantinuum 股票」的假對沖。
    反例：MUB="Micron Technology" 與 MU="Micron Technology" 相符 → 可合併。
    stem 沒有名稱（未上現貨）視為不衝突。
    """
    if not stem_name:
        return True
    a = (tokenized_name or "").lower().split()
    b = (stem_name or "").lower().split()
    if not a or not b:
        return True
    return a[0] == b[0]


    # 代幣化標記：各所命名規則不同，能認出來的就從名稱剝掉，剩下的即公司名
TOKEN_MARKERS = (
    " (bstocks)", " xstock", " ondo tokenized", " tokenized", " (tokenized)",
    " bstocks", " stock token", " pre-ipo",
)
# 公司名尾綴（比對相似度時忽略）
CORP_SUFFIXES = {
    "inc", "inc.", "corp", "corp.", "corporation", "ltd", "ltd.", "plc", "llc",
    "co", "co.", "company", "group", "holdings", "holding", "technologies",
    "technology", "n.v.", "sa", "ag", "the", "&",
}
# 代號可能帶的代幣化前後綴（各所規則不同：bStocks=B、xStocks=X、Ondo=ON、
# MEXC=STOCK、OKX 等用 X 前綴）。用來產生「可能的同一標的」候選，
# 最終是否真的同一標的由【價格】驗證（見 build_opportunities 的可對沖判定）。
TICKER_SUFFIXES = ("B", "X", "ON", "STOCK", "ONDO", "G")
TICKER_PREFIXES = ("X", "NC")


def _strip_markers(name: str) -> str:
    low = (name or "").lower()
    for m in TOKEN_MARKERS:
        low = low.replace(m, " ")
    return " ".join(low.split())


def _name_key(name: str) -> str:
    """公司名正規化：剝代幣化標記、去尾綴與標點 → 用於跨所名稱比對。
    例：'Micron Technology (bStocks)' / 'Micron Technology' → 'micron'
    """
    words = [w.strip(".,()") for w in _strip_markers(name).split()]
    words = [w for w in words if w and w not in CORP_SUFFIXES]
    return " ".join(words[:2])          # 取前兩個實詞即可辨識公司


def _names_match(a: str, b: str) -> bool:
    """名稱是否指同一標的（部分相似即可，因各所命名長短不一）"""
    ka, kb = _name_key(a), _name_key(b)
    if not ka or not kb:
        return False
    if ka == kb:
        return True
    fa, fb = ka.split()[0], kb.split()[0]
    return fa == fb and (ka.startswith(kb) or kb.startswith(ka))


def _ticker_variants(base: str) -> list[str]:
    """該代號可能對應的標準代號（涵蓋各所代幣化命名規則），【由短到長的確定順序】。

    ⚠ 回傳 list 不是 set：所有呼叫端不是「取一個當答案」就是「for + 第一個命中就 break」，
      兩者都需要優先序。set 沒有順序，而 Python 字串 hash 受 PYTHONHASHSEED 隨機化，
      同一份程式碼在不同 process 會挑到不同代號 → 分組鍵每次重啟就飄
      （實測 XAG 在 7 個 seed 下有時收斂成 AG、有時 XA）。
      排序用 (長度, 字典序)：短的優先＝盡量收斂，長度平手時字典序保證跨 process 一致。
    """
    b = base.upper()
    out = {b}
    for suf in TICKER_SUFFIXES:
        if b.endswith(suf) and len(b) > len(suf) + 1:
            out.add(b[: -len(suf)])
    for pre in TICKER_PREFIXES:
        if b.startswith(pre) and len(b) > len(pre) + 1:
            out.add(b[len(pre):])
    return sorted(out, key=lambda v: (len(v), v))


def _canonical_ticker(base: str) -> str:
    """把代號收斂成標準代號：取最短的變體（MUB→MU、AAPLX→AAPL、ONSTOCK→ON）。

    語意與原本的 min(_ticker_variants(raw), key=len) 完全相同，差別只在
    【長度平手時的決議是確定的】—— _ticker_variants 已改成依 (長度, 字典序) 排序，
    所以取第一個就是穩定的最短者。原本傳 set 給 min()，平手時取到誰由
    PYTHONHASHSEED 決定，同一支幣每次重啟可能歸到不同群
    （實測 XAG 在 7 個 seed 下有時 AG、有時 XA；XPB 有時 PB、有時 XP）。

    ⚠ 已知的另一個問題（本次【不】處理，避免把單純的不確定性修正變成分組語意重寫）：
      「盲取最短」本身對某些代號是錯的 —— XAG（白銀）會被切成不存在的 XA/AG、
      BNB 切成 BN、1000SHIB 切成 1000SHI、ABNB 切成 ABN、XAUT 切成 AUT、
      PAXG 切成 PAX。實測 CrossEx 2317 個代號有 223 個受影響。
      加「剝出來的目標必須真的掛牌」這道關卡可以修好它們（可期現標的 113→120），
      但同時會拆散 Quantinuum（QNTB/QNTG 現貨 + QNTX 合約本來靠被佔用的 QNT 當共同鍵）
      這類「自然詞幹被別的資產佔走」的標的。要動這塊得連錨點模型一起重新設計。
    """
    return _ticker_variants(base)[0]           # 已排序，第一個就是穩定的最短者


def _canonical(base: str, gate_name: str, bn_name: str) -> tuple[str, str, bool]:
    """把交易所各自的代幣代號還原成「同一個標的」的標準代號，並判定是否為 RWA。

    同一支股票在不同市場代號不同，必須先還原才能配對：
      Binance bStocks：MUB(現貨) → MU(合約)     assetName 標 "(bStocks)"
      Gate xStocks   ：AAPLX     → AAPL         name 標 "xStock"
      Ondo 代幣化    ：AAPLON    → AAPL         name 標 "Ondo Tokenized"
    回傳 (標準代號, 顯示名稱, 是否為 RWA)
    """
    b = base.upper()
    gl, bl = (gate_name or "").lower(), (bn_name or "").lower()
    for marker, suffix, raw_name, strip in (
        ("(bstocks)", "B", bn_name, " (bStocks)"),
        ("xstock", "X", gate_name, " xStock"),
        ("ondo tokenized", "ON", gate_name, " Ondo Tokenized"),
    ):
        src = bl if marker == "(bstocks)" else gl
        if marker in src and b.endswith(suffix) and len(b) > len(suffix) + 1:
            stem = b[: -len(suffix)]
            disp = (raw_name or "").replace(strip, "")
            # 撞號防護：stem 本身若是別的資產（如 QNT=Quant 幣）就不可合併，保留原代號
            if _same_underlying(disp, _STEM_NAMES.get(stem, "")):
                return stem, disp, True
            return b, disp, True
    name = bn_name or gate_name
    return b, name or b, _is_rwa(b, name)


async def refresh_universe(key: str, secret: str) -> dict:
    """重建 RWA 標的清單：CrossEx 可交易 ∩ 非加密貨幣 ∩ 有現貨(可買)且有合約(可空)。
    以「標準代號」配對，故 Binance 買 MUB 現貨 × 各所空 MU 合約會正確湊成一組。"""
    rows, gate_names, bn_names, ex_flags = await asyncio.gather(
        _fetch_crossex_symbols(key, secret),
        _fetch_gate_names(),
        _fetch_binance_names(),
        fetch_exchange_rwa_flags(),
    )
    # 撞號防護用：各代號本身的官方名稱（Gate 優先，缺的用 Binance 補）
    _STEM_NAMES.clear()
    _STEM_NAMES.update({k: v for k, v in bn_names.items() if v})
    _STEM_NAMES.update({k: v for k, v in gate_names.items() if v})

    # 先攤平 CrossEx 標的：(代號, 業務別, 交易所)
    entries = []
    for row in rows:
        sym = row.get("symbol") if isinstance(row, dict) else str(row)
        parts = (sym or "").split("_")
        if len(parts) < 4:
            continue
        ex5011 = CROSSEX_TO_5011.get(parts[0])
        if ex5011:
            entries.append(("_".join(parts[2:-1]).upper(), parts[1], ex5011))

    def name_of(b):
        return bn_names.get(b) or gate_names.get(b) or ""

    # 第一階段：找「錨點」——名稱明確標示為代幣化資產者（各所命名規則不同，靠官方名稱認）
    anchors: dict[str, dict] = {}                  # 標準代號 -> {name, category}
    for raw, _bt, _ex in entries:
        nm = name_of(raw)
        # 交易所原生旗標優先（Bitget isRwa / Bybit symbolType / OKX instCategory）：
        # 能認出名稱表查不到的標的，如 SSPC（2 倍做空 SPCX）、SPCH。
        native = ex_flags.get(raw)
        if native:
            canon0 = _canonical_ticker(raw)
            cur0 = anchors.get(canon0)
            if not cur0:
                anchors[canon0] = {"name": _strip_markers(nm).title() or raw,
                                   "named": bool(nm), "category": native}
            continue
        if not (_is_rwa(raw, nm) or any(m in (" " + nm.lower()) for m in TOKEN_MARKERS)):
            continue
        company = _strip_markers(nm).title()
        # 標準代號取「剝掉代幣化前後綴後、最短且真的有掛牌的變體」，
        # 讓 MUB/MU、AAPLX/AAPL 收斂到同一個，同時不把 XAG 切成不存在的 XA/AG
        canon = _canonical_ticker(raw)
        cur = anchors.get(canon)
        # 群組名必須優先用「有真實名稱」的那筆。沒名稱的標的若拿代號充當名稱去競爭，
        # 會把正確公司名蓋掉，導致後續名稱比對失敗（例：OPENAIX 無名稱 → 用代號
        # 'OPENAIX' 當群組名，使現貨 OPENAI 的名稱 'OpenAI' 比對不符而被丟掉）。
        if not cur or (company and (not cur.get("named") or len(company) > len(cur["name"]))):
            anchors[canon] = {"name": company or raw, "named": bool(company),
                              "category": "commodity" if _is_commodity(nm) else "equity"}

    # 補充錨點（結構性特徵）：現貨代號要「剝掉代幣化前後綴」才對得上合約代號，
    # 這是代幣化資產獨有的樣態——加密貨幣的現貨與合約用同一個代號，不需要轉換。
    # 例：XADBE(現貨) → ADBE(合約)、BACG(現貨) → BAC(合約)。這類標的的現貨代幣往往
    # 不在任何幣種名稱表裡（查不到名稱），只能靠這個結構訊號認出來。
    # 寧可寬鬆納入，是否真為同一標的由【價格驗證】把關（如 ARB→AR 會因價差過大被擋）。
    fut_bases = {raw for raw, bt, _ in entries if bt == "FUTURE"}
    spot_bases = {raw for raw, bt, _ in entries if bt != "FUTURE"}
    for raw, bt, _ex in entries:
        if bt == "FUTURE":
            continue
        for v in _ticker_variants(raw):
            # v 自己就有現貨 → 它是「現貨與合約同代號」的加密貨幣（如 ARB 的變體對到
            # AR=Arweave，而 AR 本身現貨合約都有），不是代幣化資產，不可建錨點。
            if v in spot_bases:
                continue
            if v != raw and v in fut_bases and v not in anchors:
                nm2 = name_of(v) or name_of(raw)
                anchors[v] = {"name": _strip_markers(nm2).title() or v,
                              "category": "commodity" if _is_commodity(nm2) else "equity"}

    # 第二階段：把所有標的歸入錨點群。代號變體對得上就先納入候選；
    # 若雙方都有名稱卻互相矛盾（如 QNT=Quant 幣 vs QNTB=Quantinuum 股）則排除。
    # 真正是否為同一標的，最後由【價格】在 build_opportunities 驗證。
    legs: dict[str, dict[str, dict]] = {}
    meta: dict[str, dict] = {}
    for raw, bt, ex5011 in entries:
        # 名稱檢查只對【現貨腿】做：幣種名稱表描述的是「可買到的代幣」，用來擋
        # 買錯標的（QNT=Quant 幣 vs QNTB=Quantinuum 股）。
        # 【合約腿】不可用名稱檢查——合約標的與幣種代碼是不同命名空間，會誤殺：
        # 例 AMD 合約是超微股票永續，但幣種表裡 AMD='Armenian Dram'（亞美尼亞貨幣）。
        # 合約腿是否真的同一標的，改由 build_opportunities 的【價格驗證】把關。
        nm = name_of(raw) if bt != "FUTURE" else ""
        # 合約腿原則上不比名稱（幣種表描述的是現貨代幣，對合約標的不可靠，例 AMD 合約是
        # 超微股票但幣種表 AMD='Armenian Dram'）。但若該代號【自己就有現貨掛牌】，
        # 幣種表對它就是可信的 —— 此時名稱衝突代表是另一個資產，必須排除：
        #   CVX 有自己的現貨且名稱是 'Convex Finance'（幣），與群組 'Chevron'（股）衝突
        #   QNT 有自己的現貨且是 'Quant'（幣），與群組 'Quantinuum'（股）衝突
        if bt == "FUTURE" and raw in spot_bases:
            nm = name_of(raw)
        # 代幣本身已標明是代幣化資產（bStocks/xStock/Ondo）→ 代號對得上就是它，
        # 不必再比名稱：公司改名（MicroStrategy→Strategy）或用代號當名稱
        # （"SPY (bStocks)" vs "SPDR S&P 500 ETF"）都會讓名稱比對誤殺。
        tokenized = any(m in (" " + (nm or "").lower()) for m in TOKEN_MARKERS)
        # ⚠ 由【長到短】比對錨點（reversed）——與收斂用的短優先【相反】，這是刻意的：
        #   anchors 的鍵已經是收斂過的標準代號，所以「對得上的最長變體」就是最精確的群。
        #   實例：ALABB(Astera Labs 現貨) 建出錨點 ALAB，而 ALAB(合約) 自己收斂成 ALA，
        #   於是 anchors 同時有 ALAB 與 ALA 兩個鍵。合約腿 ALAB 若短優先會落到 ALA
        #   （那是另一支加密貨幣），與現貨腿 ALABB 所在的 ALAB 群拆開 → 期現配對消失。
        #   長優先則 ALAB 直接命中 ALAB 群，兩腿團聚。
        #   同理保住 LRCX(Lam Research)、SAMSUNG、XLE、SPCX 不被切成 LRC/SAMSUN/LE/SPC。
        #   而 NVDAX 因為 NVDAX 本身不是錨點鍵（錨點是 NVDA），仍會正確收斂到 NVDA。
        #   舊碼這裡吃 set 的亂序，等於每次重啟擲骰子決定腿要歸哪一群。
        for canon in reversed(_ticker_variants(raw)):
            a = anchors.get(canon)
            if not a:
                continue
            if nm and not tokenized and not _names_match(nm, a["name"]):
                continue                            # 現貨名稱明確不符 → 不是同一標的
            # 同一交易所可能同時上多種代幣化版本（如 Gate 有 MAON 與 MAX），
            # 必須全部保留，之後由價格/價差挑最佳腿。
            legs.setdefault(canon, {}).setdefault(bt, {}).setdefault(ex5011, []).append(raw)
            meta.setdefault(canon, {"name": a["name"], "rwa": True, "category": a["category"]})
            break

    universe = {}
    for canon, bt in legs.items():
        if not meta.get(canon, {}).get("rwa"):
            continue
        buy: dict[str, list] = {}
        for src in (bt.get("SPOT") or {}, bt.get("MARGIN") or {}):
            for ex, syms in src.items():
                buy.setdefault(ex, [])
                buy[ex] += [s for s in syms if s not in buy[ex]]
        short = {ex: list(dict.fromkeys(syms)) for ex, syms in (bt.get("FUTURE") or {}).items()}
        if not buy or not short:
            continue                              # 缺任一腿就做不了期現
        universe[canon] = {
            "name": meta[canon]["name"],
            "spot": buy,                          # {exchange: 該所現貨代號}
            "futures": short,                     # {exchange: 該所合約代號}
            "spot_exchanges": sorted(buy),
            "futures_exchanges": sorted(short),
            "category": meta[canon].get("category", "equity"),
        }
    # 另存「只要有合約就算」的清單：跨所價差套利兩腿都是永續，不需要現貨腿
    futures_only = {}
    for canon, bt in legs.items():
        if not meta.get(canon, {}).get("rwa"):
            continue
        short = {ex: list(dict.fromkeys(syms)) for ex, syms in (bt.get("FUTURE") or {}).items()}
        if len(short) < 2:
            continue                              # 至少兩家才有跨所價差
        futures_only[canon] = {
            "name": meta[canon]["name"],
            "futures": short,
            "category": meta[canon].get("category", "equity"),
        }

    # 跨所價差用的 RWA 代號集合：不限 CrossEx。
    # 兩腿都是永續、不需要買現貨，故不受「CrossEx 帳戶能否買到」限制；
    # 且實際機會常出現在 CrossEx 以外的所（如 SSPC 在 Bitget vs Bybit、群友案例用 MEXC）。
    rwa_bases = {b: c for b, c in ex_flags.items()}
    for b, nm in {**gate_names, **bn_names}.items():
        if b in rwa_bases:
            continue
        if _is_rwa(b, nm) or any(m in (" " + (nm or "").lower()) for m in TOKEN_MARKERS):
            rwa_bases[b.upper()] = "commodity" if _is_commodity(nm) else "equity"

    # Gate 合成 RWA（美股/港股/A股/指數/商品，無代幣化標記）：先用名稱分類，
    # 名稱判不出來的再用價格交叉比對定案。
    synth = await _fetch_gate_synthetic()
    for b, nm in synth.items():
        # 已由 8h 資費 + 假位址確認是 RWA；名稱只用來細分 商品/股票
        cat = _classify_by_name(nm) or "equity"
        if b not in rwa_bases or (cat == "commodity" and rwa_bases[b] != "commodity"):
            rwa_bases[b] = cat            # 名稱能判定為商品時優先（交易所旗標一律給 equity）
    # 最終校正：交易所旗標一律給 equity，用官方名稱把商品（黃金/白銀/鉑/原油…）改回
    # commodity —— 這會影響「是否受美股時段限制」的顯示（商品 24 小時交易）。
    all_names = {**gate_names, **bn_names, **synth}
    for b in list(rwa_bases):
        nm = all_names.get(b)
        if nm and _is_commodity(nm):
            rwa_bases[b] = "commodity"
    logger.info(f"[rwa] Gate 合成 RWA 標的：{len(synth)} 個")

    # 槓桿 ETF 對照表：{代號: {標的, 倍數, 名稱}}。由官方 ETF 名稱自動解析
    # （"Direxion Daily TSLA Bull 2X Shares" → TSLA ×+2），名稱解析不出的用手動補充。
    leveraged: dict[str, dict] = {}
    for b, nm in {**gate_names, **bn_names, **synth}.items():
        parsed = parse_leveraged(nm)
        if parsed:
            und, mult = parsed
            leveraged[b.upper()] = {"underlying": und, "multiplier": mult, "name": nm}
    for b, (und, mult) in LEVERAGED_OVERRIDES.items():
        leveraged.setdefault(b, {"underlying": und, "multiplier": mult, "name": b})
    logger.info(f"[rwa] 槓桿 ETF 對照表：{len(leveraged)} 個")

    data = {"_ts": time.time(), "universe": universe, "futures_universe": futures_only,
            "rwa_bases": rwa_bases, "leveraged": leveraged}
    try:
        UNIVERSE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(UNIVERSE_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=1)
    except Exception as e:
        logger.warning(f"[rwa] 標的清單寫入失敗: {e}")
    logger.info(f"[rwa] 標的清單更新：{len(universe)} 個可期現的 RWA")
    return data


async def _fetch_gate_names() -> dict[str, str]:
    """Gate 全幣種官方名稱（判斷是否非加密貨幣的依據，如 "NVIDIA xStock"）"""
    async with make_session(25) as s:
        async with s.get(GATE_CURRENCIES) as r:
            return {c["currency"]: (c.get("name") or "") for c in await r.json()}


async def fetch_exchange_rwa_flags() -> dict[str, str]:
    """各交易所【原生的 RWA 分類旗標】→ {base: category}。

    這比用幣種名稱猜可靠得多（名稱只有 Gate/Binance 有，且對純合約標的不可用）：
      Bitget `isRwa: "YES"`（合約規格端點，274 個）
      Bybit  `symbolType: stock / commodity`（169 個）
      OKX    `instCategory: "4"`（商品類，XAU/XAG 屬之）
    可認出只在這些所上市、名稱查不到的標的（如 SSPC=2倍做空SPCX、SPCH）。
    """
    flags: dict[str, str] = {}

    async def bitget():
        async with make_session(20) as s:
            async with s.get("https://api.bitget.com/api/v2/mix/market/contracts",
                             params={"productType": "USDT-FUTURES"}) as r:
                d = await r.json()
        for x in d.get("data") or []:
            if x.get("isRwa") == "YES" and x.get("baseCoin"):
                flags.setdefault(x["baseCoin"].upper(), "equity")

    async def bybit():
        async with make_session(20) as s:
            async with s.get("https://api.bybit.com/v5/market/instruments-info",
                             params={"category": "linear", "limit": "1000"}) as r:
                d = await r.json()
        for x in ((d.get("result") or {}).get("list") or []):
            t = x.get("symbolType")
            if t in ("stock", "commodity") and x.get("baseCoin"):
                flags[x["baseCoin"].upper()] = "commodity" if t == "commodity" else "equity"

    async def okx():
        async with make_session(20) as s:
            async with s.get("https://www.okx.com/api/v5/public/instruments",
                             params={"instType": "SWAP"}) as r:
                d = await r.json()
        for x in d.get("data") or []:
            if x.get("instCategory") == "4" and x.get("baseCcy"):
                flags.setdefault(x["baseCcy"].upper(), "commodity")

    for fn in (bitget, bybit, okx):
        try:
            await fn()
        except Exception as e:
            logger.warning(f"[rwa] {fn.__name__} RWA 旗標取得失敗: {e}")
    logger.info(f"[rwa] 交易所原生 RWA 旗標：{len(flags)} 個標的")
    return flags


CORP_RE = r"\b(inc|corp|corporation|ltd|limited|plc|llc|co|company|group|holdings?|technologies|technology|nv|sa|ag|se|as)\b\.?"
LISTED_RE = r"\d{4,6}\.(HK|SH|SZ|TW|T)\b"
INDEX_WORDS = ("index", "average", "volatility", "msci", "s&p", "nasdaq", "dow", "ftse",
               "nikkei", "hang seng", "russell", "sector", "etf", "shares", "trust")


def _classify_by_name(name: str) -> str | None:
    """由官方名稱判斷資產類別；判不出來回 None（交由價格交叉比對）。"""
    import re
    n = (name or "").lower()
    if not n:
        return None
    if re.search(LISTED_RE, name or "", re.I):
        return "equity"                      # 港股/A股：帶 01211.HK 這種掛牌代碼
    if _is_commodity(name):
        return "commodity"
    if any(k in n for k in INDEX_WORDS):
        return "equity"                      # 指數/ETF 歸入 equity（受美股時段影響）
    if re.search(CORP_RE, n):
        return "equity"                      # 公司名字尾（Inc. / Ltd. / Group…）
    return None


async def _fetch_gate_synthetic() -> dict[str, str]:
    """Gate 的「合成 RWA」標的：美股/港股/A股/指數/商品的合約。這些標的【沒有任何
    代幣化標記】（名稱就是 "MSCI Taiwan"、"AbbVie"），Bitget/Bybit 的旗標也涵蓋不到，
    是先前 RWA 判定的系統性缺口（如 TW88=MSCI 台灣指數）。回 {base: name}。

    兩個判定條件（實測 351 個標的驗證）：
      ① 鏈上位址是假位址 invalid-XXX-…（不可充提的合成標的）
      ② 資費週期 8h —— 這是關鍵區分點：Gate 的合成 RWA 一律 8h，
         而同樣用假位址的「尚未上鏈的加密貨幣」（ARX/BASED/BULLA…）是 4h。
         實測：名稱可確認為 RWA 的 154 個有 8h、只有 2 個 4h；4h 那組 29 個全是幣。
    """
    try:
        async with make_session(30) as s:
            async with s.get(GATE_CURRENCIES) as r:
                cur = await r.json()
            async with s.get("https://api.gateio.ws/api/v4/futures/usdt/contracts") as r:
                fut = await r.json()
        fut_by_base = {c["name"].replace("_USDT", ""): c for c in fut}
        out = {}
        for c in cur:
            b = c.get("currency")
            fc = fut_by_base.get(b) if b else None
            if not fc or fc.get("funding_interval") != 28800:
                continue
            if any(str(ch.get("addr", "")).startswith("invalid-") for ch in (c.get("chains") or [])):
                out[b.upper()] = c.get("name") or ""
        return out
    except Exception as e:
        logger.warning(f"[rwa] Gate 合成標的取得失敗: {e}")
        return {}


async def _fetch_binance_names() -> dict[str, str]:
    """Binance 全幣種官方名稱（bStocks 會標 "(bStocks)"，如 "Micron Technology (bStocks)"）"""
    try:
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        async with make_session(20, headers=headers) as s:
            async with s.get(BINANCE_ASSETS) as r:
                d = await r.json()
        items = d.get("data") if isinstance(d, dict) else d
        return {x.get("assetCode"): (x.get("assetName") or "") for x in (items or []) if x.get("assetCode")}
    except Exception as e:
        logger.warning(f"[rwa] Binance 幣種名稱取得失敗（bStocks 將無法判定）: {e}")
        return {}


def load_universe() -> dict:
    try:
        with open(UNIVERSE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"_ts": 0, "universe": {}}


# ── 機會計算 ─────────────────────────────────────────────────

def build_cross_exchange_spreads(scanner) -> list[dict]:
    """RWA 跨所價差套利：同一標的的永續在不同交易所的價差。

    做多便宜的所 + 做空貴的所，等價差收斂。算式與「幣種詳情」的套利建議一致：
      進場成本 spread_pct = (做多ask − 做空bid) / 做空bid × 100   （負數 = 有利）
      資費差   diff_pct   = 做空方費率 − 做多方費率（正規化到較長週期）
      預計利潤 = diff_pct − spread_pct                          （即「價差 − 資費」）

    ⚠ 同名不同標的防護：同一代號在不同所可能是完全不同的東西
      （實例：ON 在 binance 是 $0.238 的加密貨幣、在 bybit/okx 是 $80 的 ON Semiconductor 股票）
      故先用價格分群，只在同一價格叢集內配對。
    """
    data = load_universe() or {}
    rwa_bases = data.get("rwa_bases") or {}
    if not rwa_bases or not scanner.last_result:
        return []

    # 依 RWA 代號分組（涵蓋全部交易所，不限 CrossEx：兩腿都是永續、不需買現貨）。
    # 各所命名不同，用代號變體收斂到同一標準代號（ONSTOCK→ON、XMU→MU、AAPLX→AAPL）。
    groups: dict[str, list] = {}
    names: dict[str, str] = {}
    for r in scanner.last_result.records:
        base = r.symbol.split("/")[0].upper()
        if not (r.bid_price and r.ask_price):
            continue
        for v in _ticker_variants(base):
            if v in rwa_bases:
                groups.setdefault(v, []).append(r)
                names.setdefault(v, rwa_bases[v])
                break

    out = []
    for canon, legs in groups.items():
        meta = {"name": canon, "category": names.get(canon, "equity")}
        if len(legs) < 2:
            continue

        # 價格分群：保留與中位數相近者（隔開同名不同標的）
        prices = sorted(x.mark_price or x.index_price or 0 for x in legs)
        med = prices[len(prices) // 2]
        if not med:
            continue
        lo, hi = med * (1 - XS_PRICE_CLUSTER_PCT / 100), med * (1 + XS_PRICE_CLUSTER_PCT / 100)
        legs = [x for x in legs if lo <= (x.mark_price or x.index_price or 0) <= hi]
        if len(legs) < 2:
            continue

        best = None
        for short in legs:                          # 做空（在此所賣出，成交在 bid）
            for long in legs:                       # 做多（在此所買進，成交在 ask）
                if short.exchange == long.exchange:
                    continue
                if not (short.bid_price and long.ask_price):
                    continue
                spread_pct = (long.ask_price - short.bid_price) / short.bid_price * 100
                norm_h = max(short.funding_interval_h or 8, long.funding_interval_h or 8)
                s_rate = (short.funding_rate or 0) * (norm_h / (short.funding_interval_h or 8))
                l_rate = (long.funding_rate or 0) * (norm_h / (long.funding_interval_h or 8))
                diff_pct = (s_rate - l_rate) * 100
                profit = diff_pct - spread_pct
                if best is None or profit > best["est_profit_pct"]:
                    idx_diff = None
                    if short.index_price and long.index_price:
                        idx_diff = (short.index_price / long.index_price - 1) * 100
                    best = {
                        "base": canon, "name": meta.get("name", canon),
                        "category": meta.get("category", "equity"),
                        "short_exchange": short.exchange, "short_symbol": short.symbol,
                        "short_norm_symbol": short.normalized_symbol or short.symbol,
                        "short_bid": short.bid_price, "short_index": short.index_price,
                        "short_funding": short.funding_rate, "short_interval_h": short.funding_interval_h,
                        "long_exchange": long.exchange, "long_symbol": long.symbol,
                        "long_norm_symbol": long.normalized_symbol or long.symbol,
                        "long_ask": long.ask_price, "long_index": long.index_price,
                        "long_funding": long.funding_rate, "long_interval_h": long.funding_interval_h,
                        "spread_pct": round(spread_pct, 4),
                        "index_diff_pct": round(idx_diff, 4) if idx_diff is not None else None,
                        "funding_diff_pct": round(diff_pct, 4),
                        "norm_interval_h": norm_h,
                        "est_profit_pct": round(profit, 4),
                    }
        if best and best["est_profit_pct"] >= XS_MIN_PROFIT_PCT:
            best["market_state"] = us_market_state() if best["category"] == "equity" else "always"
            out.append(best)

    out.sort(key=lambda x: x["est_profit_pct"], reverse=True)
    return out


async def _fetch_spot_quotes(scanner, want: dict[str, set]) -> dict:
    """want: {exchange_id: {該所現貨代號, ...}} → {(exchange_id, 代號): {bid, ask}}
    沿用 scanner._spot_exchanges 的 ccxt 實例（markets 已載入），不另建連線。"""
    out: dict[tuple, dict] = {}

    async def one(ex_id: str, bases: set):
        ex = scanner._spot_exchanges.get(ex_id)
        markets = getattr(ex, "markets", None) if ex else None
        if not markets:
            return
        sym_of = {}
        for b in bases:
            for quote in ("USDT", "USDC", "USD"):
                s = f"{b}/{quote}"
                if s in markets:
                    sym_of[s] = b
                    break
        if not sym_of:
            return
        try:
            tickers = await ex.fetch_tickers(list(sym_of))
        except Exception as e:
            logger.debug(f"[rwa] {ex_id} 現貨報價失敗: {e}")
            return
        for s, b in sym_of.items():
            t = tickers.get(s) or {}
            if t.get("bid") or t.get("ask"):
                out[(ex_id, b)] = {"bid": t.get("bid"), "ask": t.get("ask"), "symbol": s}

    await asyncio.gather(*[one(ex, bs) for ex, bs in want.items()])
    return out


async def build_opportunities(scanner) -> list[dict]:
    """組出 RWA 期現機會：買現貨(ask) × 空永續(bid)，兩腿都限 CrossEx 可交易。

    收益拆兩塊（與文章一致）：
      funding_apr  ：持有期間持續收的資費年化（空方收，需 funding_rate > 0）
      entry_spread ：進場當下的價差（空單賣價 vs 現貨買價），一次性
    """
    uni = (load_universe() or {}).get("universe") or {}
    if not uni or not scanner.last_result:
        return []

    # 合約腿索引：(exchange, 原始base) -> record
    fut = {}
    for r in scanner.last_result.records:
        fut[(r.exchange, r.symbol.split("/")[0].upper())] = r

    # 要查的現貨報價（用各所實際代號，如 Binance 的 MUB）
    want: dict[str, set] = {}
    for canon, meta in uni.items():
        for ex, raws in (meta.get("spot") or {}).items():
            want.setdefault(ex, set()).update(raws if isinstance(raws, list) else [raws])
    spot = await _fetch_spot_quotes(scanner, want)

    market_state = us_market_state()
    opps = []
    for base, meta in uni.items():
        # 現貨腿：取 ask 最低（買最便宜）
        spot_legs = []
        for ex, raws in (meta.get("spot") or {}).items():
            for raw in (raws if isinstance(raws, list) else [raws]):
                q = spot.get((ex, raw))
                if not (q and q.get("ask")):
                    continue
                bid, ask = q.get("bid"), q["ask"]
                spread_pct = ((ask - bid) / ask * 100) if (bid and ask) else None
                spot_legs.append({"exchange": ex, "bid": bid, "ask": ask,
                                  "symbol": q.get("symbol"), "raw_base": raw,
                                  "bid_ask_spread_pct": round(spread_pct, 4) if spread_pct is not None else None})
        if not spot_legs:
            continue

        # 合約腿：取「資費年化最高」（空方收租，正資費才是收）
        fut_legs = []
        for ex, raws in (meta.get("futures") or {}).items():
            for raw in (raws if isinstance(raws, list) else [raws]):
                r = fut.get((ex, raw.upper()))
                if not r:
                    continue
                fut_legs.append({
                    "exchange": ex, "raw_base": raw, "symbol": r.symbol,
                    # 正規化 symbol：前端開「幣種詳情」要用這個，傳交易所原始代號
                    # （如 Kraken 的 GLDX/USD:USD）後端會查不到房間 → 現貨永遠等不到
                    "norm_symbol": r.normalized_symbol or r.symbol,
                    "bid": r.bid_price, "ask": r.ask_price,
                    "funding_rate": r.funding_rate, "funding_interval_h": r.funding_interval_h,
                    "funding_apr": r.annual_rate,
                    "funding_time": r.funding_time.isoformat() if r.funding_time else None,
                })
        if not fut_legs:
            continue

        # 枚舉所有 (現貨腿 × 合約腿) 組合。同一標的在不同所的代幣份額可能不同
        # （例：TQQQ 分割後 gateio 現貨 127.41 vs binance 現貨 64.13，差 2 倍），
        # 故必須「先確認兩腿基準一致（價差在門檻內）」再挑，不能只取最便宜的現貨。
        def spread_of(sp, fu):
            if not (fu.get("bid") and sp.get("ask")):
                return None
            return (fu["bid"] / sp["ask"] - 1) * 100

        combos = []
        for sp in spot_legs:
            for fu in fut_legs:
                sd = spread_of(sp, fu)
                if sd is None:
                    continue
                combos.append((abs(sd) <= HEDGEABLE_MAX_SPREAD_PCT, fu.get("funding_apr") or -9e9, sd, sp, fu))
        if not combos:
            continue
        # 可對沖優先 → 資費年化高 → 價差有利
        combos.sort(key=lambda c: (c[0], c[1], c[2]), reverse=True)
        hedgeable, _, entry_spread_pct, best_spot, best_fut = combos[0]

        opps.append({
            "base": base,
            "name": meta.get("name", base),
            "category": meta.get("category", "equity"),
            "market_state": market_state if meta.get("category") == "equity" else "always",
            "spot": best_spot,
            "futures": best_fut,
            "funding_apr": best_fut.get("funding_apr"),
            "entry_spread_pct": round(entry_spread_pct, 4) if entry_spread_pct is not None else None,
            "hedgeable": hedgeable,
            "spot_legs": spot_legs,
            "futures_legs": fut_legs,
        })

    # 可對沖的優先，其次比資費年化（持續收租的主要來源）
    opps.sort(key=lambda x: (x.get("hedgeable", False), x.get("funding_apr") or -9e9), reverse=True)
    return opps


# ── 槓桿型交易對套利 ──────────────────────────────────────────
#
# 槓桿 ETF（如 SSPC=2倍做空 SPCX、TSLL=2倍做多 TSLA）追蹤的是【每日重設後的報酬率】，
# 不是固定的價格比。基準是美股收盤（16:00 ET）的官方 NAV：
#     理論價 = 槓桿ETF收盤價 × (1 + 倍數 × 標的自收盤的漲跌%)
# 交易所的永續指數更新有時差（如部分所要到 9:30 才更新美股價），
# 使實際價偏離理論價 → 偏離收斂即套利機會。

YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/{}"

# 名稱解析不出來、或標的代號需對應的手動補充（來源：官方 ETF 說明）
LEVERAGED_OVERRIDES = {
    "SSPC": ("SPCX", -2.0),      # Leverage Shares 2x Short SPCX Daily ETF
    "SPCH": ("SPCX", 2.0),       # Leverage Shares 2x Long SPCX Daily ETF
}
# 名稱裡的標的寫法 → 交易所實際代號
UNDERLYING_ALIAS = {
    "SK HYNIX": "SKHY", "SEMICONDUCTOR": "SOXX",
    "20+ YEAR TREASURY": "TLT", "VIX": "UVXY",
}
_LEV_PATTERNS = [
    (r"daily\s+([\w]+)\s+bull\s+(\d)x", lambda m: (m.group(1), float(m.group(2)))),
    (r"daily\s+([\w]+)\s+bear\s+(\d)x", lambda m: (m.group(1), -float(m.group(2)))),
    (r"(\d)x\s+long\s+([\w\s\+]+?)\s+daily", lambda m: (m.group(2), float(m.group(1)))),
    (r"(\d)x\s+short\s+([\w\s\+]+?)\s+daily", lambda m: (m.group(2), -float(m.group(1)))),
    (r"ultrapro\s+short\s+([\w]+)", lambda m: (m.group(1), -3.0)),
    (r"ultrapro\s+([\w]+)", lambda m: (m.group(1), 3.0)),
    (r"ultrashort\s+([\w\s\+]+)", lambda m: (m.group(1), -2.0)),
]

LEV_MIN_DEVIATION_PCT = 1.0      # 偏離超過此值才列出／通知（群友實務門檻）


def parse_leveraged(name: str) -> tuple[str, float] | None:
    """從官方 ETF 名稱解析出 (標的代號, 倍數)。倍數為負代表反向。"""
    import re
    low = (name or "").lower()
    if not any(k in low for k in ("bull", "bear", "ultra", "2x", "3x")):
        return None
    for pat, fn in _LEV_PATTERNS:
        m = re.search(pat, low)
        if m:
            und, mult = fn(m)
            und = und.strip().upper()
            return UNDERLYING_ALIAS.get(und, und), mult
    return None


async def _fetch_yahoo_closes(symbols: list[str]) -> dict[str, float]:
    """美股官方收盤價（槓桿 ETF 每日重設的基準）。收盤 = 16:00 ET。"""
    out: dict[str, float] = {}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/131.0.0.0"}

    async def one(sess, sym):
        try:
            async with sess.get(YAHOO_CHART.format(sym), params={"interval": "1d", "range": "5d"}) as r:
                if r.status != 200:
                    return
                d = await r.json()
            res = ((d.get("chart") or {}).get("result") or [])
            if not res:
                return
            closes = [c for c in res[0]["indicators"]["quote"][0]["close"] if c is not None]
            if closes:
                out[sym] = float(closes[-1])
        except Exception:
            pass

    async with make_session(20, headers=headers) as sess:
        for i in range(0, len(symbols), 8):        # 分批，避免對 Yahoo 過度併發
            await asyncio.gather(*[one(sess, s) for s in symbols[i:i + 8]])
    return out


async def build_leveraged_pairs(scanner) -> list[dict]:
    """槓桿型交易對套利：槓桿 ETF 實際價 vs 由標的推算的理論價之偏離。"""
    data = load_universe() or {}
    lev_map = data.get("leveraged") or {}
    if not lev_map or not scanner.last_result:
        return []

    # 各所報價（同代號取報價最好的一家）
    px: dict[str, dict] = {}
    for r in scanner.last_result.records:
        b = r.symbol.split("/")[0].upper()
        if not (r.bid_price and r.ask_price):
            continue
        cur = px.get(b)
        if not cur or (r.mark_price and not cur.get("mark")):
            px[b] = {"bid": r.bid_price, "ask": r.ask_price, "mark": r.mark_price,
                     "exchange": r.exchange, "symbol": r.symbol,
                     "norm_symbol": r.normalized_symbol or r.symbol,
                     "funding_rate": r.funding_rate, "funding_interval_h": r.funding_interval_h}

    pairs = [(lev, m["underlying"], m["multiplier"], m.get("name", lev))
             for lev, m in lev_map.items() if lev in px and m["underlying"] in px]
    if not pairs:
        return []

    closes = await _fetch_yahoo_closes(sorted({s for p in pairs for s in (p[0], p[1])}))
    state = us_market_state()
    out = []
    for lev, und, mult, name in pairs:
        lc, uc = closes.get(lev), closes.get(und)
        if not (lc and uc):
            continue                              # 沒有官方收盤基準就無法算理論價
        lp, up = px[lev], px[und]
        u_now = up.get("mark") or (up["bid"] + up["ask"]) / 2
        l_now = lp.get("mark") or (lp["bid"] + lp["ask"]) / 2
        u_chg = u_now / uc - 1
        theo = lc * (1 + mult * u_chg)
        if theo <= 0:
            continue
        dev = (l_now / theo - 1) * 100
        if abs(dev) < LEV_MIN_DEVIATION_PCT:
            continue
        out.append({
            "leveraged": lev, "underlying": und, "multiplier": mult, "name": name,
            "lev_close": lc, "und_close": uc,
            "und_price": u_now, "und_change_pct": round(u_chg * 100, 4),
            "lev_price": l_now, "lev_theoretical": round(theo, 6),
            "deviation_pct": round(dev, 4),
            # 偏離為負 = 槓桿 ETF 便宜 → 做多它；為正 = 貴 → 做空它
            "action": "做多槓桿ETF" if dev < 0 else "做空槓桿ETF",
            "lev_exchange": lp["exchange"], "lev_symbol": lp["symbol"],
            "lev_norm_symbol": lp["norm_symbol"],
            "lev_bid": lp["bid"], "lev_ask": lp["ask"],
            "lev_funding": lp["funding_rate"], "lev_interval_h": lp["funding_interval_h"],
            "und_exchange": up["exchange"], "und_symbol": up["symbol"],
            "und_norm_symbol": up["norm_symbol"],
            "market_state": state,
        })
    out.sort(key=lambda x: abs(x["deviation_pct"]), reverse=True)
    return out


# ── Gate 現貨槓桿代幣（3L/3S/5L/5S）單邊機會 ──────────────────────────
#
# 與上面「槓桿 ETF 永續對」是完全不同的商品，不能共用公式：
#   • 上面那些是【美股槓桿 ETF 的永續合約】——標的每日在美股收盤 16:00 ET 重設。
#   • 這裡是【Gate 自家發行的再平衡槓桿代幣】（XAU3L = "XAU3xLong"），只有現貨、沒有合約。
#     官方再平衡規則：每日 16:00 UTC 定時檢查，3L 的實際槓桿在 2.25x~4.125x 內不調整、
#     超出區間或標的日漲跌 >1% 才調回 3x；另有標的變動 ±20% 的不定時再平衡。
#     來源：Gate 幫助中心「ETF 槓桿代幣再平衡機制」與公告 17352。
#   → 所以基準點是【上一個 16:00 UTC】，不是美股收盤。
#
# 為什麼只列「偏離為負」：這些代幣不在 Gate 統一杠桿的 763 個可借對裡（實測 0 個），
# 借不到就無法做空 → 偏離為正（代幣偏貴）時無法進場，只有偏離為負（偏便宜）能買。
#
# 對沖腿：買 1 份代幣（N U）→ 空 |倍數| × N U 名目的【標的 Gate 永續】，Delta≈0。
# （之前誤判成「無法對沖」是錯的：不能做空的是代幣本身，對沖腿用標的永續即可。）
GATE_SPOT_PAIRS = "https://api.gateio.ws/api/v4/spot/currency_pairs"
GATE_SPOT_TICKERS = "https://api.gateio.ws/api/v4/spot/tickers"
GATE_SPOT_KLINE = "https://api.gateio.ws/api/v4/spot/candlesticks"
GATE_FUT_CONTRACTS = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
GATE_FUT_TICKERS = "https://api.gateio.ws/api/v4/futures/usdt/tickers"
GATE_FUT_KLINE = "https://api.gateio.ws/api/v4/futures/usdt/candlesticks"

GATE_ETF_REBALANCE_UTC_HOUR = 16     # Gate 槓桿代幣定時再平衡（官方 2023 起由 00:00 改 16:00 UTC）
LEV_SPOT_MIN_DEVIATION_PCT = 0.5     # 偏離低於此值視為追蹤誤差雜訊（實測絕對值中位數 0.34%）
LEV_SPOT_MIN_VOLUME_USDT = 5_000     # 24h 成交額門檻，濾掉根本吃不到量的殭屍代幣
_LEV_TOKEN_RE = r"([A-Z0-9]+)([35])([LS])"


async def build_gate_leveraged_spot(scanner) -> list[dict]:
    """Gate 現貨槓桿代幣 vs 由標的永續推算的理論價，只回「偏離為負＝可買進」的。"""
    import re

    data = load_universe() or {}
    rwa_bases = data.get("rwa_bases") or {}
    if not rwa_bases:
        return []

    now = int(time.time())
    anchor = now - ((now - GATE_ETF_REBALANCE_UTC_HOUR * 3600) % 86400)

    async def jget(sess, url, **params):
        async with sess.get(url, params=params) as r:
            if r.status != 200:
                raise Exception(f"HTTP {r.status}")
            return await r.json()

    async with make_session(25) as sess:
        try:
            pairs, contracts, sp_tk, fu_tk = await asyncio.gather(
                jget(sess, GATE_SPOT_PAIRS), jget(sess, GATE_FUT_CONTRACTS),
                jget(sess, GATE_SPOT_TICKERS), jget(sess, GATE_FUT_TICKERS))
        except Exception as e:
            logger.warning(f"[rwa] Gate 槓桿代幣清單抓取失敗: {e}")
            return []

        perps = {c["name"].split("_")[0]: c for c in contracts
                 if c.get("name", "").endswith("_USDT") and not c.get("in_delisting")}
        sp_price = {t["currency_pair"]: t for t in sp_tk}
        fu_price = {t["contract"]: t for t in fu_tk}

        # 標的必須是 RWA、且【有 Gate 永續】才收——沒有永續就真的無法對沖，不列出
        toks: list[tuple[str, str, float]] = []          # (代幣, 標的, 倍數)
        for p in pairs:
            base = p.get("base") or ""
            if p.get("quote") != "USDT" or p.get("trade_status") != "tradable":
                continue
            m = re.fullmatch(_LEV_TOKEN_RE, base)
            if not m:
                continue
            stem = m.group(1)
            if stem not in rwa_bases or stem not in perps:
                continue
            vol = float((sp_price.get(f"{base}_USDT") or {}).get("quote_volume") or 0)
            if vol < LEV_SPOT_MIN_VOLUME_USDT:
                continue
            toks.append((base, stem, float(m.group(2)) * (1 if m.group(3) == "L" else -1)))
        if not toks:
            return []

        # 錨點價（上一個 16:00 UTC 那根 1h K 的開盤）：代幣走現貨、標的走永續
        async def spot_open(pair):
            k = await jget(sess, GATE_SPOT_KLINE, currency_pair=pair, interval="1h",
                           **{"from": str(anchor), "limit": "1"})
            return float(k[0][5]) if k else None        # [t, quoteVol, close, high, low, open]

        async def fut_open(name):
            k = await jget(sess, GATE_FUT_KLINE, contract=name, interval="1h",
                           **{"from": str(anchor), "limit": "1"})
            return float(k[0]["o"]) if k else None

        async def gather_anchor(fn, keys):
            out: dict[str, float] = {}
            for i in range(0, len(keys), 8):            # 分批，共用 connector 的 per-host 上限是 10
                chunk = keys[i:i + 8]
                res = await asyncio.gather(*[fn(k) for k in chunk], return_exceptions=True)
                out.update({k: v for k, v in zip(chunk, res) if isinstance(v, float) and v > 0})
            return out

        lev_anchor = await gather_anchor(spot_open, [f"{t[0]}_USDT" for t in toks])
        und_anchor = await gather_anchor(fut_open, sorted({f"{t[1]}_USDT" for t in toks}))

    # 標的永續在 5011 裡的正規化代號（給「幣種詳情」用）
    norm: dict[str, str] = {}
    for r in (scanner.last_result.records if scanner.last_result else []):
        if r.exchange == "gateio":
            norm.setdefault(r.symbol.split("/")[0].upper(), r.normalized_symbol or r.symbol)

    hours = round((now - anchor) / 3600, 1)
    out = []
    for tok, stem, mult in toks:
        la, ua = lev_anchor.get(f"{tok}_USDT"), und_anchor.get(f"{stem}_USDT")
        lt, ft = sp_price.get(f"{tok}_USDT"), fu_price.get(f"{stem}_USDT")
        if not (la and ua and lt and ft):
            continue
        try:
            l_now = float(lt["last"])
            u_now = float(ft["mark_price"])
        except (KeyError, TypeError, ValueError):
            continue
        if not (l_now > 0 and u_now > 0):
            continue
        theo = la * (1 + mult * (u_now / ua - 1))
        if theo <= 0:
            continue
        dev = (l_now / theo - 1) * 100
        if dev > -LEV_SPOT_MIN_DEVIATION_PCT:
            continue                                    # 只留「偏離為負＝代幣便宜＝可買進」
        c = perps[stem]
        try:
            fr = float(c.get("funding_rate")) if c.get("funding_rate") is not None else None
        except (TypeError, ValueError):
            fr = None
        out.append({
            "token": tok, "underlying": stem, "multiplier": mult,
            "token_price": l_now, "token_theoretical": round(theo, 8),
            "deviation_pct": round(dev, 4),
            "token_anchor": la, "und_anchor": ua, "und_price": u_now,
            "und_change_pct": round((u_now / ua - 1) * 100, 4),
            "anchor_ts": anchor, "hours_since_anchor": hours,
            "volume_24h_usdt": round(float(lt.get("quote_volume") or 0)),
            "category": rwa_bases.get(stem, "equity"),
            # 兩腿：現貨在 Gate 買代幣，對沖在 Gate 永續空 |倍數| 倍名目
            "spot_pair": f"{tok}_USDT",
            "hedge_contract": f"{stem}_USDT",
            "hedge_notional_x": abs(mult),
            "hedge_side": "空" if mult > 0 else "多",     # 3S 是反向，對沖腿要做多標的
            "hedge_funding": fr,
            "hedge_interval_h": round((c.get("funding_interval") or 28800) / 3600),
            "und_norm_symbol": norm.get(stem, f"{stem}/USDT"),
            "market_state": us_market_state(),
        })
    out.sort(key=lambda x: x["deviation_pct"])          # 偏離越負排越前
    return out

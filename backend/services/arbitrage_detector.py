"""套利偵測：找出跨交易所 funding rate 套利機會"""

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from models import FundingRecord, ArbitrageOpportunity

logger = logging.getLogger(__name__)

ALIASES_PATH = Path(__file__).resolve().parent.parent / "symbol_aliases.json"
CONTRACTS_PATH = Path(__file__).resolve().parent.parent / "data" / "token_contracts.json"

# 全域別名：variant -> canonical（任何交易所都適用，例如 PLTRX -> PLTR）
_alias_map: dict[str, str] = {}
# 交易所專屬別名：exchange -> {variant -> canonical}（僅特定交易所適用）
_exchange_alias_map: dict[str, dict[str, str]] = {}
# 合約地址對照表：{coin: {exchange: {chain: contract_address}}}
_token_contracts: dict[str, dict[str, dict[str, str]]] = {}


def _load_aliases():
    """從 symbol_aliases.json 載入別名對照表。
    使用 in-place 更新（clear + update）而非重新賦值，
    確保所有已 import _alias_map 參考的模組都能即時看到新資料。
    """
    new_alias: dict[str, str] = {}
    new_exchange: dict[str, dict[str, str]] = {}
    try:
        if ALIASES_PATH.exists():
            with open(ALIASES_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            for variant, canonical in data.get("aliases", {}).items():
                if not variant.startswith("_"):
                    new_alias[variant.upper()] = canonical.upper()
            for exchange, mappings in data.get("exchange_aliases", {}).items():
                ex_map = {}
                for variant, canonical in mappings.items():
                    ex_map[variant.upper()] = canonical.upper()
                new_exchange[exchange.lower()] = ex_map
            total = len(new_alias) + sum(len(m) for m in new_exchange.values())
            if total:
                logger.info(f"載入 {len(new_alias)} 筆全域別名 + {len(new_exchange)} 個交易所專屬別名")
    except Exception as e:
        logger.warning(f"載入幣種別名失敗: {e}")
    # 解析失敗（例：手改 JSON 打錯字、寫了 // 註解）時 new_* 是空的，
    # 若照樣 clear+update 會把整張別名表靜默清空 → 全庫分組瞬間走樣、
    # 下一輪掃描還會把對不上的費率歷史刪掉。空表一律視為載入失敗，保留舊表。
    if not new_alias and not new_exchange:
        if _alias_map or _exchange_alias_map:
            logger.error("幣種別名載入結果為空，保留既有別名表（請檢查 symbol_aliases.json 格式）")
        return
    # in-place 更新：保留物件位址，讓所有已 import 的模組即時生效
    _alias_map.clear()
    _alias_map.update(new_alias)
    _exchange_alias_map.clear()
    _exchange_alias_map.update(new_exchange)


def _strip_1000x(base: str) -> tuple[str, int]:
    """偵測 1000X 命名慣例（如 1000PEPE → PEPE，multiplier=1000）"""
    if base.startswith("1000") and len(base) > 4:
        candidate = base[4:]
        if len(candidate) >= 2:
            return candidate, 1000
    return base, 1


def _normalize_symbol(symbol: str, exchange: str = None) -> str:
    """將 symbol 正規化為統一名稱（套利分組用）
    優先查交易所專屬別名，再查全域別名，再處理 STOCK 後綴 / 1000X 慣例。
    """
    base = symbol.split("/")[0].upper()
    # 交易所專屬別名優先
    if exchange:
        ex_map = _exchange_alias_map.get(exchange.lower(), {})
        if base in ex_map:
            return f"{ex_map[base]}/USDT:USDT"
    # 股票類代幣：MEXC 用 XXXSTOCK 後綴（如 CRCLSTOCK → CRCL）
    # 衝突幣種（如 CATSTOCK vs 加密貨幣 CAT）需在 exchange_aliases 中覆寫
    if base.endswith("STOCK") and len(base) > 5:
        stripped_stock = base[:-5]
        canonical = _alias_map.get(stripped_stock, stripped_stock)
        return f"{canonical}/USDT:USDT"
    # BingX NC 系列合約：NCSK{ticker}2USD（股票）、NCCO{ticker}2USD（商品）
    # NCSI{ticker}2USD（指數）、NCFX{pair}2USD（外匯）
    if base.startswith("NC") and base.endswith("2USD") and len(base) > 6:
        without_suffix = base[:-4]  # 去 "2USD"
        for prefix in ("NCSK", "NCCO", "NCFX", "NCSI"):
            if without_suffix.startswith(prefix):
                ticker = without_suffix[len(prefix):]
                # 跳過版本號前綴（如 NCCO1, NCSI724）
                while ticker and ticker[0].isdigit():
                    ticker = ticker[1:]
                if ticker:
                    canonical = _alias_map.get(ticker, ticker)
                    return f"{canonical}/USDT:USDT"
                break
    # 全域別名
    canonical = _alias_map.get(base, base)
    if canonical != base:
        return f"{canonical}/USDT:USDT"
    # 1000X 慣例（1000PEPE → PEPE）
    stripped, _ = _strip_1000x(base)
    return f"{stripped}/USDT:USDT"


def _load_token_contracts():
    """從 data/token_contracts.json 載入合約地址對照表"""
    global _token_contracts
    _token_contracts = {}
    try:
        if CONTRACTS_PATH.exists():
            with open(CONTRACTS_PATH, "r", encoding="utf-8") as f:
                _token_contracts = json.load(f)
            if _token_contracts:
                logger.info(f"載入合約地址對照表：{len(_token_contracts)} 個幣種")
    except Exception as e:
        logger.warning(f"載入合約地址對照表失敗: {e}")


def _is_same_coin(coin: str, exchange_a: str, exchange_b: str) -> bool | None:
    """用合約地址判斷兩個交易所的同名幣是否為同一幣種。
    回傳 True=同幣, False=不同幣, None=無法判斷（缺少資料）
    """
    coin_data = _token_contracts.get(coin.upper(), {})
    nets_a = coin_data.get(exchange_a.lower(), {})
    nets_b = coin_data.get(exchange_b.lower(), {})
    if not nets_a or not nets_b:
        return None  # 至少一方沒有合約地址資料

    # 找共同鏈，比對合約地址。
    #
    # ⚠ 原本是「迭代 set、第一條雙方都有地址的鏈就直接 return」，有兩個問題：
    #   1. set 沒有順序且受 PYTHONHASHSEED 隨機化 → 各鏈結論矛盾時，同一組幣
    #      在不同 process 會得到不同答案（實測 token_contracts.json 有 13 組矛盾，
    #      ETH 的 binance vs bitget 跑 6 次有 3 次判成「不同幣」）。
    #      判成不同幣時 detect_arbitrage 會把整個交易所踢出配對，
    #      ETH / STRK / RLUSD 這種主流幣的套利機會就無故消失。
    #   2. 「第一條就定生死」本來就不對：同一支幣在多鏈發行，某條鏈的地址少一個
    #      前導零、或某所填了包裝代幣位址，都會造成單鏈不符。
    #
    # 改成【任一條共同鏈的地址相符就算同幣】，全部可比的鏈都不符才判不同幣。
    # 理由：地址「相符」是強證據（撞地址機率趨近 0），地址「不符」是弱證據
    # （多鏈發行、包裝代幣、資料填寫差異都會造成）。強證據優先，且與迭代順序無關。
    common_chains = sorted(set(nets_a.keys()) & set(nets_b.keys()))
    if not common_chains:
        return None  # 沒有共同鏈可比較

    comparable = 0
    for chain in common_chains:
        addr_a, addr_b = nets_a[chain], nets_b[chain]
        if not (addr_a and addr_b):
            continue
        comparable += 1
        if addr_a.lower() == addr_b.lower():
            return True                      # 任一鏈地址一致 = 同幣（強證據，直接定案）
    if comparable:
        return False                         # 有得比、但沒有任何一條相符 = 不同幣
    return None                              # 共同鏈都缺地址，無法判斷


# 啟動時載入一次
_load_aliases()
_load_token_contracts()


def reload_aliases():
    """重新載入別名與合約地址對照表（供 API 呼叫）"""
    _load_aliases()
    _load_token_contracts()


def detect_arbitrage(
    records: list[FundingRecord],
    min_profit_pct: float = 0.3,  # 預計利潤閾值（%），預設 0.3%
) -> list[ArbitrageOpportunity]:
    """
    按 symbol 分組（考慮別名），找最高/最低費率交易所，
    計算費率差、跨所價差成本、預計利潤，
    預計利潤超過閾值則生成套利機會。
    """
    # 按正規化後的 symbol 分組（考慮交易所專屬別名）
    by_symbol: dict[str, list[FundingRecord]] = defaultdict(list)
    for r in records:
        normalized = _normalize_symbol(r.symbol, r.exchange)
        by_symbol[normalized].append(r)

    opportunities = []

    for symbol, group in by_symbol.items():
        if len(group) < 2:
            continue

        # [自動同名不同幣偵測] 比較 mark_price，價差超過 50% 的排除配對
        # 以中位數為基準，偏離過大的 record 視為不同幣
        priced = [r for r in group if r.mark_price and r.mark_price > 0]
        if len(priced) >= 2:
            prices = sorted(r.mark_price for r in priced)
            median_price = prices[len(prices) // 2]
            valid = []
            for r in group:
                if r.mark_price and r.mark_price > 0:
                    ratio = r.mark_price / median_price
                    if 0.5 <= ratio <= 2.0:
                        valid.append(r)
                    else:
                        logger.warning(
                            f"[同名不同幣] {r.symbol} @ {r.exchange} 價格 {r.mark_price:.6f} "
                            f"偏離中位數 {median_price:.6f} ({ratio:.2f}x)，已排除"
                        )
                else:
                    valid.append(r)  # 無價格的保留（不誤殺）
            group = valid
            if len(group) < 2:
                continue

        # [合約地址比對] 用合約地址排除同名不同幣的配對
        if _token_contracts and len(group) >= 2:
            base_coin = symbol.split("/")[0].upper()
            contract_valid = []
            excluded_exchanges = set()
            for r in group:
                # 檢查這個 record 是否跟組內多數為同一幣
                is_ok = True
                for other in group:
                    if other.exchange == r.exchange:
                        continue
                    result = _is_same_coin(base_coin, r.exchange, other.exchange)
                    if result is False:
                        # 合約地址不同 = 確認不是同一幣
                        logger.warning(
                            f"[合約地址不同] {base_coin} @ {r.exchange} vs {other.exchange}，排除 {r.exchange}"
                        )
                        is_ok = False
                        excluded_exchanges.add(r.exchange)
                        break
                if is_ok:
                    contract_valid.append(r)
            if excluded_exchanges:
                group = contract_valid
                if len(group) < 2:
                    continue

        # 按結算週期分組，只配對相同週期的交易所
        # 跨週期配對有方向性風險（短週期極端費率可能回歸），不是真正的套利
        # 例外：滿資費時允許跨週期（費率到頂不會更高，且交易所常改為 1h 結算）
        interval_groups = {}
        for r in group:
            interval_groups.setdefault(r.funding_interval_h, []).append(r)

        # 建立配對候選：只配對相同週期
        pair_sets = []
        for base_interval, sub_group in interval_groups.items():
            if len(sub_group) >= 2:
                pair_sets.append((base_interval, sub_group))

        # 跨週期配對：依本小時是否結算計算有效費率
        # 情境：BN 1h 結算 vs CW 8h 不結算 → 有效費率差 = BN實際費率 - 0
        if len(interval_groups) > 1:
            now_utc = datetime.now(timezone.utc)

            def _is_settling_soon(r):
                # 1h 以內的週期：每小時必定結算，不需看 funding_time
                if r.funding_interval_h <= 1:
                    return True
                if r.funding_time is None:
                    return False
                secs = (r.funding_time - now_utc).total_seconds()
                return 0 <= secs <= 3600

            def _effective_rate(r):
                # 這個小時會結算 → 用實際費率；不結算 → 算 0
                return r.funding_rate if _is_settling_soon(r) else 0.0

            cross_best = {}  # pair_key -> (ep, long_r, short_r, rd, sp, ls, ss)
            for i, r_a in enumerate(group):
                for r_b in group[i+1:]:
                    if r_a.funding_interval_h == r_b.funding_interval_h:
                        continue  # 同週期已由上方邏輯處理
                    eff_a = _effective_rate(r_a)
                    eff_b = _effective_rate(r_b)
                    if eff_a == 0.0 and eff_b == 0.0:
                        continue  # 兩邊本小時都不結算，無機會

                    if eff_a > eff_b:
                        long_r, short_r = r_b, r_a
                        norm_high, norm_low = eff_a, eff_b
                        ls = _is_settling_soon(r_b)
                        ss = _is_settling_soon(r_a)
                    else:
                        long_r, short_r = r_a, r_b
                        norm_high, norm_low = eff_b, eff_a
                        ls = _is_settling_soon(r_a)
                        ss = _is_settling_soon(r_b)

                    rd = norm_high - norm_low
                    rd_pct = rd * 100
                    la = long_r.ask_price
                    sb = short_r.bid_price
                    sp = (la / sb - 1) * 100 if la and sb and sb > 0 else 0.0
                    # 負價差代表開倉時就有額外收益（Long 所 ask < Short 所 bid），納入計算
                    # 硬性條件：正價差必須嚴格小於費率差（無法確認溢價會即時收斂）
                    if sp >= rd_pct:
                        continue

                    ep = rd_pct - sp

                    if ep > min_profit_pct:
                        pair_key = (long_r.exchange, short_r.exchange)
                        if pair_key not in cross_best or ep > cross_best[pair_key][0]:
                            cross_best[pair_key] = (ep, long_r, short_r, rd, sp, ls, ss)

            cross_seen_pairs = set()
            for pair_key, (ep, long_r, short_r, rd, sp, ls, ss) in cross_best.items():
                if pair_key not in cross_seen_pairs:
                    cross_seen_pairs.add(pair_key)
                    opportunities.append(ArbitrageOpportunity(
                        symbol=symbol,
                        long_symbol=long_r.symbol,
                        short_symbol=short_r.symbol,
                        long_exchange=long_r.exchange,
                        short_exchange=short_r.exchange,
                        long_rate=long_r.funding_rate,
                        short_rate=short_r.funding_rate,
                        long_interval_h=long_r.funding_interval_h,
                        short_interval_h=short_r.funding_interval_h,
                        norm_interval_h=1,
                        rate_diff=round(rd, 8),
                        spread_pct=round(sp, 4),
                        estimated_profit=round(ep, 4),
                        long_is_delisting=long_r.is_delisting,
                        short_is_delisting=short_r.is_delisting,
                        cross_interval=True,
                        long_settling_soon=ls,
                        short_settling_soon=ss,
                    ))

        seen_pairs = set()  # 避免重複配對
        for base_interval, sub_group in pair_sets:
            if len(sub_group) < 2:
                continue

            # 遍歷所有配對，找利潤最高的（費率差 - 價差）
            best = None
            for i, r_a in enumerate(sub_group):
                for r_b in sub_group[i+1:]:
                    # 確定方向：費率高的做空，費率低的做多（同週期直接比即時費率）
                    if r_a.funding_rate > r_b.funding_rate:
                        long_r, short_r = r_b, r_a
                    else:
                        long_r, short_r = r_a, r_b

                    rd = short_r.funding_rate - long_r.funding_rate
                    rd_pct = rd * 100

                    la = long_r.ask_price
                    sb = short_r.bid_price
                    sp = (la / sb - 1) * 100 if la and sb and sb > 0 else 0.0

                    ep = rd_pct - sp
                    if best is None or ep > best[0]:
                        best = (ep, long_r, short_r, rd, sp)

            if best and best[0] > min_profit_pct:
                ep, long_r, short_r, rd, sp = best
                pair_key = (long_r.exchange, short_r.exchange)
                if pair_key not in seen_pairs:
                    seen_pairs.add(pair_key)
                    opportunities.append(ArbitrageOpportunity(
                        symbol=symbol,
                        long_symbol=long_r.symbol,
                        short_symbol=short_r.symbol,
                        long_exchange=long_r.exchange,
                        short_exchange=short_r.exchange,
                        long_rate=long_r.funding_rate,
                        short_rate=short_r.funding_rate,
                        long_interval_h=long_r.funding_interval_h,
                        short_interval_h=short_r.funding_interval_h,
                        norm_interval_h=base_interval,
                        rate_diff=round(rd, 8),
                        spread_pct=round(sp, 4),
                        estimated_profit=round(ep, 4),
                        long_is_delisting=long_r.is_delisting,
                        short_is_delisting=short_r.is_delisting,
                    ))

    # 按預計利潤降序排列
    opportunities.sort(key=lambda x: x.estimated_profit, reverse=True)
    return opportunities

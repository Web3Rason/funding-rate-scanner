"""
一次性復原 constituent_snapshot.json
背景執行，完成後自動退出
"""
import asyncio
import json
import aiohttp
import sys
import os
from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parent
SNAPSHOT_PATH = BASE_DIR / "logs" / "constituent_snapshot.json"
LAST_SCAN_PATH = BASE_DIR / "last_scan.json"

SOURCES = ("binance", "bybit", "okx", "kucoinfutures", "gateio", "bitget", "mexc")

CONSTITUENT_EXCHANGE_ALIASES = {
    "mxc": "mexc",
    "gate": "gateio",
    "okex": "okx",
}

def norm_ex(name: str) -> str:
    raw = str(name).lower()
    return CONSTITUENT_EXCHANGE_ALIASES.get(raw, raw)

def normalize_constituents(raw_list, source):
    result = []
    for item in raw_list:
        if source == "binance":
            result.append({"exchange": norm_ex(item.get("exchange", "")), "weight": float(item.get("weight", 0)), "price": float(item.get("price", 0))})
        elif source == "bybit":
            result.append({"exchange": norm_ex(item.get("exchange", "")), "weight": float(item.get("weight", 0)), "price": float(item.get("price", 0))})
        elif source == "okx":
            result.append({"exchange": norm_ex(item.get("exch", "")), "weight": float(item.get("wgt", 0)), "price": float(item.get("symPx", 0))})
        elif source == "kucoinfutures":
            result.append({"exchange": norm_ex(item.get("exchange", "")), "weight": float(item.get("weight", 0)), "price": float(item.get("price", 0))})
        elif source == "gateio":
            result.append({"exchange": norm_ex(item.get("exchange", "")), "weight": None, "price": None})
        elif source == "bitget":
            result.append({"exchange": norm_ex(item.get("exchange", "")), "weight": float(item.get("weight", 0)), "price": float(item.get("equivalentPrice", 0))})
        elif source == "mexc":
            result.append({"exchange": norm_ex(item.get("exchange", "")), "weight": None, "price": None})
    return result

async def fetch_kucoin_index_map(session):
    url = "https://api-futures.kucoin.com/api/v1/contracts/active"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return {}
            data = await resp.json()
            result = {}
            for item in data.get("data", []):
                base = item.get("baseCurrency", "")
                idx = item.get("indexSymbol", "")
                if base and idx:
                    result[base.upper()] = idx
            return result
    except Exception as e:
        print(f"[KuCoin] index map 失敗: {e}")
        return {}

async def fetch_one(session, source, base, kucoin_index_map=None):
    try:
        if source == "binance":
            for sym in [f"{base}USDT", f"1000{base}USDT"]:
                url = f"https://fapi.binance.com/fapi/v1/constituents?symbol={sym}"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        raw = data.get("constituents", [])
                        if raw:
                            return normalize_constituents(raw, "binance")
            return None
        elif source == "bybit":
            url = f"https://api.bybit.com/v5/market/index-price-components?indexName={base}USDT"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200: return None
                data = await resp.json()
                raw = data.get("result", {}).get("components", [])
                return normalize_constituents(raw, "bybit") if raw else None
        elif source == "okx":
            url = f"https://www.okx.com/api/v5/market/index-components?index={base}-USDT"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200: return None
                data = await resp.json()
                components = data.get("data", {})
                if isinstance(components, list) and components:
                    components = components[0]
                raw = components.get("components", [])
                return normalize_constituents(raw, "okx") if raw else None
        elif source == "kucoinfutures":
            _KU_BASE_ALIAS = {"BTC": "XBT"}
            lookup = _KU_BASE_ALIAS.get(base.upper(), base.upper())
            index_sym = (kucoin_index_map or {}).get(lookup)
            if not index_sym: return None
            url = f"https://api-futures.kucoin.com/api/v1/index/query?symbol={index_sym}&startAt=0&maxCount=1"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200: return None
                data = await resp.json()
                data_list = data.get("data", {}).get("dataList", [])
                if not data_list: return None
                latest = data_list[0]
                raw = latest.get("decompositionList") or latest.get("decomposionList", [])
                return normalize_constituents(raw, "kucoinfutures") if raw else None
        elif source == "gateio":
            url = f"https://api.gateio.ws/api/v4/futures/usdt/index_constituents/{base}_USDT"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200: return None
                data = await resp.json()
                raw = data.get("constituents", [])
                return normalize_constituents(raw, "gateio") if raw else None
        elif source == "bitget":
            url = f"https://api.bitget.com/api/v3/market/index-components?symbol={base}USDT"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200: return None
                data = await resp.json()
                if data.get("code") != "00000": return None
                raw = data.get("data", {}).get("componentList", [])
                return normalize_constituents(raw, "bitget") if raw else None
        elif source == "mexc":
            url = f"https://contract.mexc.com/api/v1/contract/detail?symbol={base}_USDT"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200: return None
                data = await resp.json()
                if not data.get("success"): return None
                origins = data.get("data", {}).get("indexOrigin", [])
                raw = [{"exchange": o} for o in origins]
                return normalize_constituents(raw, "mexc") if raw else None
    except Exception:
        return None

async def main():
    # 讀取 last_scan.json 取得幣種清單
    with open(LAST_SCAN_PATH, encoding="utf-8") as f:
        scan_data = json.load(f)

    # 正規化 symbol -> base coin，按 source 分組
    symbols_by_source = {}
    for r in scan_data.get("records", []):
        ex = r["exchange"]
        if ex not in SOURCES:
            continue
        sym = r.get("normalized_symbol") or r.get("symbol", "")
        base = sym.split("/")[0].upper() if "/" in sym else sym.upper()
        if base.startswith("1000"):
            base = base[4:]
        symbols_by_source.setdefault(ex, set()).add(base)

    # 讀取現有快照（可能是空的）
    try:
        with open(SNAPSHOT_PATH, encoding="utf-8") as f:
            snapshot = json.load(f)
    except Exception:
        snapshot = {}

    now_iso = datetime.now(timezone.utc).isoformat()
    total_fetched = 0
    batch_size = 20
    delay = 1.5

    async with aiohttp.ClientSession() as session:
        # KuCoin index map 先取一次
        kucoin_map = await fetch_kucoin_index_map(session)
        print(f"[KuCoin] index map: {len(kucoin_map)} 個幣種")

        for source, bases in symbols_by_source.items():
            base_list = sorted(bases)
            print(f"\n[{source}] 開始抓取 {len(base_list)} 個幣種...")
            source_count = 0

            for i in range(0, len(base_list), batch_size):
                batch = base_list[i:i + batch_size]
                tasks = [fetch_one(session, source, base, kucoin_map if source == "kucoinfutures" else None) for base in batch]
                results = await asyncio.gather(*tasks)

                for base, constituents in zip(batch, results):
                    if not constituents:
                        continue
                    sym_key = f"{base}/USDT:USDT"
                    if sym_key not in snapshot:
                        snapshot[sym_key] = {}
                    snapshot[sym_key][source] = constituents
                    snapshot[sym_key][f"_updated_{source}"] = now_iso
                    source_count += 1
                    total_fetched += 1

                progress = min(i + batch_size, len(base_list))
                print(f"  [{source}] {progress}/{len(base_list)} ({source_count} 筆有效)", end="\r")

                if i + batch_size < len(base_list):
                    await asyncio.sleep(delay)

            print(f"  [{source}] 完成：{source_count}/{len(base_list)} 筆有效")

            # 每個 source 完成後立即存檔（避免中途崩潰遺失）
            with open(SNAPSHOT_PATH, "w", encoding="utf-8") as f:
                json.dump(snapshot, f, ensure_ascii=False)
            print(f"  [{source}] 已存檔")

    print(f"\n全部完成！共寫入 {total_fetched} 筆成分資料")
    print(f"快照幣種數: {len(snapshot)}")

if __name__ == "__main__":
    asyncio.run(main())

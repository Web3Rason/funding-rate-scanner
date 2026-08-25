"""跨 process 決定性回歸測試（2026-08-07 溢價圖空白事件）

【這個測試在防什麼】
Python 的字串 hash 受 PYTHONHASHSEED 隨機化，所以 set / dict_keys 的迭代順序
【每個 process 都不一樣、但同一 process 內固定】。程式碼裡只要出現
「從 set 取一個元素當答案」，行為就變成後端開機時抽籤決定：
  - 溢價圖：ACE 的變體是 {ACE, 1000ACE}，抽到 1000ACE → 打 Binance 回 400 → 空圖
  - RWA 錨點：XAG 的變體 {XAG, AG, XA} 用 min(key=len) 取，AG 與 XA 長度平手 → 隨機
  - 套利同幣判定：ETH 在 binance/bitget 的共同鏈結論互相矛盾，取第一條 → 隨機

這種 bug 重啟後壞的東西會換一批，極難重現。本測試用【多個固定 seed 跑子行程】把
不確定性直接逼出來：同一份輸入在不同 seed 下必須得到完全相同的輸出。

跑法（backend 目錄下）：
    python tests/test_symbol_determinism.py
"""

import json
import os
import subprocess
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parent.parent
SEEDS = ["0", "1", "2", "7", "42", "12345", "99991"]

# 子行程裡實際執行的探針：只 import 純函式，不啟動掃描器
PROBE = r"""
import json, sys
sys.path.insert(0, r"{backend}")

out = {{}}

# ── 1. RWA 代號變體：順序，以及標準代號的收斂結果 ──
from services.rwa_arb import _ticker_variants, _canonical_ticker
for t in ("XAG", "XPB", "XOM", "XAU", "NVDAX", "AAPLX", "MUB", "ONSTOCK",
          "XMU", "QNTB", "QNTX", "ALAB", "LRCX"):
    out[f"variants:{{t}}"] = list(_ticker_variants(t))
    out[f"canon:{{t}}"] = _canonical_ticker(t)

# ── 2. 套利的同幣判定 ──
from services import arbitrage_detector as ad
for coin, a, b in (("ETH", "binance", "bitget"), ("STRK", "binance", "bitget"),
                   ("RLUSD", "binance", "bitget"), ("VELO", "binance", "bitget"),
                   ("SRM", "binance", "bitget")):
    out[f"same:{{coin}}:{{a}}:{{b}}"] = ad._is_same_coin(coin, a, b)

# ── 3. 溢價查詢的 symbol 變體（不連網，只看挑出來的順序）──
from services.funding_scanner import FundingScanner
sc = FundingScanner.__new__(FundingScanner)      # 不跑 __init__，避免建連線
sc.last_result = None
for s in ("ACE/USDT:USDT", "ETH/USDT:USDT", "BTC/USDT:USDT",
          "1000PEPE/USDT:USDT", "LINK/USDT:USDT"):
    for ex in ("binance", "bybit", "mexc"):
        v = sc._get_symbol_variants(s, ex)
        out[f"symvar:{{s}}:{{ex}}"] = list(v)
        # 消費端實際會拿去打 API 的那一個
        out[f"picked:{{s}}:{{ex}}"] = next(iter(v)).split("/")[0].upper()

print(json.dumps(out, ensure_ascii=False, sort_keys=True))
"""


def run_with_seed(seed: str) -> dict:
    env = {**os.environ, "PYTHONHASHSEED": seed}
    r = subprocess.run([sys.executable, "-c", PROBE.format(backend=str(BACKEND))],
                       capture_output=True, text=True, env=env, cwd=str(BACKEND))
    if r.returncode != 0:
        raise RuntimeError(f"seed={seed} 探針執行失敗:\n{r.stderr[-2000:]}")
    return json.loads(r.stdout.strip().splitlines()[-1])


def main() -> int:
    print(f"用 {len(SEEDS)} 個 PYTHONHASHSEED 各跑一個子行程：{', '.join(SEEDS)}\n")
    results = {s: run_with_seed(s) for s in SEEDS}

    base_seed = SEEDS[0]
    baseline = results[base_seed]
    failures: list[str] = []

    # ── 檢查 A：所有 key 在所有 seed 下必須完全一致 ──
    for key in sorted(baseline):
        vals = {s: results[s].get(key) for s in SEEDS}
        distinct = {json.dumps(v, ensure_ascii=False, sort_keys=True) for v in vals.values()}
        if len(distinct) > 1:
            failures.append(
                f"[不確定] {key}\n" +
                "".join(f"    seed={s:<6} {json.dumps(v, ensure_ascii=False)}\n"
                        for s, v in vals.items()))

    # ── 檢查 B：語意正確性（不只是穩定，還要是對的）──
    # 這些是實打交易所 API 驗過的正確答案（2026-08-07）：
    #   ACEUSDT / ETHUSDT / LINKUSDT 在 Binance 回 200；1000ACEUSDT 等回 400 Invalid symbol
    #   1000PEPEUSDT 回 200；PEPEUSDT 回 400
    expected_picked = {
        "picked:ACE/USDT:USDT:binance": "ACE",
        "picked:ETH/USDT:USDT:binance": "ETH",
        "picked:BTC/USDT:USDT:binance": "BTC",
        "picked:LINK/USDT:USDT:binance": "LINK",
        "picked:1000PEPE/USDT:USDT:binance": "1000PEPE",
    }
    for key, want in expected_picked.items():
        got = baseline.get(key)
        if got != want:
            failures.append(f"[挑錯] {key}\n    期望 {want}，實得 {got}\n"
                            f"    完整候選 {baseline.get(key.replace('picked:', 'symvar:'))}\n")

    # 標準代號收斂：語意維持「取最短變體」不變（這是既有設計），
    # 本測試只釘住【結果必須確定】，外加真正該收斂的案例不可退化。
    # 註：XAG→AG、XPB→PB 是既有設計的已知缺陷（AG/XA 實測都不存在於 CrossEx），
    #     這裡只保證它每次都得到同一個答案，不在本次修正範圍 —— 見 _canonical_ticker 的說明。
    expected_canon = {
        "canon:MUB": "MU", "canon:XMU": "MU", "canon:AAPLX": "AAPL",
        "canon:NVDAX": "NVDA", "canon:ONSTOCK": "ON",
        "canon:QNTB": "QNT", "canon:QNTX": "QNT",     # Quantinuum 兩腿要落在同一個鍵
    }
    for key, want in expected_canon.items():
        got = baseline.get(key)
        if got != want:
            failures.append(f"[標準代號錯] {key}\n    期望 {want}，實得 {got}\n"
                            f"    完整候選 {baseline.get(key.replace('canon:', 'variants:'))}\n")

    if failures:
        print(f"❌ 失敗 {len(failures)} 項\n")
        for f in failures:
            print(f)
        return 1

    print(f"✅ 全部通過：{len(baseline)} 個探針在 {len(SEEDS)} 個 seed 下結果完全一致，且語意正確")
    return 0


if __name__ == "__main__":
    sys.exit(main())

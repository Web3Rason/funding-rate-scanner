"""即時監測 LAB 在各交易所的指數成分來源，一有變動就發 TG。

獨立程式，不動主後端。直接重用 funding_scanner 的抓取邏輯（含 Gate 等權重修正），
避免邏輯漂移。每 INTERVAL 秒比對一次，偵測：成分交易所新增/移除、權重顯著變動(>5%)。

用法：python monitor_lab_index.py
"""
import asyncio
import json
import subprocess
import sys
import time
from pathlib import Path

import aiohttp

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "backend"))
from services.funding_scanner import FundingScanner  # noqa: E402

SYMBOL = "LAB/USDT"
COIN = "LAB"
# 只通知與此交易所有關的變動：它自家指數的成分變動，或其他指數對它(作為成分)的增減/權重變動。
# 同時是來源 id 與成分正規化名（皆為 "bitget"）。
TARGET = "bitget"
SOURCES = ("binance", "bybit", "okx", "kucoinfutures", "gateio", "bitget", "mexc")
# 特定來源的權重變動門檻：Bybit 指數更新太頻繁，其權重變動 <5% 不通知（新增/移除仍通知）
SOURCE_WEIGHT_THRESHOLD = {"bybit": 0.05}
INTERVAL = 5                  # 輪詢秒數
KUCOIN_MAP_TTL = 300          # KuCoin 合約清單(payload 大)快取秒數，不每輪重抓
WEIGHT_ROUND = 6              # 權重比對精度（四捨五入到小數 6 位，僅濾浮點雜訊；任何實際變動都報）
STATE_FILE = HERE / "logs" / "lab_index_monitor_state.json"
NOTIFY = HERE.parent / "tools" / "notify.py"

# 來源交易所代號 → 顯示名
SRC_LABEL = {
    "binance": "Binance", "bybit": "Bybit", "okx": "OKX",
    "kucoinfutures": "KuCoin", "gateio": "Gate", "bitget": "Bitget", "mexc": "MEXC",
}


def send_tg(msg: str):
    try:
        subprocess.run([sys.executable, str(NOTIFY), msg], cwd=str(HERE.parent),
                       timeout=30, capture_output=True,
                       creationflags=(0x08000000 if sys.platform == "win32" else 0))  # CREATE_NO_WINDOW：不彈黑窗
        print(f"[TG] {msg}", flush=True)
    except Exception as e:
        print(f"[TG 失敗] {e}", flush=True)


def load_state() -> dict:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state: dict):
    try:
        STATE_FILE.parent.mkdir(exist_ok=True)
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
    except Exception as e:
        print(f"[存檔失敗] {e}", flush=True)


async def fetch_current(scanner: FundingScanner, session: aiohttp.ClientSession, kucoin_map: dict) -> dict:
    """回傳 {source: {constituent_exchange: weight}}，抓不到的來源不放進結果（不誤判為移除）。
    7 個來源併發抓取，整輪耗時≈最慢一支，不會累加。
    """
    async def _one(src):
        try:
            cons = await scanner._fetch_one_constituent(session, src, SYMBOL, kucoin_index_map=kucoin_map)
        except Exception:
            cons = None
        return src, cons

    pairs = await asyncio.gather(*[_one(s) for s in SOURCES])
    result = {}
    for src, cons in pairs:
        if not cons:
            continue
        result[src] = {c["exchange"]: c.get("weight") for c in cons}
    return result


def fmt_w(w):
    return "-" if w is None else f"{w * 100:.2f}%"


def diff_source(src: str, old: dict, new: dict, weight_threshold: float = 0.0) -> list[tuple[str, str]]:
    """比對單一來源的成分變化，回傳 (成分交易所, 變動描述) 列表。
    weight_threshold > 0 時，權重變動幅度 < 門檻的不列入（新增/移除不受門檻限制）。
    """
    items = []
    old_ex, new_ex = set(old), set(new)
    for ex in sorted(new_ex - old_ex):
        items.append((ex, f"  ＋新增 {ex}（{fmt_w(new[ex])}）"))
    for ex in sorted(old_ex - new_ex):
        items.append((ex, f"  －移除 {ex}（原 {fmt_w(old[ex])}）"))
    for ex in sorted(old_ex & new_ex):
        ow, nw = old[ex], new[ex]
        if ow is None or nw is None:
            continue
        delta = abs(nw - ow)
        if weight_threshold > 0:
            if delta < weight_threshold:
                continue
        elif round(nw, WEIGHT_ROUND) == round(ow, WEIGHT_ROUND):
            continue
        items.append((ex, f"  ～{ex} 權重 {fmt_w(ow)} → {fmt_w(nw)}"))
    return items


async def main():
    scanner = FundingScanner()
    prev = load_state()
    first = not prev
    print(f"[啟動] 監測 {COIN} 指數來源，每 {INTERVAL}s 一次。已載入基準：{len(prev)} 個來源", flush=True)

    session = aiohttp.ClientSession()
    kucoin_map = {}
    kucoin_ts = 0.0
    try:
      while True:
        # KuCoin 合約清單 payload 大，快取重用，TTL 到才重抓
        if not kucoin_map or (time.time() - kucoin_ts) > KUCOIN_MAP_TTL:
            try:
                m = await scanner._fetch_kucoin_index_symbol_map(session)
                if m:
                    kucoin_map = m
                    kucoin_ts = time.time()
            except Exception:
                pass

        try:
            cur = await fetch_current(scanner, session, kucoin_map)
        except Exception as e:
            print(f"[抓取失敗] {e}", flush=True)
            await asyncio.sleep(INTERVAL)
            continue

        if first:
            # 首次只建立基準並通知一次目前狀態
            base_lines = [f"{SRC_LABEL.get(s, s)}: " + "、".join(
                f"{ex}({fmt_w(w)})" for ex, w in cur[s].items()) for s in sorted(cur)]
            send_tg(f"📡 LAB 指數來源監測啟動（每{INTERVAL}秒）\n" + "\n".join(base_lines))
            prev = cur
            save_state(prev)
            first = False
            await asyncio.sleep(INTERVAL)
            continue

        # 比對（只比對這次有抓到的來源；抓不到的沿用舊值，不誤報）
        # 只通知與 Bitget 有關的變動：
        #   - Bitget 自家指數(src==TARGET)：其成分任何變動都報
        #   - 其他交易所指數：只報「bitget 這個成分」被新增/移除/權重變動
        change_blocks = []
        for src in cur:
            thr = SOURCE_WEIGHT_THRESHOLD.get(src, 0.0)
            items = diff_source(src, prev.get(src, {}), cur[src], weight_threshold=thr)
            if src == TARGET:
                lines = [t for _, t in items]
            else:
                lines = [t for ex, t in items if ex == TARGET]
            if lines:
                change_blocks.append(f"【{SRC_LABEL.get(src, src)} 指數】\n" + "\n".join(lines))
                prev[src] = cur[src]      # 有通知才更新基準
            elif src not in prev:
                prev[src] = cur[src]      # 首次見到的來源先建基準（不通知）
            # 其餘情況（含 <門檻 的漂移）刻意不更新基準：
            # 讓比較基準停在「上次通知的值」，小幅連續漂移累積到 ≥門檻時才會觸發，不會被無視。

        if change_blocks:
            send_tg(f"⚠️ LAB 指數變動（Bitget 相關）\n" + "\n\n".join(change_blocks))
            save_state(prev)
        else:
            ts = time.strftime("%H:%M:%S")
            print(f"[{ts}] 無變動（{len(cur)} 個來源）", flush=True)

        await asyncio.sleep(INTERVAL)
    finally:
        await session.close()


if __name__ == "__main__":
    asyncio.run(main())

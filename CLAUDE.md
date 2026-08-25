# 5011 Funding Rate Scanner — 開發守則

資金費率掃描器：同時輪詢十幾間交易所 × 上百個永續合約。因為併發量極大，**連線管理是這個專案的頭號地雷**，以下為最高優先鐵則。

---

## 🔴 鐵則：所有 REST 連線一律用 `make_session()`，禁止裸開 `aiohttp.ClientSession(...)`

**新增交易所、新增功能、修 bug 時，只要要發 HTTP 請求，一律：**

```python
from exchanges._session import make_session
```

- 常駐 session：`self._session = make_session(30)`
- 一次性請求：`async with make_session(15) as s: ...`
- `make_session(timeout_total)` 帶連線上限（`limit=200`、`limit_per_host=10`）、keepalive、DNS 快取。原本寫 `ClientSession(timeout=ClientTimeout(total=X))` → `make_session(X)`；沒指定逾時 → `make_session()`（預設 30 秒）。工廠本體與詳細註解在 `backend/exchanges/_session.py`。
- **connector 為全程序共用**（`connector_owner=False`）：`limit_per_host` 若綁在各自的 connector 上，等於每個 session 各有一份額度（本專案 19 個常駐 session + 60 處 `async with`），單一 host 就沒有天花板——實測曾對單一 BingX host 累積 133 條。共用後 per-host 上限才是真正的全程序上限；代價是同一 host 的額度由所有呼叫端共享（歷史回填 batch=10 若與其他請求撞同一 host 會排隊，且 `ClientTimeout(total=)` 含等待連線池的時間）。

### 為什麼（血淋淋的實例，2026-07-18）

直接用 `aiohttp.ClientSession()` 會套 aiohttp 預設 connector：**每個 host 併發連線無上限**。掃描時十幾間交易所的請求同時發，會對單一 host 一口氣開上百條、全體累積**破千條連線** → **灌爆本機路由器/Wi-Fi 連線表 → 不只這支程式，是整台電腦的網路掉包、變慢。**

當天實測：`SYN_SENT` 衝到 **1002 條**、對自家路由器 **掉包 50~60%**、延遲 **1226ms**。改用 `make_session` 限流後：SYN 峰值降到 **45**、掉包 **0%**、延遲回到個位數 ms。

> ⚠️ 這個 bug 最初只修了 `exchanges/` 層，`services/` 層（`funding_scanner.py`、`premium_tracker.py`、`ws_manager.py`、`leverage_cache.py`）漏套，就又爆了一次。**新增任何檔案、任何層，都適用這條規則，沒有例外。**

---

## WebSocket 不套 make_session

WS 一律走 `websockets.connect(...)`（獨立 websockets 函式庫），**不要**把它接到 `make_session`：連線上限（`limit_per_host=10`）會把多條 WS 卡住、總逾時會把長連線掐死。**WS 用 websockets、REST 用 make_session，兩者分清楚。**

（型別註記 `session: aiohttp.ClientSession` 可以保留，那只是型別，不是建立連線。）

---

## ✅ 改完的自我檢查（每次新增交易所/功能後必做）

1. **靜態檢查**：在 `backend/` 搜尋 `aiohttp.ClientSession(`，結果**只准**出現在「型別註記」和 `exchanges/_session.py` 本身。任何「建立 session」的裸用法都是違規，改成 `make_session`。
2. **動態檢查**（可選但強烈建議）：程式跑起來後，掃描高峰時用 PowerShell 看連線風暴——
   ```powershell
   (Get-NetTCPConnection -State SynSent).Count
   ```
   正常應在數十條內；若飆到上百上千，代表又有地方漏了限流。

---

## 🔴 鐵則：常駐程式的臨時資源一定要關 —— 自建 ccxt 實例用 `try/finally + ccxt_to_close` 模式

5011 是**常駐程式（永不關閉）**。任何「每輪掃描臨時建、用完要丟」的資源（ccxt 實例、aiohttp session），**只要有一條路徑漏關，就會隨執行時間無限累積 → RAM 一路爬到當機**。不能靠重啟遮蓋，**設計上就要讓記憶體有界**。

臨時建 ccxt 實例查資料時，一律用這個模式（血淋淋實例 2026-07-20：23h 內 RAM 800MB→2.7GB）：

```python
ccxt_to_close = None            # 「一定要關的自建實例」與「能不能用」分開追蹤
if 需要自建:
    ex = ccxt_cls({...})
    ccxt_to_close = ex          # 建立當下就登記 → 之後不管成敗都會關
try:
    await ex.load_markets()     # ← 失敗很常見（timeout / 限流 / 事件迴圈壅塞）
    ...
finally:
    if ccxt_to_close is not None:
        await asyncio.shield(ccxt_to_close.close())   # shield：被取消也跑完釋放 connector
```

**反面教材（就是這次的洩漏源）：** `load_markets()` 失敗就 `ex = None`，然後 `finally: if ex is not None: await ex.close()` → 條件永遠不成立 → session 被 GC 遺棄 → log 狂噴 `Unclosed client session`（當天累積 2600+ 筆）。**「能不能用」和「要不要關」是兩件事，用兩個變數，別共用一個。**

**另一個地雷：每輪要臨時建實例的「批次重查」一定要限併發 + 設硬上限。** 資料短暫失真時，候選數會從 20~30 暴增到上千，無上限的 `asyncio.gather` 會一次現建上千個 ccxt 實例 → 記憶體暴衝 + 掃描卡死。用 `asyncio.Semaphore(N)` 限併發 + `cap` 硬上限（只查最重要的前 N 個、其餘保留不誤殺），把峰值實例數釘死在十幾個。範例：`funding_scanner.py` 的 `_filter_structural_spread`。

**自我檢查（每次新增交易所/功能後）：** 程式跑起來看 `logs/scanner.log`，`grep "Unclosed client session"` 重啟後應**持續為 0**；掃描間 baseline RAM 應平坦不墊高（真洩漏才會單調上升）。

---

## 已知殘留 / 未來可選優化

修正後仍有一個**小殘留**：掃描高峰時 `Established` 連線數可能到 400+，偶發一次幾百 ms 的延遲凸起（**無掉包、下一輪即恢復**）。若要連這個也磨平，需要**收斂「單輪掃描同時開的 session 總數」**（例如共用連線池、或加一個全掃描層級的總併發上限），而不只是靠每個 session 的 per-host 上限。屬進一步優化，非必要。

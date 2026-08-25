# 任務進度
## 2026-06-15
- [x] 修 bybit 歷史資費顯示錯誤（H 等幣明顯不符官方）
  - 根因：歷史矩陣用 ccxt `fetch_funding_rate_history` 抓官方結算費率去覆蓋快取的預測費率，但 bybit `load_markets()` 預設連 option 市場（`instruments-info?category=option`，payload 巨大）一起載入，該端點在本機常逾時 → load_markets 拋錯 → 歷史 fetch 回空 → 預測值一直沒被覆蓋。H 這種費率常打到 ±cap 的幣，預測 vs 結算差很多 → 特別明顯；binance 因預測≈結算所以看起來正常
  - 修法：新增 `scope_bybit_markets(ex, exchange_id)`（`exchanges/ccxt_exchange.py`），bybit 實例在 load_markets 前把 `options['fetchMarkets']['types']` 限縮為 `['spot','linear','inverse']`（排除 option，只改 types 子鍵不整塊覆寫）
  - 套用 7 處 bybit ccxt 實例：`ccxt_exchange.py` 主合約實例；`funding_scanner.py` 現貨初始化/重試/期現；`leverage_cache.py` 槓桿批次/逐幣；`ws_manager.py` ccxt.pro spot
  - 效果：load_markets 6.8s→1.0s、市場類型不再含 option；實測 H 歷史回傳結算值 -1.0217%（=官方），預測值可正確被覆蓋；順帶消除「現貨搬磚/期現/槓桿」的 option 逾時噪音
  - Gemini review：APPROVE（已採納其「只改 types 子鍵、不整塊覆寫」建議）；Fable-5 模型暫無法存取，略過
  - **需重啟 5011 後端才生效（執行中程序仍是舊碼，由使用者決定何時重啟）**

## 2026-04-21
- [x] 價差折線圖 Y 軸改用實際 min/max + 5% padding（取代對稱 maxAbs），充分利用畫面高度
- [x] 價差/資費差折線圖 X 軸時間刻度改用絕對定位對齊資料點，帶 4px 刻度線（`SymbolDetail.jsx`）
- [x] 無跨 0 時隱藏零線，避免視覺干擾
- [x] 套利建議移除「預計利潤 > 0.3%」篩選：所有交易所配對皆顯示（`SymbolDetail.jsx`）
- [x] 多方/空方下拉排序穩定化：用 `[...EXCHANGE_ORDER, ...SPOT_ORDER].filter()` 取代 Set+indexOf
- [x] 表頭三段式排序：desc → asc → off；改 `arbSort = {key, dir}` 狀態；各欄有預設方向（費率差/利潤降序、價差升序）
- [x] 移除套利跨週期篩選：Gate (1H) 可配 Binance (8H)（RDNT 修復），保留 `normH = max(...)` 正規化
- [x] 修 SILVER 等統整幣連線 5005 失敗：`ws_manager.py` `_upd` 同時輸出 `symbol`（交易所原生）與 `normalized_symbol`（統整），15 個 WS handler 全數套用；`_build_snapshot` scan cache 同步
- [x] 碎肉流預設「自選」模式 + 即時費率差由大到小（`CoinwMeat.jsx`）
- [x] 價差公式方向翻正：後端 fetch 交換 `exchange_a` / `exchange_b`，label 改為 `(做多賣一 / 做空買一 - 1) × 100%`，負值 = 進場有利（綠），正值 = 成本（紅）；tooltip 圓點、文字色、fill gradient 同步翻轉
- [x] 已 `npm run build`；**需重啟 5011 後端才生效（WS 部分）**

## 2026-04-14
- [x] 幣種詳情「當前資金費率」表格支援多欄位排序（`SymbolDetail.jsx`）
  - 7 個欄位皆可點擊排序：交易所、資金費率、下次收費、週期、買一價、賣一價、倉位限制
  - 點擊欄頭：第一次降序 → 第二次升序 → 第三次移除
  - 可同時選擇多個欄位排序，依點擊順序為主排序→次排序→...
  - 多欄位時顯示排序序號（1,2,3...）+ 方向箭頭（▾/▴）
  - 已排序欄位高亮為黃色
  - null 值自動排最後
  - 已 `npm run build`；**需重啟 5011 後端才生效**
- [x] 幣種詳情「套利建議」新增篩選條件（`SymbolDetail.jsx`）
  - 多方交易所下拉選單：限定做多方為特定交易所
  - 空方交易所下拉選單：限定做空方為特定交易所
  - 費率差≥：只顯示費率差大於指定百分比的配對
  - 價差≥：只顯示價差大於指定百分比的配對
  - 清除按鈕一鍵重置
  - 篩選計數顯示（如 3/12）
  - 修正 premiumIdx/connectStatus 改用穩定 key 避免篩選後 index 錯位
  - 已 `npm run build`；**需重啟 5011 後端才生效**

## 2026-04-13
- [x] 修復 Ourbit 費率不顯示：`SymbolDetail.jsx` 的 `EXCHANGE_ORDER` 遺漏 ourbit
- [x] 修復 Ourbit funding_time 非整點：ticker timestamp 是行情時間，改用 interval_h 計算下次結算整點
- [x] 新增大宗商品跨交易所別名：PALLADIUM→XPD, PLATINUM→XPT, ALUMINUM/ALUMINIUM→XAL
- [x] 新增 WebSocket 即時串流功能
  - 後端：`ws_manager.py`，11 個交易所 WS handler（Binance/Bybit/OKX/Bitget/MEXC/Gate.io/CoinW/Ourbit/BingX/KuCoin/Aster）
  - 後端：`main.py` 新增 `/ws/symbol` WebSocket endpoint
  - 前端：`SymbolDetail.jsx` 連接 WS，即時更新費率/買一/賣一，顯示 LIVE 標記
  - 節流：每交易所最多每 500ms 推送一次
  - 架構：_SymbolRoom 管理每幣種連線，ref-counting 自動斷線
  - 無 WS 的交易所（Hyperliquid/Trade.xyz）使用掃描快取
  - BingX：`wss://open-api-swap.bingx.com/swap-market`，gzip 解壓，markPrice + bookTicker
  - KuCoin：先 POST bullet-public 取 token，tickerV2 (bid/ask) + instrument (mark/index/funding)
  - Aster：`wss://fstream.asterdex.com/ws`，Binance 相容格式
  - 已 `npm run build`；**需重啟 5011 後端才生效**

## 2026-04-11
- [x] 碎肉流新增「CW張數上限」欄位：顯示 CoinW 檔位1 最大持倉張數 + 約略 USDT
  - 前一版錯誤：用 `/v1/perpum/instruments` 的 `maxPosition`，結果對 ORDER 顯示 33,333,333（錯），正解是 80,000
  - 正確端點：`GET https://futuresapi.coinw.com/v1/futuresc/thirdClient/trade/getLadderConfig?quote=-1`（不需 auth，帶 UA / Referer 即可），來自 5005 既有實作
  - 回傳結構：`data.ladderConfig[].ladderList[]`，取 `ladder=1` 的 `maxPiece` 就是使用者要的值（ladder 1 = 最高槓桿、最小倉位上限）
  - 後端：`leverage_cache.py` 新增 `_fetch_coinw_ladder_map()`，`_fetch_coinw` / `_fetch_coinw_on_demand` / `_bootstrap_coinw_raw` 全部改用 ladder 1 maxPiece
  - `backend/data/leverage_tiers.json` 已手動清除舊版 `_coinw_raw` 和 `coinw` USDT 快取，重啟後會重抓
  - 前端：`CoinwMeat.jsx` 新增欄位顯示「XXXXX 張 ≈ YYYYYU」（青色）
  - 已 `npm run build`；**需重啟 5011 後端才生效**
- [x] 新增 Ourbit 交易所（MEXC 白標，598 個永續合約）
  - 新建 `exchanges/ourbit_exchange.py`：純 REST API 無需認證
  - 端點：`/api/v1/contract/detail`（合約清單）+ `/api/v1/contract/ticker`（行情+費率）
  - 結算週期：用 `conceptPlate` 推斷（stock→24h），未知的批次查 `/api/v1/contract/funding_rate/{sym}`
  - 快取：`data/ourbit_intervals.json`，TTL 4 小時
  - 前端：6 個元件新增 Ourbit label + 自選交易所下拉
  - 已 `npm run build`；**需重啟 5011 後端才生效**
- [x] 股票/商品/指數代幣跨交易所整合（_normalize_symbol 層級，影響所有功能）
  - MEXC `XXXSTOCK` 後綴：`_normalize_symbol` 自動剝離（如 CRCLSTOCK → CRCL）
  - Gate.io `XXXX` 後綴：exchange_aliases 新增 13 筆（AAPLX→AAPL, TSLAX→TSLA 等）
  - BingX `NC*` 系列合約：`NCSK{ticker}2USD`（股票）、`NCCO{ticker}2USD`（商品）、`NCSI`（指數）、`NCFX`（外匯）自動剝離前綴和 2USD 後綴，支援版本號（如 NCCO1, NCSI724）
  - 衝突處理：MEXC `CATSTOCK`/`CVXSTOCK` → `CAT_EQ`/`CVX_EQ`
  - 商品別名新增：OILWTI→CL, OILBRENT→BRENT, NATURALGAS→NG
  - **需重啟 5011 後端才生效**
- [x] 碎肉流 CW張數上限旁新增「持倉上限(U)」欄位（USDT 單位），`CoinwMeat.jsx` 新欄位顯示 max_piece × lot_size × mark_price，原 CW張數上限欄位保留只顯示張數。已 `npm run build`
- [x] 碎肉流「預期收益」支援 CoinW：當某一側為 CoinW，用檔位 1 張數 × 每張 USDT × 每日資費差計算
  - 原問題：`limit = min(high_notional or 0, low_notional or 0)`，若 CoinW 側 notional 缺失會變 0 → `expected_profit=0` → 前端顯示「-」
  - 解法：`_build_opportunity` 改為，若 coinw_info 有效 → `effective_limit = max_piece × lot_size × mark_price`（不套 50k cap）
  - 否則沿用舊邏輯 `min(high, low)` 並套 50k cap
  - 0 或 None 一律回傳 `expected_profit = None`（前端會顯示 -）
  - **需重啟 5011 後端才生效**（僅後端邏輯變更，前端不需重建）
- [x] 碎肉流新增「自選」模式：選 2 個交易所，即時計算過去 24h 每日資費差
  - 後端：`funding_scanner.py` 新增 `compute_custom_pair_meat_flow(ex_a, ex_b)`，從 `_rate_history` 即時計算，沿用 16h 覆蓋與 spike 過濾規則（CoinW 放寬 16h 限制）
  - 後端：`main.py` `/api/coinw-meat` 新增 `mode=custom` 參數，接收 `ex_a` / `ex_b`
  - 前端：`CoinwMeat.jsx` 新增「自選」按鈕 + 2 個下拉選單，動態組 URL
  - 已 `npm run build`；**需重啟 5011 後端才會生效（使用者自行決定何時重啟）**

## 2026-04-09
- [x] 優化 5011 指數成分異動通知頻率
  - [x] 將指數追蹤基準由 Binance 改為「市場中位數」(Market Median)
  - [x] 調高成分異動通知閾值：dev/spike >= 0.5%
  - [x] 調高指數污染偵測閾值：spike >= 0.8%, dev >= 1.0%
  - [x] 優化通知內容，顯示相對於中位數的偏離，Binance 異動現在包含數值
  - [x] 過濾 0.01%~0.04% 的微小變動噪音
- [ ] 待辦：觀察實測結果是否符合用戶預期

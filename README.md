# 5011 Funding Rate Scanner

多交易所永續合約資金費率即時掃描 + 跨所套利偵測工具。

## 支援交易所

| 交易所 | 接入方式 |
|--------|---------|
| Binance | ccxt |
| Bybit | ccxt |
| OKX | ccxt |
| Bitget | ccxt |
| MEXC | ccxt |
| Gate.io | ccxt |
| BingX | ccxt |
| KuCoin | ccxt |
| Aster | ccxt + 備援 REST |
| CoinW | 直接 REST API |
| Hyperliquid | 直接 REST API |
| Trade.xyz | 直接 REST API（Hyperliquid HIP-3 perpDEX） |
| Ourbit | 直接 REST API（MEXC 白標，598 個合約） |
| BitMart | 直接 REST API（V2 Futures，詳情用 WS 推送 bid/ask/mark/index） |
| DeepCoin | 直接 REST API（USDT 永續，無 markPrice 用 last 替代） |
| LBank | 直接 REST API（USDT 永續；批次行情無 bid/ask，按需用訂單簿 marketOrder 補單合約盤口） |
| Kraken | 直接 REST API（Kraken Futures PF_ 永續，資費是絕對值，需除以 indexPrice 換成相對率） |
| Deribit | 直接 REST API（inverse + USDC 線性永續，統一用 funding_8h 口徑） |
| Lighter | 官方 WS `market_stats/all`（一條訂閱拿全部 227 個市場的資費與買賣一價；REST 沒有批量 bid/ask 端點）。每小時結算，`current_funding_rate` 是每小時百分比 → ÷100 換成小數 |
| Lighter RH | Lighter on Robinhood Chain，與主站是**完全獨立的部署**（`api.rh.lighter.xyz`，另一條鏈、另一組帳戶、market_id 獨立編號）。40 個市場、幾乎全是股票 perp（SPY/QQQ/NVDA/COIN/ANTHROPIC…），maker/taker 手續費皆 0。API 規格與主站相同，共用 `lighter_exchange.py` 一份實作 |

> ⚠ Lighter 兩間所的 `/candlesticks` 都被 CloudFront 擋（403），ccxt 4.5.32 也沒有這間所，
> 故**價差圖與溢價圖對 Lighter / Lighter RH 沒有資料**；資費掃描與即時報價不受影響。
> 兩站同名的幣（SOL、ZEC、HYPE…）是不同場子的報價，實測價格與資費確實不同，不是重複資料。

結算頻率依各交易所及合約而異（1h / 4h / 8h / 24h），系統會自動偵測並顯示。

## 功能

### 費率總覽
- 矩陣視圖：橫軸交易所、縱軸幣種，一目了然
- 正負費率顏色編碼（綠=正、紅=負）
- 條件篩選：指定交易所 + 費率範圍 + 快速預設按鈕
- 自選清單（★ 星號收藏，localStorage 持久化）
- 點擊幣種開啟詳細面板：
  - 各交易所即時費率（含 bid/ask 價格、下次收費時間、結算週期）
  - 歷史費率矩陣（3 天內，支援框選統計、欄位拖曳排序、高度調整）
  - 套利建議（所有交易所配對的費率差 + 價差 + 預計利潤）
  - 價差走勢圖（5 分 K 線溢價歷史，含 hover 十字線）

### 套利偵測
- 自動找出跨所費率差套利機會
- 計算跨所 bid/ask 進場成本：`(做多賣一 / 做空買一 - 1) * 100%`（負值 = 進場有利）
- 預計利潤 = 費率收入 + 進場成本（考慮價差正負）
- 跨週期配對用 `normH = max(aInterval, bInterval)` 正規化（Gate 1H 可配 Binance 8H）
- 表頭三段式排序：desc → asc → off，各欄有預設方向
- 多空交易所下拉選單（排序穩定，不隨資料變動）
- 隱藏交易所篩選、費率範圍篩選

### Bybit 期現套利
- 自動比對 Bybit 合約與現貨價格
- 正費率 → 買現貨空合約（收資費）；負費率 → 融券空現貨多合約
- 顯示進場價差、每期收益、年化收益
- 查詢帳號可借額度 + 借貸日息（需設定 Bybit API Key）
- 依資費率閾值篩選

### 現貨搬磚
- 跨交易所現貨價差套利偵測（每 60 秒掃描）
- 支援 7 個現貨交易所 + Binance Alpha
- 5 檔加權均價計算精確價差
- 自動偵測共同充提鏈（合約地址精準匹配）
- 展開查看各交易所價格明細 + 充值/提現鏈狀態
- 鏈名正規化（統一 BEP20→BSC、ERC20→ETH 等不同命名）

### 指數監控
- 以 Binance 指數價格為基準，偵測各交易所的指數偏離
- 突發偏離模式：歷史中位數基線 vs 當前偏離，識別異常
- **指數成分追蹤**：每次掃描從 Binance/Bybit/OKX/KuCoin/Gate.io/Bitget/MEXC 抓取指數成分
  - 快照比對：偵測成分交易所新增/移除、權重變化
  - 成分變更警示橫幅 + 行內標記
  - 成分對比面板：各交易所的成分交易所、權重、價格一覽
  - 變更歷史時間線（保留 7 天）
- **成分價格污染偵測**：當某交易所指數偏離 BN >= 0.5% 時，自動檢查其成分是否有單一來源報價異常
  - 中位數基準：成分交易所價格偏離其餘中位數 >= 1% 視為污染
  - 獨立通知路徑：不依賴成分結構變更（added/removed），污染出現即可觸發
  - TG 通知包含：偏離幅度、污染來源交易所、其報價、中位數、偏差百分比、權重
  - 1 小時冷卻防重複通知

### 溢價指數（Bybit 全市場）
- 追蹤 Bybit 全部 USDT 永續（~585 個）的即時溢價指數
- 兩層數據：
  - **即時層**：WS 訂閱全部 `tickers.{symbol}`（單連線、delta 合併、斷線自動重連），即時溢價 = (mark − index) / index
  - **焦點層**：|溢價| Top 20 ∪ 資費使用率 ≥ 90%（貼上下限）的合約，每 5 秒抓官方溢價指數 1m K 線（`premium-index-price-kline`），含 60 分鐘 sparkline
- 資費上下限從 `instruments-info` 動態取得（`upperFundingRate`/`lowerFundingRate`，各合約不同、可不對稱）
- 「距頂/地板」使用率進度條：資費貼死上下限（≥95% 紅色）= H 幣事件型訊號；溢價指數回升 = 資費即將脫離地板的領先指標（資費 = 溢價指數 TWAP 夾上下限）
- 合約清單每 30 分鐘重整（新上市/下市自動增減）
- 背景服務：`backend/services/premium_tracker.py`（獨立於掃描器）

### TG 通知類型
| 通知 | 觸發條件 |
|------|---------|
| 滿資費 | BN 費率接近上限（cap 95%）且距結算 < 1 小時 |
| 成分結構異動 | 指數成分 added/removed + 對應交易所偏離 >= 0.5% |
| 成分價格污染 | 指數偏離 >= 0.5% + 單一來源報價偏離中位數 >= 1% |

### 滿資費通知
- 每次掃描後檢查 BN 費率是否接近上限（cap 的 95%）且距結算 < 1 小時
- 自動配對 Aster，TG 通知策略機會（Long BN + Short Aster）
- 同幣種同一輪結算只通知一次

### 碎肉流（Meat Flow）
- 多交易所 24h 累計資費差套利機會掃描
- 四種模式：全配對、含 CoinW、同週期、自選（任意兩交易所）
- 自動找出資費差最大的交易所配對
- 顯示價差（進場成本）、預計利潤、預期收益（日）
- CoinW 檔位 1 張數上限 + 持倉上限（USDT）
- 預期收益計算：CoinW 側用檔位 1 張數 × 幣價 × 每日資費差
- 已排除單次異常資費（spike 過濾）

### 幣種別名
- `backend/symbol_aliases.json` 管理跨所幣種名稱對照
- 所有命名以 **Binance 為標準**
- 三層機制：
  - `aliases`：全域別名，任何交易所都適用（例：PLTRX → PLTR、OILWTI → CL）
  - `exchange_aliases`：交易所專屬別名（例：Gate.io CRCLX → CRCL、BingX CRCLX → CRCL）
  - 自動規則：MEXC `XXXSTOCK` 後綴自動剝離、BingX `NC{SK|CO|FX|SI}*2USD` 前綴自動剝離
- 股票類代幣整合：MEXC（CRCLSTOCK）、Gate.io（CRCLX）、BingX（NCSKCRCL2USD）、Ourbit（CRCL）→ 統一為 `CRCL`
- 商品 / 指數同理：BingX NCCOGOLD2USD → GOLD、NCSISP5002USD → SP500
- 衝突保護：MEXC CATSTOCK / CVXSTOCK 不與 crypto CAT / CVX 合併
- 費率總覽中別名幣種會以黃色小字標註原始名稱（BingX NC 前綴已清理顯示）
- 套利面板交易所旁標註原始幣種名

## 技術棧

- **後端**：Python + FastAPI (port 3011)
- **前端**：React 19 + Vite 7 + Tailwind CSS 4 (port 5011)
- **交易所接入**：ccxt（8 個 CEX）+ aiohttp 直接 REST（CoinW、Hyperliquid、Trade.xyz、Ourbit、BitMart、DeepCoin、LBank）
- **費率掃描**：無需 API Key（公開資料）
- **進階功能**：部分功能需 API Key（Bybit 借幣額度查詢、OKX/BingX/MEXC 充提鏈資訊）

## 快速啟動

```bash
# 一鍵啟動（正式運行模式：backend :5011 同時 serve API + 靜態前端）
start.bat
```

正式模式下前端是 build 成靜態檔（`frontend/dist`），由 backend 直接 serve，**修改 `.jsx`/`.css` 後必須重新 build** 才會生效：

```bash
cd frontend && npm run build
```

開發模式（熱重載）：

```bash
cd backend && python main.py          # 後端 :5011（API + dist）
cd frontend && npm run dev            # 另開 Vite dev server（:5173），改 jsx 立即生效
```

### 對外存取

本專案預設只綁 `127.0.0.1`，設計上是本機自用工具。

若要從外部存取，請自行架反向代理（Cloudflare Tunnel、Tailscale、nginx 皆可），
並**務必加上認證**——後端 API 本身沒有任何身分驗證，直接對外開等於把控制權交出去。

### 首次安裝

```bash
# 後端
cd backend
pip install -r requirements.txt

# 前端
cd frontend
npm install
```

## 專案結構

```
5011_Funding-Rate-Scanner/
├── backend/
│   ├── main.py                      # FastAPI 入口
│   ├── models.py                    # Pydantic 資料模型
│   ├── symbol_aliases.json          # 幣種別名對照表
│   ├── requirements.txt
│   ├── exchanges/
│   │   ├── base.py                  # 交易所抽象介面
│   │   ├── ccxt_exchange.py         # ccxt 通用實作（8 個 CEX）
│   │   ├── aster_exchange.py        # Aster DEX
│   │   ├── coinw_exchange.py        # CoinW 直接 REST
│   │   ├── hyperliquid_exchange.py  # Hyperliquid 直接 REST
│   │   ├── tradexyz_exchange.py    # Trade.xyz (HIP-3 perpDEX)
│   │   ├── ourbit_exchange.py      # Ourbit 直接 REST（MEXC 白標）
│   │   ├── bitmart_exchange.py     # BitMart V2 Futures 直接 REST
│   │   ├── deepcoin_exchange.py    # DeepCoin 直接 REST（USDT 永續）
│   │   └── lbank_exchange.py       # LBank 直接 REST（USDT 永續）
│   ├── data/
│   │   ├── leverage_tiers.json      # 各交易所槓桿梯度快取
│   │   ├── bingx_intervals.json     # BingX 結算週期快取
│   │   └── ourbit_intervals.json    # Ourbit 結算週期快取
│   └── services/
│       ├── funding_scanner.py       # 掃描排程器 + 碎肉流 + 現貨搬磚 + Bybit 期現
│       ├── premium_tracker.py       # Bybit 全市場溢價指數追蹤（WS + 焦點輪詢）
│       ├── leverage_cache.py        # 交易所倉位上限快取（含 CoinW 檔位）
│       ├── normalizer.py            # 費率正規化 + 年化計算
│       └── arbitrage_detector.py    # 套利偵測引擎 + 幣種正規化
├── frontend/
│   └── src/
│       ├── App.jsx                  # 主頁面 + Tab 切換
│       ├── components/
│       │   ├── FundingTable.jsx     # 費率矩陣表格（含條件篩選、自選清單）
│       │   ├── ArbitragePanel.jsx   # 套利面板（含排序、隱藏交易所）
│       │   ├── SymbolDetail.jsx     # 幣種詳細（即時費率 + 歷史矩陣 + 套利建議 + 價差圖）
│       │   ├── CoinwMeat.jsx        # 碎肉流面板（多交易所資費差套利）
│       │   ├── IndexTracking.jsx    # 指數追蹤面板
│       │   ├── PremiumIndexPanel.jsx # 溢價指數面板（Bybit 全市場）
│       │   ├── ExchangeFilter.jsx   # 交易所篩選元件
│       │   ├── BybitSpotFutures.jsx # Bybit 期現套利面板
│       │   ├── SpotArbitrage.jsx    # 現貨搬磚面板
│       │   └── StatusBar.jsx        # 狀態列
│       └── hooks/
│           └── usePolling.js        # 輪詢 Hook
├── config.json                      # 掃描設定
├── start.bat                        # 一鍵啟動
└── logs/                            # 日誌 + CoinW 歷史快取
```

## API

| 端點 | 說明 |
|------|------|
| `GET /api/status` | 掃描器狀態 |
| `GET /api/funding-rates` | 費率查詢（支援 symbol/exchange/sort 參數） |
| `GET /api/arbitrage` | 套利機會列表（支援 min_diff/symbol 篩選） |
| `GET /api/funding-history` | 幣種歷史費率（參數：symbol, limit） |
| `GET /api/funding-realtime` | 即時查詢某幣種在各交易所的最新費率 |
| `GET /api/price-premium` | 兩交易所的 5 分 K 線價格溢價歷史 |
| `GET /api/premium-index` | Bybit 全市場即時溢價指數（支援 search 參數） |
| `GET /api/bybit-spot-futures` | Bybit 期現套利機會（從掃描快取讀取） |
| `GET /api/spot-arbitrage` | 現貨搬磚機會（支援 min_spread/symbol 篩選） |
| `GET /api/index-tracking` | 指數追蹤：各交易所指數偏離（支援 min_deviation/exchange 篩選） |
| `GET /api/index-constituents` | 指數成分查詢（參數：symbol） |
| `GET /api/index-constituents/changes` | 指數成分變更歷史（參數：hours） |
| `GET /api/coinw-meat` | 碎肉流機會（參數：min_daily_diff, mode=all/coinw/same_interval/custom, ex_a, ex_b） |
| `GET /api/aliases` | 幣種別名對照（含反向對照） |
| `POST /api/scan` | 手動觸發掃描 |

## 設定

`config.json`：
```json
{
    "scan_interval": 3600,
    "min_annual_diff": 10.0,
    "exchanges": ["binance", "bybit", "okx", ...],
    "bybit_api": { "apiKey": "...", "secret": "..." },
    "okx_api": { "apiKey": "...", "secret": "...", "password": "..." },
    "bingx_api": { "apiKey": "...", "secret": "..." },
    "mexc_api": { "apiKey": "...", "secret": "..." }
}
```

- `scan_interval`：費率掃描間隔（秒），實際排程為每小時 :30 執行
- `min_annual_diff`：套利偵測最低年化利差閾值
- `*_api`：各交易所 API Key（選填，用於 Bybit 借幣額度查詢、充提鏈資訊取得等進階功能）

`backend/symbol_aliases.json`：
```json
{
    "aliases": {
        "PLTRX": "PLTR",
        "OILWTI": "CL", "OILBRENT": "BRENT", "NATURALGAS": "NG"
    },
    "exchange_aliases": {
        "gateio": { "CRCLX": "CRCL", "TSLAX": "TSLA", "NVDAX": "NVDA", "..." : "..." },
        "mexc": { "CATSTOCK": "CAT_EQ", "CVXSTOCK": "CVX_EQ" },
        "bingx": { "CRCLX": "CRCL", "AAPLX": "AAPL", "..." : "..." }
    }
}
```
此外 `_normalize_symbol` 自動處理：MEXC `XXXSTOCK` 後綴剝離、BingX `NC*2USD` 前綴剝離。

### 判斷幣種別名的 SOP

當發現套利面板出現不合理的高溢價（例如 +3000%），通常是不同交易所用不同名稱指向同一個代幣，或同名卻是不同代幣。判斷流程：

1. **從掃描資料找可疑代幣**：在 `last_scan.json` 中搜尋相關名稱（例如搜 `FUN` 會找到 FUN、SPORTFUN、FUNTOKEN）
2. **比對 mark price**：把所有交易所同名/近似名的代幣列出，看 mark price
3. **價格相近 = 同一代幣**：例如 Binance SPORTFUN $0.0456 ≈ Aster FUN $0.0456 → 同一個幣
4. **以 Binance 命名為標準**：確定 Binance 上該幣叫什麼，其他交易所的名稱作為別名
5. **寫入規則**：
   - 若該名稱在所有交易所都不同 → 寫入 `aliases`（全域別名）
   - 若僅特定交易所名稱不同（且其他交易所有同名但不同幣） → 寫入 `exchange_aliases`（交易所專屬別名）
6. **重啟後端**即生效

範例（FUN / SPORTFUN / FUNTOKEN 判斷過程）：

```
交易所          代幣名稱         mark price     實際是（Binance標準）
─────────────────────────────────────────────────────────────────
Binance         SPORTFUN        $0.0456        SPORTFUN ✓
Binance         FUN             $0.0013        FUN ✓
Bybit           SPORTFUN        $0.0455        SPORTFUN ✓
MEXC            SPORTFUN        $0.0456        SPORTFUN ✓
MEXC            FUN             $0.0013        FUN ✓
KuCoin          SPORTFUN        $0.0456        SPORTFUN ✓
Bitget          FUN             $0.0456        SPORTFUN ← 要改
Bitget          FUNTOKEN        $0.0013        FUN ← 要改
Gate.io         FUN             $0.0456        SPORTFUN ← 要改
BingX           FUNTOKEN        $0.0013        FUN ← 要改
Aster           FUN             $0.0456        SPORTFUN ← 要改
```

結論：Bitget/Gate.io/Aster 的 FUN 實為 SPORTFUN，Bitget/BingX 的 FUNTOKEN 實為 FUN → 寫入 `exchange_aliases`。

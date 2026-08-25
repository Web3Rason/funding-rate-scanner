from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class FundingRecord(BaseModel):
    exchange: str
    symbol: str  # ccxt 標準格式：BTC/USDT:USDT
    normalized_symbol: Optional[str] = None  # 正規化後的 symbol（別名合併用）
    funding_rate: float  # 8 小時基準費率
    next_funding_rate: Optional[float] = None
    funding_time: Optional[datetime] = None
    mark_price: Optional[float] = None
    index_price: Optional[float] = None
    bid_price: Optional[float] = None   # 買一價（best bid）
    ask_price: Optional[float] = None   # 賣一價（best ask）
    annual_rate: float  # 年化百分比
    funding_interval_h: float = 8  # 結算頻率（小時）
    is_delisting: bool = False  # 即將下架標記
    delisting_time: Optional[datetime] = None  # 下架時間（自動偵測）


class ArbitrageOpportunity(BaseModel):
    symbol: str  # 正規化後的統一名稱
    long_symbol: str = ""  # 做多交易所的原始 symbol
    short_symbol: str = ""  # 做空交易所的原始 symbol
    long_exchange: str
    short_exchange: str
    long_rate: float  # 做多方原始每期費率
    short_rate: float  # 做空方原始每期費率
    long_interval_h: float = 8
    short_interval_h: float = 8
    norm_interval_h: float = 8  # 比較基準週期（取配對中較長者）
    rate_diff: float  # 正規化費率差（統一到 norm_interval_h）
    spread_pct: float = 0.0  # 跨所價差% = (做空bid / 做多ask - 1) * 100
    estimated_profit: float = 0.0  # 預計利潤% = 正規化費率差% + 價差%
    expected_profit: float = 0.0  # 預期收益(USDT) = 預計利潤% * min(倉位限制, 50000) / 100
    long_max_notional: Optional[float] = None  # 做多交易所最大名義價值(USDT)
    short_max_notional: Optional[float] = None  # 做空交易所最大名義價值(USDT)
    long_is_delisting: bool = False  # 做多交易所即將下架
    short_is_delisting: bool = False  # 做空交易所即將下架
    cross_interval: bool = False  # 跨週期配對（1h vs 8h 等）
    long_settling_soon: bool = True   # 做多方本小時會結算
    short_settling_soon: bool = True  # 做空方本小時會結算


class ScanResult(BaseModel):
    timestamp: datetime
    records: list[FundingRecord] = []
    arbitrage: list[ArbitrageOpportunity] = []
    errors: dict[str, str] = {}
    scan_duration_ms: float = 0

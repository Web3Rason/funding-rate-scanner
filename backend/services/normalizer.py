"""費率規範化：統一轉為 8 小時基準費率"""

from datetime import datetime, timezone


def guess_interval_from_next_funding(next_funding_ts_ms) -> float:
    """從 nextFundingTime（毫秒）反推 funding interval（小時）的跨交易所啟發式。

    原理：8h 結算固定在 0/8/16 點，4h 在 0/4/8/12/16/20 點，2h 在偶數點。
    若 nextFundingTime 不落在這些點上，代表 interval 較短。
    只能推出「至多」的 interval（保守），呼叫端應僅在推算值比現值更短時才覆寫，
    用來校正快取過時（例：高波動幣臨時 4h→1h，週期快取尚未刷新但收費時間已反映新節奏）。
    """
    if not next_funding_ts_ms:
        return 8.0
    try:
        dt = datetime.fromtimestamp(int(next_funding_ts_ms) / 1000, tz=timezone.utc)
        if dt.minute != 0 or dt.second != 0:
            return 1.0
        hour = dt.hour
        if hour % 2 != 0:
            return 1.0
        if hour % 4 != 0:
            return 2.0
        if hour % 8 != 0:
            return 4.0
        return 8.0
    except (ValueError, TypeError, OSError):
        return 8.0


def normalize_rate_to_8h(
    rate: float,
    exchange_id: str,
    funding_timestamp_ms: int | None = None,
) -> float:
    """
    將各交易所的 funding rate 統一轉為 8 小時基準。
    大多數交易所已經是 8h 基準，直接回傳。
    """
    # 目前主流交易所（Binance, Bybit, OKX, Bitget, MEXC, Gate, BingX, KuCoin, Aster）
    # 都是 8 小時結算一次，費率已是 8h 基準
    return rate


def calc_annual_rate(rate: float, interval_h: float = 8) -> float:
    """每次結算費率 → 年化百分比（根據實際結算頻率計算）"""
    settlements_per_day = 24 / interval_h
    return rate * settlements_per_day * 365 * 100

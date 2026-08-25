from abc import ABC, abstractmethod
from models import FundingRecord


class BaseExchange(ABC):
    """交易所抽象介面"""

    name: str

    @abstractmethod
    async def fetch_funding_rates(self) -> list[FundingRecord]:
        """取得所有永續合約的 funding rate"""
        ...

    async def fetch_funding_history(self, symbol: str, since: int = 0, limit: int = 100) -> list[dict]:
        """取得某幣種的歷史費率，子類別可覆寫"""
        return []

    @abstractmethod
    async def close(self):
        """關閉連線"""
        ...

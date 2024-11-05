from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional
from ..data_ingestion.models import Side, MarketUpdate

@dataclass
class PriceLevel:
    price: Decimal
    size: Decimal = Decimal('0')
    order_count: int = 0
    
    def __lt__(self, other):
        return self.price < other.price

@dataclass
class OrderBookLevel:
    price: Decimal
    orders: Dict[str, Decimal] = field(default_factory=dict)  # order_id -> size
    
    @property
    def total_size(self) -> Decimal:
        return sum(self.orders.values(), Decimal('0'))
    
    @property
    def order_count(self) -> int:
        return len(self.orders)

@dataclass
class OrderBookSnapshot:
    symbol: str
    timestamp: int
    bids: List[PriceLevel]
    asks: List[PriceLevel]
    sequence_number: int
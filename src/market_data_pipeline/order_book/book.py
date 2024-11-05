from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from sortedcontainers import SortedDict  # type: ignore
from ..utils.logging import get_logger
from .models import OrderBookLevel, OrderBookSnapshot, PriceLevel
from ..data_ingestion.models import MarketUpdate, Side, UpdateType

logger = get_logger(__name__)

class OrderBook:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids = SortedDict()  # price -> OrderBookLevel
        self.asks = SortedDict()  # price -> OrderBookLevel
        self.last_update_time = 0
        self.sequence_number = 0
        logger.info(f"Initialized order book for {symbol}")

    def process_update(self, update: MarketUpdate) -> bool:
        """Process a market update and update the order book accordingly."""
        try:
            if update.symbol != self.symbol:
                logger.warning(f"Received update for wrong symbol: {update.symbol}")
                return False

            if update.sequence_number <= self.sequence_number:
                logger.warning(f"Received out-of-sequence update: {update.sequence_number}")
                return False

            price = Decimal(str(update.price))
            size = Decimal(str(update.size))
            book_side = self.bids if update.side == Side.BID else self.asks

            if update.update_type == UpdateType.DELETE:
                if price in book_side:
                    del book_side[price]
            elif update.update_type == UpdateType.MODIFY:
                if price in book_side:
                    book_side[price] = OrderBookLevel(
                        price=price,
                        orders={str(update.sequence_number): size}
                    )
            else:  # ADD
                book_side[price] = OrderBookLevel(
                    price=price,
                    orders={str(update.sequence_number): size}
                )

            self.last_update_time = update.timestamp
            self.sequence_number = update.sequence_number
            return True

        except Exception as e:
            logger.error(f"Error processing order book update: {e}")
            return False

    def get_price_levels(self, side: Side, depth: int = 10) -> List[PriceLevel]:
        """Get a list of price levels for the specified side up to the given depth."""
        book_side = self.bids if side == Side.BID else self.asks
        levels = []
        
        prices = list(book_side.keys())
        if side == Side.BID:
            prices.reverse()  # Highest to lowest for bids
            
        for price in prices[:depth]:
            level = book_side[price]
            levels.append(PriceLevel(
                price=level.price,
                size=level.total_size,
                order_count=level.order_count
            ))
            
        return levels

    def get_top_of_book(self) -> Tuple[Optional[PriceLevel], Optional[PriceLevel]]:
        """Get the best bid and ask price levels."""
        best_bid = None
        best_ask = None
        
        if self.bids:
            price = self.bids.keys()[-1]  # Highest bid
            level = self.bids[price]
            best_bid = PriceLevel(
                price=level.price,
                size=level.total_size,
                order_count=level.order_count
            )
            
        if self.asks:
            price = self.asks.keys()[0]  # Lowest ask
            level = self.asks[price]
            best_ask = PriceLevel(
                price=level.price,
                size=level.total_size,
                order_count=level.order_count
            )
            
        return best_bid, best_ask

    def get_snapshot(self) -> OrderBookSnapshot:
        """Create a snapshot of the current order book state."""
        return OrderBookSnapshot(
            symbol=self.symbol,
            timestamp=self.last_update_time,
            bids=self.get_price_levels(Side.BID),
            asks=self.get_price_levels(Side.ASK),
            sequence_number=self.sequence_number
        )

    def clear(self) -> None:
        """Clear all orders from the book."""
        self.bids.clear()
        self.asks.clear()
        logger.info(f"Cleared order book for {self.symbol}")
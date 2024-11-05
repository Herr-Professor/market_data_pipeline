from typing import Dict, Optional, Set
from .book import OrderBook
from ..data_ingestion.models import MarketUpdate
from ..utils.logging import get_logger

logger = get_logger(__name__)

class OrderBookManager:
    def __init__(self):
        self.books: Dict[str, OrderBook] = {}
        logger.info("Initialized order book manager")

    def get_or_create_book(self, symbol: str) -> OrderBook:
        """Get an existing order book or create a new one."""
        if symbol not in self.books:
            self.books[symbol] = OrderBook(symbol)
            logger.info(f"Created new order book for {symbol}")
        return self.books[symbol]

    def process_update(self, update: MarketUpdate) -> bool:
        """Process an update for the appropriate order book."""
        book = self.get_or_create_book(update.symbol)
        return book.process_update(update)

    def get_book(self, symbol: str) -> Optional[OrderBook]:
        """Get an order book for a symbol if it exists."""
        return self.books.get(symbol)

    def remove_book(self, symbol: str) -> None:
        """Remove an order book."""
        if symbol in self.books:
            del self.books[symbol]
            logger.info(f"Removed order book for {symbol}")
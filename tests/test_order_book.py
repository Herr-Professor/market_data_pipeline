# src/market_data_pipeline/order_book/tests/test_order_book.py
import pytest
from decimal import Decimal
from market_data_pipeline.order_book.book import OrderBook
from market_data_pipeline.order_book.manager import OrderBookManager
from market_data_pipeline.data_ingestion.models import MarketUpdate, Side, UpdateType

@pytest.fixture
def order_book():
    return OrderBook("AAPL")

@pytest.fixture
def book_manager():
    return OrderBookManager()

def create_update(
    symbol: str,
    price: float,
    size: float,
    side: Side,
    update_type: UpdateType,
    sequence_number: int
) -> MarketUpdate:
    return MarketUpdate(
        timestamp=1000000,
        symbol=symbol,
        price=price,
        size=size,
        side=side,
        update_type=update_type,
        sequence_number=sequence_number,
        exchange_id="TEST"
    )

class TestOrderBook:
    def test_add_order(self, order_book):
        update = create_update("AAPL", 100.0, 10.0, Side.BID, UpdateType.ADD, 1)
        assert order_book.process_update(update) is True
        
        bid_levels = order_book.get_price_levels(Side.BID)
        assert len(bid_levels) == 1
        assert bid_levels[0].price == Decimal('100.0')
        assert bid_levels[0].size == Decimal('10.0')

    def test_modify_order(self, order_book):
        # Add initial order
        add_update = create_update("AAPL", 100.0, 10.0, Side.BID, UpdateType.ADD, 1)
        order_book.process_update(add_update)
        
        # Modify order
        modify_update = create_update("AAPL", 100.0, 5.0, Side.BID, UpdateType.MODIFY, 2)
        assert order_book.process_update(modify_update) is True
        
        bid_levels = order_book.get_price_levels(Side.BID)
        assert bid_levels[0].size == Decimal('5.0')

    def test_delete_order(self, order_book):
        # Add order
        add_update = create_update("AAPL", 100.0, 10.0, Side.BID, UpdateType.ADD, 1)
        order_book.process_update(add_update)
        
        # Delete order
        delete_update = create_update("AAPL", 100.0, 0.0, Side.BID, UpdateType.DELETE, 2)
        assert order_book.process_update(delete_update) is True
        
        bid_levels = order_book.get_price_levels(Side.BID)
        assert len(bid_levels) == 0

    def test_top_of_book(self, order_book):
        # Add multiple orders
        updates = [
            create_update("AAPL", 100.0, 10.0, Side.BID, UpdateType.ADD, 1),
            create_update("AAPL", 101.0, 5.0, Side.BID, UpdateType.ADD, 2),
            create_update("AAPL", 102.0, 8.0, Side.ASK, UpdateType.ADD, 3),
            create_update("AAPL", 103.0, 3.0, Side.ASK, UpdateType.ADD, 4),
        ]
        
        for update in updates:
            order_book.process_update(update)
            
        best_bid, best_ask = order_book.get_top_of_book()
        assert best_bid.price == Decimal('101.0')
        assert best_ask.price == Decimal('102.0')

    def test_sequence_number_validation(self, order_book):
        # Add order with sequence number 2
        update1 = create_update("AAPL", 100.0, 10.0, Side.BID, UpdateType.ADD, 2)
        assert order_book.process_update(update1) is True
        
        # Try to add order with lower sequence number
        update2 = create_update("AAPL", 101.0, 5.0, Side.BID, UpdateType.ADD, 1)
        assert order_book.process_update(update2) is False

class TestOrderBookManager:
    def test_create_book(self, book_manager):
        book = book_manager.get_or_create_book("AAPL")
        assert book is not None
        assert book.symbol == "AAPL"

    def test_process_update(self, book_manager):
        update = create_update("AAPL", 100.0, 10.0, Side.BID, UpdateType.ADD, 1)
        assert book_manager.process_update(update) is True
        
        book = book_manager.get_book("AAPL")
        assert book is not None
        best_bid, _ = book.get_top_of_book()
        assert best_bid.price == Decimal('100.0')

    def test_remove_book(self, book_manager):
        book_manager.get_or_create_book("AAPL")
        book_manager.remove_book("AAPL")
        assert book_manager.get_book("AAPL") is None
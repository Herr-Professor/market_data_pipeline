import pytest
import asyncio
from market_data_pipeline.data_ingestion.models import MarketUpdate, Side, UpdateType
from market_data_pipeline.data_ingestion.feed_simulator import MarketDataSimulator
from market_data_pipeline.data_ingestion.buffer import CircularBuffer
from market_data_pipeline.data_ingestion.feed_handler import FeedHandler

@pytest.mark.asyncio
async def test_market_data_simulator():
    symbols = ["AAPL", "MSFT"]
    initial_prices = {"AAPL": 150.0, "MSFT": 300.0}
    simulator = MarketDataSimulator(symbols, initial_prices)
    
    updates = []
    async for update in simulator.start():
        updates.append(update)
        if len(updates) >= 10:
            simulator.stop()
            break
    
    assert len(updates) == 10
    for update in updates:
        assert update.symbol in symbols
        assert isinstance(update.price, float)
        assert isinstance(update.size, float)
        assert isinstance(update.side, Side)
        assert isinstance(update.update_type, UpdateType)

@pytest.mark.asyncio
async def test_feed_handler():
    symbols = {"AAPL", "MSFT"}
    handler = FeedHandler(symbols)
    
    # Create sample update
    update = MarketUpdate(
        timestamp=1000000000,
        symbol="AAPL",
        price=150.0,
        size=100,
        side=Side.BID,
        update_type=UpdateType.ADD,
        sequence_number=1,
        exchange_id="TEST"
    )
    
    # Process update
    processed_update = await handler.process_update(update)
    assert processed_update is not None
    assert processed_update.symbol == "AAPL"
    
    # Test sequence gap detection
    gap_update = MarketUpdate(
        timestamp=1000000001,
        symbol="AAPL",
        price=151.0,
        size=100,
        side=Side.ASK,
        update_type=UpdateType.MODIFY,
        sequence_number=5,  # Gap in sequence
        exchange_id="TEST"
    )
    
    processed_gap_update = await handler.process_update(gap_update)
    assert processed_gap_update is not None  # Handler should still process the update
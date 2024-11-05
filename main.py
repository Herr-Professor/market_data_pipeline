# src/market_data_pipeline/main.py
import asyncio
import signal
from datetime import datetime
from typing import Dict, Set
from src.market_data_pipeline.data_ingestion.feed_simulator import MarketDataSimulator
from src.market_data_pipeline.data_ingestion.feed_handler import FeedHandler
from src.market_data_pipeline.order_book.models import OrderBookSnapshot
from src.market_data_pipeline.utils.logging import get_logger

logger = get_logger(__name__)

class MarketDataApp:
    def __init__(self):
        self.symbols = ["AAPL", "MSFT", "GOOGL"]
        self.initial_prices = {"AAPL": 150.0, "MSFT": 300.0, "GOOGL": 2500.0}
        self.running = False
        self.simulator = MarketDataSimulator(
            symbols=self.symbols,
            initial_prices=self.initial_prices,
            volatility=0.001,
            update_interval=0.1
        )
        self.handler = FeedHandler(set(self.symbols))
        self.update_count = 0

    async def print_book_snapshot(self, symbol: str) -> None:
        """Print formatted order book snapshot."""
        snapshot = self.handler.get_book_snapshot(symbol)
        if not snapshot:
            return

        print(f"\n=== Order Book Snapshot for {symbol} ===")
        print(f"Time: {datetime.fromtimestamp(snapshot.timestamp/1e9)}")
        print(f"Sequence: {snapshot.sequence_number}")
        
        print("\nBids:")
        for level in snapshot.bids[:5]:  # Show top 5 levels
            print(f"  {level.price}: {level.size} ({level.order_count} orders)")
            
        print("\nAsks:")
        for level in snapshot.asks[:5]:  # Show top 5 levels
            print(f"  {level.price}: {level.size} ({level.order_count} orders)")
        
        best_bid, best_ask = self.handler.get_top_of_book(symbol)
        if best_bid and best_ask:
            spread = (best_ask.price - best_bid.price) / best_bid.price * 100
            print(f"\nSpread: {spread:.3f}%")

    async def monitor_books(self) -> None:
        """Periodically monitor and print book states."""
        while self.running:
            for symbol in self.symbols:
                await self.print_book_snapshot(symbol)
            await asyncio.sleep(5)  # Update every 5 seconds

    async def run(self) -> None:
        """Run the market data application."""
        self.running = True
        
        # Start the book monitoring task
        monitor_task = asyncio.create_task(self.monitor_books())
        
        try:
            async for update in self.simulator.start():
                self.update_count += 1
                processed_update = await self.handler.process_update(update)
                
                if processed_update:
                    logger.debug(f"Processed update {self.update_count}: {processed_update}")
                
                # Periodic health check
                if self.update_count % 100 == 0:
                    await self.handler.check_all_books()
                    
        except KeyboardInterrupt:
            logger.info("Shutting down market data application...")
        finally:
            self.running = False
            self.simulator.stop()
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass

def main():
    app = MarketDataApp()
    asyncio.run(app.run())

if __name__ == "__main__":
    main()
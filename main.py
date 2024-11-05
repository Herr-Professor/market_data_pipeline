# src/market_data_pipeline/main.py
import asyncio
import signal
from datetime import datetime
from typing import Dict, Set
from src.market_data_pipeline.data_ingestion.feed_simulator import MarketDataSimulator
from src.market_data_pipeline.data_ingestion.feed_handler import FeedHandler
from src.market_data_pipeline.order_book.models import OrderBookSnapshot
from src.market_data_pipeline.analytics.engine import AnalyticsEngine
from src.market_data_pipeline.utils.logging import get_logger

logger = get_logger(__name__)

class MarketDataApp:
    def __init__(self):
        self.symbols = ["AAPL", "MSFT", "GOOGL"]
        self.initial_prices = {"AAPL": 150.0, "MSFT": 300.0, "GOOGL": 2500.0}
        self.running = False
        
        # Initialize components
        self.simulator = MarketDataSimulator(
            symbols=self.symbols,
            initial_prices=self.initial_prices,
            volatility=0.001,
            update_interval=0.1
        )
        self.handler = FeedHandler(set(self.symbols))
        
        # Initialize analytics engine with the book manager
        self.analytics = AnalyticsEngine(
            book_manager=self.handler.book_manager,
            window_size=100,  # Keep last 100 updates for calculations
            metrics_interval=1.0  # Calculate metrics every second
        )
        self.update_count = 0

    async def print_analytics(self, symbol: str) -> None:
        """Print analytics information for a symbol."""
        metrics = self.analytics.get_latest_metrics(symbol)
        summary = self.analytics.get_analytics_summary(symbol)
        
        if metrics and summary:
            print(f"\n=== Analytics for {symbol} ===")
            print(f"Time: {datetime.fromtimestamp(metrics.timestamp/1e9)}")
            print(f"Mid Price: {metrics.mid_price:.2f}")
            print(f"Spread (bps): {metrics.spread_bps:.2f}")
            print(f"Volume Weighted Price: {metrics.volume_weighted_price:.2f}")
            print(f"Order Imbalance: {metrics.order_imbalance:.2%}")
            print(f"Rolling Volume: {metrics.rolling_volume:.2f}")
            print(f"Volatility: {metrics.volatility:.2%}")
            print("\nMoving Averages:")
            print(f"Price MA: {summary['moving_average']:.2f}")
            print(f"Volume MA: {summary['volume_ma']:.2f}")

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

    async def monitor_markets(self) -> None:
        """Periodically monitor and print market states."""
        while self.running:
            for symbol in self.symbols:
                await self.print_book_snapshot(symbol)
                await self.print_analytics(symbol)
            await asyncio.sleep(5)  # Update every 5 seconds

    async def run(self) -> None:
        """Run the market data application."""
        self.running = True
        
        # Start the analytics engine
        analytics_task = asyncio.create_task(self.analytics.start())
        
        # Start the monitoring task
        monitor_task = asyncio.create_task(self.monitor_markets())
        
        try:
            async for update in self.simulator.start():
                self.update_count += 1
                
                # Process update through feed handler
                processed_update = await self.handler.process_update(update)
                
                # Process update through analytics engine
                if processed_update:
                    self.analytics.process_update(processed_update)
                    logger.debug(f"Processed update {self.update_count}: {processed_update}")
                
                # Periodic health check
                if self.update_count % 100 == 0:
                    await self.handler.check_all_books()
                    
        except KeyboardInterrupt:
            logger.info("Shutting down market data application...")
        finally:
            self.running = False
            self.simulator.stop()
            self.analytics.stop()
            
            # Cancel and await all tasks
            analytics_task.cancel()
            monitor_task.cancel()
            try:
                await asyncio.gather(analytics_task, monitor_task, return_exceptions=True)
            except asyncio.CancelledError:
                pass

def main():
    app = MarketDataApp()
    asyncio.run(app.run())

if __name__ == "__main__":
    main()
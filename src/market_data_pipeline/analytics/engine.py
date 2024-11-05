from decimal import Decimal
from typing import Dict, Optional, List
import asyncio
from ..order_book.manager import OrderBookManager
from ..data_ingestion.models import MarketUpdate
from .metrics import MarketAnalytics, MarketMetrics
from ..utils.logging import get_logger

logger = get_logger(__name__)

class AnalyticsEngine:
    def __init__(
        self,
        book_manager: OrderBookManager,
        window_size: int = 100,
        metrics_interval: float = 1.0
    ):
        self.book_manager = book_manager
        self.analytics = MarketAnalytics(window_size)
        self.metrics_interval = metrics_interval
        self.metrics_history: Dict[str, List[MarketMetrics]] = {}
        self.running = False
        logger.info("Initialized analytics engine")

    async def start(self) -> None:
        """Start the analytics engine."""
        self.running = True
        logger.info("Starting analytics engine")
        
        try:
            while self.running:
                await self._calculate_metrics()
                await asyncio.sleep(self.metrics_interval)
        except Exception as e:
            logger.error(f"Error in analytics engine: {e}")
            raise
        finally:
            self.running = False

    def stop(self) -> None:
        """Stop the analytics engine."""
        self.running = False
        logger.info("Stopping analytics engine")

    def process_update(self, update: MarketUpdate) -> None:
        """Process a market update for analytics."""
        try:
            self.analytics.update_time_series(update)
        except Exception as e:
            logger.error(f"Error processing update in analytics engine: {e}")

    async def _calculate_metrics(self) -> None:
        """Calculate metrics for all active symbols."""
        try:
            for symbol, book in self.book_manager.books.items():
                snapshot = book.get_snapshot()
                metrics = self.analytics.calculate_book_metrics(snapshot)
                
                if metrics:
                    # Store metrics history
                    if symbol not in self.metrics_history:
                        self.metrics_history[symbol] = []
                    self.metrics_history[symbol].append(metrics)
                    
                    # Generate and log signals
                    signals = self.analytics.generate_signals(metrics)
                    if any(signals.values()):
                        logger.info(f"Signals generated for {symbol}: {signals}")
                        
                    # Trim history if needed
                    if len(self.metrics_history[symbol]) > 1000:  # Keep last 1000 metrics
                        self.metrics_history[symbol].pop(0)

        except Exception as e:
            logger.error(f"Error calculating metrics: {e}")

    def get_latest_metrics(self, symbol: str) -> Optional[MarketMetrics]:
        """Get the most recent metrics for a symbol."""
        try:
            history = self.metrics_history.get(symbol, [])
            return history[-1] if history else None
        except Exception as e:
            logger.error(f"Error getting latest metrics: {e}")
            return None

    def get_analytics_summary(self, symbol: str) -> Dict[str, Decimal]:
        """Get analytics summary for a symbol."""
        return self.analytics.get_analytics_summary(symbol)
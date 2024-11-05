from decimal import Decimal
from typing import List, Optional, Dict, Tuple
from dataclasses import dataclass
from datetime import datetime
import numpy as np
from ..order_book.models import OrderBookSnapshot, PriceLevel
from ..data_ingestion.models import MarketUpdate, Side
from ..utils.logging import get_logger

logger = get_logger(__name__)

@dataclass
class MarketMetrics:
    symbol: str
    timestamp: int
    spread: Decimal
    spread_bps: Decimal
    mid_price: Decimal
    volume_weighted_price: Decimal
    rolling_volume: Decimal
    volatility: Decimal
    order_imbalance: Decimal

class MarketAnalytics:
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self.price_history: Dict[str, List[Decimal]] = {}
        self.volume_history: Dict[str, List[Decimal]] = {}
        self.update_history: Dict[str, List[MarketUpdate]] = {}
        self._moving_averages: Dict[str, Decimal] = {}
        self._volatilities: Dict[str, Decimal] = {}
        logger.info(f"Initialized market analytics with window size {window_size}")

    def calculate_book_metrics(self, snapshot: OrderBookSnapshot) -> Optional[MarketMetrics]:
        """Calculate metrics from order book snapshot."""
        try:
            if not snapshot.bids or not snapshot.asks:
                return None

            # Basic metrics
            best_bid = snapshot.bids[0]
            best_ask = snapshot.asks[0]
            spread = best_ask.price - best_bid.price
            mid_price = (best_ask.price + best_bid.price) / Decimal('2')
            spread_bps = (spread / mid_price) * Decimal('10000')

            # Volume calculations
            bid_volume = sum(level.size for level in snapshot.bids)
            ask_volume = sum(level.size for level in snapshot.asks)
            total_volume = bid_volume + ask_volume

            # Volume weighted price
            vwap_bids = sum(level.price * level.size for level in snapshot.bids)
            vwap_asks = sum(level.price * level.size for level in snapshot.asks)
            vwp = (vwap_bids + vwap_asks) / total_volume if total_volume > 0 else mid_price

            # Order imbalance
            imbalance = (bid_volume - ask_volume) / (bid_volume + ask_volume) if total_volume > 0 else Decimal('0')

            # Get stored volatility or calculate if not available
            volatility = self._volatilities.get(snapshot.symbol, Decimal('0'))

            return MarketMetrics(
                symbol=snapshot.symbol,
                timestamp=snapshot.timestamp,
                spread=spread,
                spread_bps=spread_bps,
                mid_price=mid_price,
                volume_weighted_price=vwp,
                rolling_volume=total_volume,
                volatility=volatility,
                order_imbalance=imbalance
            )

        except Exception as e:
            logger.error(f"Error calculating book metrics: {e}")
            return None

    def update_time_series(self, update: MarketUpdate) -> None:
        """Update time series data with new market update."""
        try:
            symbol = update.symbol
            
            # Initialize histories if needed
            if symbol not in self.price_history:
                self.price_history[symbol] = []
                self.volume_history[symbol] = []
                self.update_history[symbol] = []

            # Update histories
            self.price_history[symbol].append(Decimal(str(update.price)))
            self.volume_history[symbol].append(Decimal(str(update.size)))
            self.update_history[symbol].append(update)

            # Maintain window size
            if len(self.price_history[symbol]) > self.window_size:
                self.price_history[symbol].pop(0)
                self.volume_history[symbol].pop(0)
                self.update_history[symbol].pop(0)

            # Update derived metrics
            self._update_moving_averages(symbol)
            self._update_volatility(symbol)

        except Exception as e:
            logger.error(f"Error updating time series: {e}")

    def _update_moving_averages(self, symbol: str) -> None:
        """Update moving averages for symbol."""
        try:
            prices = self.price_history[symbol]
            if len(prices) > 0:
                self._moving_averages[symbol] = sum(prices) / Decimal(str(len(prices)))
        except Exception as e:
            logger.error(f"Error updating moving averages: {e}")

    def _update_volatility(self, symbol: str) -> None:
        """Update volatility calculation for symbol."""
        try:
            prices = self.price_history[symbol]
            if len(prices) > 1:
                # Convert to numpy array for efficient calculation
                price_array = np.array([float(p) for p in prices])
                returns = np.diff(np.log(price_array))
                volatility = np.std(returns) * np.sqrt(252)  # Annualized
                self._volatilities[symbol] = Decimal(str(volatility))
        except Exception as e:
            logger.error(f"Error updating volatility: {e}")

    def generate_signals(self, metrics: MarketMetrics) -> Dict[str, bool]:
        """Generate trading signals based on current metrics."""
        signals = {}
        
        try:
            # Volume imbalance signal
            signals['volume_imbalance'] = abs(metrics.order_imbalance) > Decimal('0.7')

            # Spread anomaly signal
            signals['wide_spread'] = metrics.spread_bps > Decimal('50')  # 50 bps threshold

            # Volatility breakout signal
            volatility_threshold = Decimal('0.02')  # 2% threshold
            signals['high_volatility'] = metrics.volatility > volatility_threshold

            # Price movement signal based on moving average
            ma = self._moving_averages.get(metrics.symbol)
            if ma:
                signals['price_trend'] = metrics.mid_price > ma

        except Exception as e:
            logger.error(f"Error generating signals: {e}")

        return signals

    def get_analytics_summary(self, symbol: str) -> Dict[str, Decimal]:
        """Get summary of current analytics for a symbol."""
        try:
            return {
                'moving_average': self._moving_averages.get(symbol, Decimal('0')),
                'volatility': self._volatilities.get(symbol, Decimal('0')),
                'volume_ma': sum(self.volume_history.get(symbol, [])) / Decimal(str(self.window_size)) \
                    if symbol in self.volume_history and self.volume_history[symbol] else Decimal('0')
            }
        except Exception as e:
            logger.error(f"Error getting analytics summary: {e}")
            return {}
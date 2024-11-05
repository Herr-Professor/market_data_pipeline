import asyncio
import logging
import random
from datetime import datetime
from typing import List, AsyncGenerator
import numpy as np
from ..utils.logging import get_logger
from .models import MarketUpdate, Side, UpdateType

logger = get_logger(__name__)

class MarketDataSimulator:
    def __init__(
        self,
        symbols: List[str],
        initial_prices: dict[str, float],
        volatility: float = 0.001,
        update_interval: float = 0.1,
    ):
        self.symbols = symbols
        self.prices = initial_prices.copy()
        self.volatility = volatility
        self.update_interval = update_interval
        self.sequence_number = 0
        self.running = False
        logger.info(f"Initialized simulator with {len(symbols)} symbols")

    def _generate_update(self, symbol: str) -> MarketUpdate:
        """Generate a realistic market update."""
        self.sequence_number += 1
        
        # Simulate price movement using geometric Brownian motion
        price_change = np.random.normal(0, self.volatility) * self.prices[symbol]
        self.prices[symbol] += price_change
        
        # Generate realistic size
        size = random.lognormvariate(4, 0.5)  # Generates realistic order sizes
        
        return MarketUpdate(
            timestamp=int(datetime.now().timestamp() * 1e9),  # nanoseconds
            symbol=symbol,
            price=self.prices[symbol],
            size=size,
            side=random.choice(list(Side)),
            update_type=random.choice(list(UpdateType)),
            sequence_number=self.sequence_number,
            exchange_id="SIM"
        )

    async def start(self) -> AsyncGenerator[MarketUpdate, None]:
        """Start generating market updates."""
        self.running = True
        logger.info("Starting market data simulation")
        
        try:
            while self.running:
                # Randomly select a symbol to update
                symbol = random.choice(self.symbols)
                update = self._generate_update(symbol)
                
                yield update
                
                await asyncio.sleep(self.update_interval)
        except Exception as e:
            logger.error(f"Error in market data simulation: {e}")
            raise
        finally:
            self.running = False
            logger.info("Market data simulation stopped")

    def stop(self):
        """Stop generating market updates."""
        self.running = False
        logger.info("Stopping market data simulation")
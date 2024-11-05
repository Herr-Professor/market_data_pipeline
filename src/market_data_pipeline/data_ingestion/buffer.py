from collections import deque
from typing import Optional, Deque
import logging
from .models import MarketUpdate
from ..utils.logging import get_logger

logger = get_logger(__name__)

class CircularBuffer:
    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self.buffer: Deque[MarketUpdate] = deque(maxlen=max_size)
        logger.info(f"Initialized circular buffer with max size {max_size}")

    def add(self, update: MarketUpdate) -> None:
        """Add a market update to the buffer."""
        try:
            self.buffer.append(update)
            if len(self.buffer) == self.max_size:
                logger.debug("Buffer reached maximum size, oldest update removed")
        except Exception as e:
            logger.error(f"Error adding update to buffer: {e}")
            raise

    def get_latest(self, n: int = 1) -> list[MarketUpdate]:
        """Get the n most recent updates."""
        try:
            return list(self.buffer)[-n:]
        except Exception as e:
            logger.error(f"Error retrieving updates from buffer: {e}")
            raise

    def clear(self) -> None:
        """Clear all updates from the buffer."""
        try:
            self.buffer.clear()
            logger.info("Buffer cleared")
        except Exception as e:
            logger.error(f"Error clearing buffer: {e}")
            raise


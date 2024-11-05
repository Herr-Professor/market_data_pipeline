import asyncio
import logging
from typing import Dict, Optional, Set, Tuple
from .models import MarketUpdate
from .buffer import CircularBuffer
from ..order_book.manager import OrderBookManager
from ..order_book.models import OrderBookSnapshot, PriceLevel
from ..utils.logging import get_logger

logger = get_logger(__name__)

class FeedHandler:
    def __init__(
        self,
        symbols: Set[str],
        buffer_size: int = 10000,
        sequence_gap_threshold: int = 10
    ):
        self.symbols = symbols
        self.buffer = CircularBuffer(buffer_size)
        self.sequence_gap_threshold = sequence_gap_threshold
        self.last_sequence_numbers: Dict[str, int] = {}
        self.book_manager = OrderBookManager()
        self.book_update_counts: Dict[str, int] = {symbol: 0 for symbol in symbols}
        logger.info(f"Initialized feed handler for symbols: {symbols}")

    async def process_update(self, update: MarketUpdate) -> Optional[MarketUpdate]:
        """Process a market update, handling sequence gaps and validation."""
        try:
            if update.symbol not in self.symbols:
                logger.warning(f"Received update for unknown symbol: {update.symbol}")
                return None

            # Check sequence number
            last_seq = self.last_sequence_numbers.get(update.symbol, 0)
            seq_gap = update.sequence_number - last_seq - 1

            if seq_gap > 0:
                if seq_gap > self.sequence_gap_threshold:
                    logger.error(
                        f"Large sequence gap detected for {update.symbol}: "
                        f"expected {last_seq + 1}, got {update.sequence_number}"
                    )
                    await self._handle_large_sequence_gap(update.symbol, last_seq + 1, update.sequence_number - 1)
                else:
                    logger.warning(
                        f"Small sequence gap detected for {update.symbol}: "
                        f"missed {seq_gap} updates"
                    )

            # Update last sequence number
            self.last_sequence_numbers[update.symbol] = update.sequence_number

            # Store update in buffer
            self.buffer.add(update)
            
            # Update order book
            if self.book_manager.process_update(update):
                self.book_update_counts[update.symbol] += 1
                await self._check_book_state(update.symbol)
            
            return update

        except Exception as e:
            logger.error(f"Error processing market update: {e}")
            raise

    async def _handle_large_sequence_gap(self, symbol: str, start_seq: int, end_seq: int) -> None:
        """Handle large sequence gaps by requesting missing updates or resetting the book."""
        try:
            # In a production system, you might want to request missing updates from the exchange
            # For now, we'll reset the book for the affected symbol
            logger.warning(f"Resetting order book for {symbol} due to large sequence gap")
            book = self.book_manager.get_book(symbol)
            if book:
                book.clear()
        except Exception as e:
            logger.error(f"Error handling sequence gap for {symbol}: {e}")

    async def _check_book_state(self, symbol: str) -> None:
        """Monitor order book state and log any anomalies."""
        try:
            book = self.book_manager.get_book(symbol)
            if not book:
                return

            best_bid, best_ask = book.get_top_of_book()
            
            # Check for crossed book
            if best_bid is not None and best_ask is not None:
                if best_bid.price >= best_ask.price:
                    logger.error(f"Crossed book detected for {symbol}: "
                               f"Bid {best_bid.price} >= Ask {best_ask.price}")

            # Log periodic book state
            if self.book_update_counts[symbol] % 1000 == 0:
                snapshot = book.get_snapshot()
                logger.info(
                    f"Order book state for {symbol} after {self.book_update_counts[symbol]} updates:\n"
                    f"Top Bid: {best_bid.price if best_bid else 'None'}@{best_bid.size if best_bid else 'None'}\n"
                    f"Top Ask: {best_ask.price if best_ask else 'None'}@{best_ask.size if best_ask else 'None'}\n"
                    f"Bid Levels: {len(snapshot.bids)}, Ask Levels: {len(snapshot.asks)}"
                )

        except Exception as e:
            logger.error(f"Error checking book state for {symbol}: {e}")

    def get_buffer_snapshot(self) -> list[MarketUpdate]:
        """Get current buffer contents."""
        return self.buffer.get_latest(self.buffer.max_size)

    def get_book_snapshot(self, symbol: str) -> Optional[OrderBookSnapshot]:
        """Get current state of the order book for a symbol."""
        book = self.book_manager.get_book(symbol)
        return book.get_snapshot() if book else None

    def get_top_of_book(self, symbol: str) -> Tuple[Optional[PriceLevel], Optional[PriceLevel]]:
        """Get the best bid and ask for a symbol."""
        book = self.book_manager.get_book(symbol)
        return book.get_top_of_book() if book else (None, None)

    async def check_all_books(self) -> None:
        """Periodic health check of all order books."""
        for symbol in self.symbols:
            book = self.book_manager.get_book(symbol)
            if not book:
                continue

            snapshot = book.get_snapshot()
            if not snapshot.bids and not snapshot.asks:
                logger.warning(f"Empty order book detected for {symbol}")
                continue

            best_bid, best_ask = book.get_top_of_book()
            if best_bid is not None and best_ask is not None:
                spread = (best_ask.price - best_bid.price) / best_bid.price * 100
                if spread > 5.0:  # Alert on spreads > 5%
                    logger.warning(
                        f"Large spread detected for {symbol}: {spread:.2f}% "
                        f"(Bid: {best_bid.price}, Ask: {best_ask.price})"
                    )
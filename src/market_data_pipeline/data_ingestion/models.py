from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional
import struct

class Side(str, Enum):
    BID = "bid"
    ASK = "ask"

class UpdateType(str, Enum):
    ADD = "add"
    MODIFY = "modify"
    DELETE = "delete"

@dataclass
class MarketUpdate:
    timestamp: int  # nanoseconds since epoch
    symbol: str
    price: float
    size: float
    side: Side
    update_type: UpdateType
    sequence_number: int
    exchange_id: str

    def to_binary(self) -> bytes:
        """Convert market update to binary format."""
        return struct.pack(
            "!QddfII12s12s",
            self.timestamp,
            self.price,
            self.size,
            self.side.encode(),
            self.update_type.encode(),
            self.sequence_number,
            self.exchange_id.encode()
        )

    @classmethod
    def from_binary(cls, data: bytes) -> 'MarketUpdate':
        """Create market update from binary data."""
        unpacked = struct.unpack("!QddfII12s12s", data)
        return cls(
            timestamp=unpacked[0],
            symbol=unpacked[3].decode().strip('\x00'),
            price=unpacked[1],
            size=unpacked[2],
            side=Side(unpacked[3].decode().strip('\x00')),
            update_type=UpdateType(unpacked[4].decode().strip('\x00')),
            sequence_number=unpacked[5],
            exchange_id=unpacked[6].decode().strip('\x00')
        )
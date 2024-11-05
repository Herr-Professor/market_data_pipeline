from dataclasses import dataclass, field
from typing import List
import yaml
import os

@dataclass
class MarketDataConfig:
    symbols: List[str]
    buffer_size: int
    max_depth: int

@dataclass
class StorageConfig:
    base_path: str
    compression: str = "snappy"
    partition_cols: List[str] = field(default_factory=lambda: ["date", "symbol"])  # Provide default list

@dataclass
class Config:
    market_data: MarketDataConfig
    storage: StorageConfig
    log_config_path: str

    @classmethod
    def load_config(cls, config_path: str) -> 'Config':
        with open(config_path, 'r') as f:
            config_dict = yaml.safe_load(f)
        
        market_data = MarketDataConfig(**config_dict['market_data'])
        storage = StorageConfig(**config_dict['storage'])
        
        return cls(
            market_data=market_data,
            storage=storage,
            log_config_path=config_dict['log_config_path']
        )

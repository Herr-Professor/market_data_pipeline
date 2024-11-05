import pytest
from market_data_pipeline.config.settings import Config, MarketDataConfig, StorageConfig

def test_config_loading(config):
    """Test configuration loading and validation."""
    conf = Config(
        market_data=MarketDataConfig(**config['market_data']),
        storage=StorageConfig(**config['storage']),
        log_config_path="config/logging_config.yaml"
    )
    
    assert conf.market_data.symbols == ['AAPL', 'MSFT', 'GOOGL']
    assert conf.market_data.buffer_size == 1000
    assert conf.storage.compression == 'snappy'
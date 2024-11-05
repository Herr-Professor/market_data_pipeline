import pytest
import sys
from pathlib import Path

# Add the src directory to Python path
src_path = str(Path(__file__).parent.parent / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

@pytest.fixture
def config():
    """Sample configuration for testing."""
    return {
        'market_data': {
            'symbols': ['AAPL', 'MSFT', 'GOOGL'],
            'buffer_size': 1000,
            'max_depth': 10
        },
        'storage': {
            'base_path': 'data/',
            'compression': 'snappy',
            'partition_cols': ['date', 'symbol']
        },
        'log_config_path': 'config/logging_config.yaml'
    }
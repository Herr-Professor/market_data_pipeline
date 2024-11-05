import logging
import logging.config
import yaml
import os
from pathlib import Path

def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for the given name."""
    return logging.getLogger(name)

def setup_logging(
    config_path: str = "config/logging_config.yaml",
    default_level: int = logging.INFO
) -> None:
    """Set up logging configuration."""
    try:
        # Create logs directory if it doesn't exist
        Path("logs").mkdir(exist_ok=True)
        
        with open(config_path, 'rt') as f:
            config = yaml.safe_load(f)
        logging.config.dictConfig(config)
    except Exception as e:
        print(f"Error in logging configuration: {e}")
        print("Using default logging configuration")
        logging.basicConfig(level=default_level)

"""
Centralized logging configuration.
Provides consistent logging across all modules.
"""
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

# Global logger registry
_loggers = {}


def get_logger(name: str, 
              log_file: Optional[str] = None,
              level: str = 'INFO') -> logging.Logger:
    """
    Get or create a logger with consistent formatting.
    
    Args:
        name: Logger name (usually __name__)
        log_file: Optional log file path
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
    
    Returns:
        Configured logger instance
    """
    if name in _loggers:
        return _loggers[name]
    
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level.upper()))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)  # File gets all logs
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    _loggers[name] = logger
    return logger


def setup_logger_for_module(module_name: str, 
                           logs_dir: Path,
                           prefix: str = '') -> logging.Logger:
    """
    Setup logger for a specific module with timestamped file.
    
    Args:
        module_name: Name of the module
        logs_dir: Directory for log files
        prefix: Optional prefix for log filename
    
    Returns:
        Configured logger
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = logs_dir / f"{prefix}{module_name}_{timestamp}.log"
    
    return get_logger(module_name, str(log_file))


class LoggerContextManager:
    """Context manager for temporary log level changes."""
    
    def __init__(self, logger: logging.Logger, level: str):
        self.logger = logger
        self.new_level = getattr(logging, level.upper())
        self.old_level = None
    
    def __enter__(self):
        self.old_level = self.logger.level
        self.logger.setLevel(self.new_level)
        return self.logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.setLevel(self.old_level)

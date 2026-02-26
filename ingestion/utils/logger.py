"""
Sistema de logging centralizado
"""
import logging
import sys
from pathlib import Path
from datetime import datetime
from pythonjsonlogger import jsonlogger
from config import Config

def setup_logger(name: str = 'music-ingestion') -> logging.Logger:
    """
    Configura y retorna un logger
    
    Args:
        name: Nombre del logger
    
    Returns:
        logging.Logger: Logger configurado
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, Config.LOG_LEVEL))
    
    # Evita duplicar handlers
    if logger.handlers:
        return logger
    
    # Console handler (human-readable)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler (JSON format para Cloud Logging)
    if Config.ENVIRONMENT == 'production':
        Config.LOGS_DIR.mkdir(exist_ok=True)
        
        log_file = Config.LOGS_DIR / f"ingestion_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        
        json_format = jsonlogger.JsonFormatter(
            '%(timestamp)s %(level)s %(name)s %(message)s'
        )
        file_handler.setFormatter(json_format)
        logger.addHandler(file_handler)
    
    return logger


# Logger global
logger = setup_logger()
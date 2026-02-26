"""
Decorador para reintentos automáticos con backoff exponencial
"""
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)
import logging
from config import Config

logger = logging.getLogger(__name__)

def retry_on_api_error(max_attempts: int = None, backoff_seconds: int = None):
    """
    Decorador para reintentar operaciones que fallen por errores de API
    
    Args:
        max_attempts: Número máximo de intentos
        backoff_seconds: Segundos base para backoff exponencial
    
    Returns:
        Decorated function con retry logic
    """
    max_attempts = max_attempts or Config.RETRY_MAX_ATTEMPTS
    backoff_seconds = backoff_seconds or Config.RETRY_BACKOFF_SECONDS
    
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(
            multiplier=backoff_seconds,
            min=backoff_seconds,
            max=backoff_seconds * 10
        ),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True
    )
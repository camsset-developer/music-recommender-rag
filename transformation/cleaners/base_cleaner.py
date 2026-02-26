"""
Clase base para todos los cleaners de datos
"""
from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger('transformation')


class BaseCleaner(ABC):
    """
    Clase base abstracta para implementar cleaners de datos.
    
    Todos los cleaners deben heredar de esta clase e implementar
    el método clean().
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Inicializa el cleaner.
        
        Args:
            config: Diccionario de configuración específico del cleaner
        """
        self.config = config or {}
        self.stats = {
            'records_processed': 0,
            'records_modified': 0,
            'records_dropped': 0,
            'errors': 0
        }
        logger.info(f"✅ {self.__class__.__name__} inicializado")
    
    @abstractmethod
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Método abstracto que debe implementar cada cleaner.
        
        Args:
            df: DataFrame con los datos a limpiar
            
        Returns:
            DataFrame limpio
        """
        pass
    
    def validate_input(self, df: pd.DataFrame) -> bool:
        """
        Valida que el DataFrame de entrada tenga el formato correcto.
        
        Args:
            df: DataFrame a validar
            
        Returns:
            True si es válido, False en caso contrario
        """
        if df is None or df.empty:
            logger.warning("⚠️  DataFrame vacío o None")
            return False
        return True
    
    def get_stats(self) -> Dict[str, int]:
        """
        Retorna las estadísticas de limpieza.
        
        Returns:
            Diccionario con estadísticas
        """
        return self.stats.copy()
    
    def reset_stats(self):
        """Reinicia las estadísticas."""
        self.stats = {
            'records_processed': 0,
            'records_modified': 0,
            'records_dropped': 0,
            'errors': 0
        }
    
    def log_summary(self):
        """Imprime un resumen de las operaciones de limpieza."""
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 Resumen de {self.__class__.__name__}:")
        logger.info(f"   Registros procesados: {self.stats['records_processed']}")
        logger.info(f"   Registros modificados: {self.stats['records_modified']}")
        logger.info(f"   Registros eliminados: {self.stats['records_dropped']}")
        logger.info(f"   Errores: {self.stats['errors']}")
        logger.info(f"{'='*60}\n")
"""
Text Cleaner - Limpieza y normalización de campos de texto
"""
import pandas as pd
import re
from typing import Optional
import logging

logger = logging.getLogger('transformation')


class TextCleaner:
    """
    Limpia y normaliza campos de texto como nombres de tracks, artistas, álbumes.
    """
    
    def __init__(self, config: Optional[dict] = None):
        """
        Inicializa el text cleaner.
        
        Args:
            config: Configuración de limpieza
        """
        self.config = config or {}
        self.lowercase = self.config.get('lowercase', True)
        self.remove_special_chars = self.config.get('remove_special_chars', False)
        self.remove_extra_spaces = self.config.get('remove_extra_spaces', True)
        self.min_length = self.config.get('min_length', 1)
        self.max_length = self.config.get('max_length', 500)
        
        self.stats = {
            'records_processed': 0,
            'records_modified': 0,
            'records_dropped': 0,
            'errors': 0
        }
        
        logger.info("✅ TextCleaner inicializado")
    
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia campos de texto en el DataFrame.
        
        Args:
            df: DataFrame con datos raw
            
        Returns:
            DataFrame con textos limpios
        """
        logger.info("🧹 Iniciando limpieza de texto...")
        
        if df is None or df.empty:
            logger.warning("⚠️  DataFrame vacío")
            return df
        
        df_clean = df.copy()
        initial_count = len(df_clean)
        
        # Campos de texto a limpiar
        text_fields = ['track_name', 'artist_name', 'album_name']
        
        for field in text_fields:
            if field in df_clean.columns:
                logger.info(f"   Limpiando campo: {field}")
                df_clean[field] = df_clean[field].apply(self._clean_text)
        
        # Eliminar registros sin nombre de track o artista
        df_clean = df_clean.dropna(subset=['track_name', 'artist_name'])
        
        self.stats['records_processed'] = initial_count
        self.stats['records_dropped'] = initial_count - len(df_clean)
        self.stats['records_modified'] = len(df_clean)
        
        logger.info(f"✅ Limpieza de texto completada")
        logger.info(f"   Procesados: {self.stats['records_processed']}")
        logger.info(f"   Modificados: {self.stats['records_modified']}")
        logger.info(f"   Eliminados: {self.stats['records_dropped']}")
        
        return df_clean
    
    def _clean_text(self, text: str) -> Optional[str]:
        """
        Limpia un texto individual.
        
        Args:
            text: Texto a limpiar
            
        Returns:
            Texto limpio o None si es inválido
        """
        if pd.isna(text) or not isinstance(text, str):
            return None
        
        # Eliminar espacios al inicio y final
        text = text.strip()
        
        # Remover espacios extra
        if self.remove_extra_spaces:
            text = re.sub(r'\s+', ' ', text)
        
        # Remover caracteres especiales si está configurado
        if self.remove_special_chars:
            # Mantener letras, números, espacios y algunos caracteres comunes
            text = re.sub(r'[^a-zA-Z0-9\s\-\'\".,!?()]', '', text)
        
        # Convertir a minúsculas si está configurado
        # (generalmente NO lo hacemos para nombres propios)
        # if self.lowercase:
        #     text = text.lower()
        
        # Validar longitud
        if len(text) < self.min_length or len(text) > self.max_length:
            return None
        
        return text if text else None
    
    def get_stats(self) -> dict:
        """Retorna estadísticas de limpieza."""
        return self.stats.copy()
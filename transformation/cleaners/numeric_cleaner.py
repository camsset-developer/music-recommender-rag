"""
Numeric Cleaner - Limpieza y normalización de campos numéricos
"""
import pandas as pd
import numpy as np
from typing import Optional, List
import logging

logger = logging.getLogger('transformation')


class NumericCleaner:
    """
    Limpia y normaliza campos numéricos como popularity, duration, playcount.
    """
    
    def __init__(self, config: Optional[dict] = None):
        """
        Inicializa el numeric cleaner.
        
        Args:
            config: Configuración de limpieza
        """
        self.config = config or {}
        self.fill_missing_with_median = self.config.get('fill_missing_with_median', True)
        self.remove_outliers = self.config.get('remove_outliers', False)
        self.outlier_std_threshold = self.config.get('outlier_std_threshold', 3.0)
        
        self.stats = {
            'records_processed': 0,
            'records_modified': 0,
            'missing_filled': 0,
            'outliers_removed': 0,
            'errors': 0
        }
        
        logger.info("✅ NumericCleaner inicializado")
    
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia campos numéricos en el DataFrame.
        
        Args:
            df: DataFrame con datos raw
            
        Returns:
            DataFrame con datos numéricos limpios
        """
        logger.info("🔢 Iniciando limpieza de datos numéricos...")
        
        if df is None or df.empty:
            logger.warning("⚠️  DataFrame vacío")
            return df
        
        df_clean = df.copy()
        initial_count = len(df_clean)
        
        # Campos numéricos a limpiar
        numeric_fields = {
            'popularity': (0, 100),
            'duration_ms': (0, 3600000),  # 0 a 60 minutos
            'explicit': (0, 1)
        }
        
        # Limpiar campos de Spotify
        for field, (min_val, max_val) in numeric_fields.items():
            if field in df_clean.columns:
                logger.info(f"   Limpiando campo: {field}")
                df_clean = self._clean_numeric_field(
                    df_clean, field, min_val, max_val
                )
        
        # Limpiar campos de Last.fm si existen
        if 'lastfm' in df_clean.columns:
            logger.info("   Limpiando campos de Last.fm...")
            df_clean = self._clean_lastfm_fields(df_clean)
        
        self.stats['records_processed'] = initial_count
        self.stats['records_modified'] = len(df_clean)
        
        logger.info(f"✅ Limpieza numérica completada")
        logger.info(f"   Procesados: {self.stats['records_processed']}")
        logger.info(f"   Missing rellenados: {self.stats['missing_filled']}")
        logger.info(f"   Outliers removidos: {self.stats['outliers_removed']}")
        
        return df_clean
    
    def _clean_numeric_field(
        self, 
        df: pd.DataFrame, 
        field: str, 
        min_val: float, 
        max_val: float
    ) -> pd.DataFrame:
        """
        Limpia un campo numérico específico.
        
        Args:
            df: DataFrame
            field: Nombre del campo
            min_val: Valor mínimo válido
            max_val: Valor máximo válido
            
        Returns:
            DataFrame limpio
        """
        # Convertir a numérico, forzando errores a NaN
        df[field] = pd.to_numeric(df[field], errors='coerce')
        
        # Contar valores faltantes
        missing_before = df[field].isna().sum()
        
        # Validar rangos
        df.loc[df[field] < min_val, field] = np.nan
        df.loc[df[field] > max_val, field] = np.nan
        
        # Rellenar valores faltantes con la mediana
        if self.fill_missing_with_median and df[field].notna().any():
            median_val = df[field].median()
            df[field].fillna(median_val, inplace=True)
            missing_after = df[field].isna().sum()
            filled = missing_before - missing_after
            self.stats['missing_filled'] += filled
            if filled > 0:
                logger.info(f"      Rellenados {filled} valores con mediana: {median_val:.2f}")
        
        # Remover outliers si está configurado
        if self.remove_outliers:
            outliers = self._detect_outliers(df[field])
            if outliers.any():
                df = df[~outliers]
                self.stats['outliers_removed'] += outliers.sum()
                logger.info(f"      Removidos {outliers.sum()} outliers")
        
        return df
    
    def _clean_lastfm_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia campos específicos de Last.fm.
        
        Args:
            df: DataFrame con datos de Last.fm
            
        Returns:
            DataFrame limpio
        """
        # Extraer playcount si existe
        if 'lastfm' in df.columns:
            try:
                # Intentar extraer playcount del struct
                df['lastfm_playcount'] = df['lastfm'].apply(
                    lambda x: x.get('playcount', 0) if isinstance(x, dict) else 0
                )
                df['lastfm_playcount'] = pd.to_numeric(df['lastfm_playcount'], errors='coerce').fillna(0)
                
                # Validar que sea no negativo
                df.loc[df['lastfm_playcount'] < 0, 'lastfm_playcount'] = 0
                
                logger.info(f"      ✅ Campo lastfm_playcount extraído")
            except Exception as e:
                logger.warning(f"      ⚠️  Error extrayendo playcount: {e}")
        
        return df
    
    def _detect_outliers(self, series: pd.Series) -> pd.Series:
        """
        Detecta outliers usando el método de desviación estándar.
        
        Args:
            series: Serie de datos numéricos
            
        Returns:
            Serie booleana indicando outliers
        """
        mean = series.mean()
        std = series.std()
        threshold = self.outlier_std_threshold * std
        
        outliers = (series < mean - threshold) | (series > mean + threshold)
        return outliers
    
    def get_stats(self) -> dict:
        """Retorna estadísticas de limpieza."""
        return self.stats.copy()
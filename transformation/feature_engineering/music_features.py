"""
Music Features - Generación de features específicas de música
"""
import pandas as pd
import numpy as np
from typing import Optional, Dict, Tuple
import logging

logger = logging.getLogger('transformation')


class MusicFeatureGenerator:
    """
    Genera features específicas para datos musicales.
    """
    
    def __init__(self, config: Optional[dict] = None):
        """
        Inicializa el generador de features musicales.
        
        Args:
            config: Configuración de features
        """
        self.config = config or {}
        self.normalize_popularity = self.config.get('normalize_popularity', True)
        self.create_popularity_bins = self.config.get('create_popularity_bins', True)
        self.popularity_bins = self.config.get('popularity_bins', [0, 20, 40, 60, 80, 100])
        self.create_era_from_year = self.config.get('create_era_from_year', True)
        self.eras = self.config.get('eras', {})
        
        self.stats = {
            'features_created': 0,
            'records_processed': 0
        }
        
        logger.info("✅ MusicFeatureGenerator inicializado")
    
    def generate_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Genera features musicales.
        
        Args:
            df: DataFrame con datos limpios
            
        Returns:
            DataFrame con features adicionales
        """
        logger.info("🎵 Generando features musicales...")
        
        if df is None or df.empty:
            logger.warning("⚠️  DataFrame vacío")
            return df
        
        df_features = df.copy()
        initial_cols = len(df_features.columns)
        
        # 1. Features de popularidad
        if 'popularity' in df_features.columns:
            df_features = self._create_popularity_features(df_features)
        
        # 2. Features de duración
        if 'duration_ms' in df_features.columns:
            df_features = self._create_duration_features(df_features)
        
        # 3. Features de fecha/era
        if 'release_date' in df_features.columns:
            df_features = self._create_temporal_features(df_features)
        
        # 4. Features de Last.fm
        if 'lastfm' in df_features.columns:
            df_features = self._create_lastfm_features(df_features)
        
        final_cols = len(df_features.columns)
        self.stats['features_created'] = final_cols - initial_cols
        self.stats['records_processed'] = len(df_features)
        
        logger.info(f"✅ Features musicales generadas: {self.stats['features_created']}")
        
        return df_features
    
    def _create_popularity_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Crea features basadas en popularidad.
        
        Args:
            df: DataFrame
            
        Returns:
            DataFrame con features de popularidad
        """
        logger.info("   Creando features de popularidad...")
        
        # Normalizar popularidad (0-1)
        if self.normalize_popularity:
            df['popularity_normalized'] = df['popularity'] / 100.0
        
        # Bins de popularidad
        if self.create_popularity_bins:
            df['popularity_bin'] = pd.cut(
                df['popularity'],
                bins=self.popularity_bins,
                labels=['very_low', 'low', 'medium', 'high', 'very_high'],
                include_lowest=True
            )
            df['popularity_bin'] = df['popularity_bin'].astype(str)
        
        # Clasificación simple
        df['is_popular'] = (df['popularity'] >= 60).astype(int)
        
        logger.info("      ✅ Features de popularidad creadas")
        return df
    
    def _create_duration_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Crea features basadas en duración.
        
        Args:
            df: DataFrame
            
        Returns:
            DataFrame con features de duración
        """
        logger.info("   Creando features de duración...")
        
        # Duración en segundos y minutos
        df['duration_seconds'] = df['duration_ms'] / 1000.0
        df['duration_minutes'] = df['duration_seconds'] / 60.0
        
        # Categorías de duración
        df['duration_category'] = pd.cut(
            df['duration_minutes'],
            bins=[0, 2, 3.5, 5, 10, float('inf')],
            labels=['very_short', 'short', 'medium', 'long', 'very_long']
        )
        df['duration_category'] = df['duration_category'].astype(str)
        
        # Es una canción corta/larga
        df['is_short_track'] = (df['duration_minutes'] < 3).astype(int)
        df['is_long_track'] = (df['duration_minutes'] > 5).astype(int)
        
        logger.info("      ✅ Features de duración creadas")
        return df
    
    def _create_temporal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Crea features temporales basadas en fecha de lanzamiento.
        
        Args:
            df: DataFrame
            
        Returns:
            DataFrame con features temporales
        """
        logger.info("   Creando features temporales...")
        
        # Convertir a datetime
        df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
        
        # Extraer año, mes, día
        df['release_year'] = df['release_date'].dt.year
        df['release_month'] = df['release_date'].dt.month
        df['release_day'] = df['release_date'].dt.day
        
        # Década
        df['release_decade'] = (df['release_year'] // 10) * 10
        df['release_decade'] = df['release_decade'].apply(lambda x: f"{int(x)}s" if pd.notna(x) else None)
        
        # Era musical
        if self.create_era_from_year:
            df['music_era'] = df['release_year'].apply(self._get_music_era)
        
        # Antiguedad (años desde el lanzamiento)
        current_year = pd.Timestamp.now().year
        df['track_age_years'] = current_year - df['release_year']
        
        # Es un lanzamiento reciente (< 1 año)
        df['is_recent_release'] = (df['track_age_years'] < 1).astype(int)
        
        logger.info("      ✅ Features temporales creadas")
        return df
    
    def _create_lastfm_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Crea features basadas en datos de Last.fm.
        
        Args:
            df: DataFrame
            
        Returns:
            DataFrame con features de Last.fm
        """
        logger.info("   Creando features de Last.fm...")
        
        try:
            # Extraer playcount
            df['lastfm_playcount'] = df['lastfm'].apply(
                lambda x: x.get('playcount', 0) if isinstance(x, dict) else 0
            )
            
            # Normalizar playcount (log transform para manejar valores muy grandes)
            df['lastfm_playcount_log'] = np.log1p(df['lastfm_playcount'])
            
            # Extraer número de tags
            df['lastfm_num_tags'] = df['lastfm'].apply(
                lambda x: len(x.get('tags', [])) if isinstance(x, dict) else 0
            )
            
            # Extraer número de tracks similares
            df['lastfm_num_similar'] = df['lastfm'].apply(
                lambda x: len(x.get('similar_tracks', [])) if isinstance(x, dict) else 0
            )
            
            # Extraer top 3 tags
            for i in range(3):
                df[f'lastfm_tag_{i+1}'] = df['lastfm'].apply(
                    lambda x: x.get('tags', [])[i]['name'] if isinstance(x, dict) and len(x.get('tags', [])) > i else None
                )
            
            logger.info("      ✅ Features de Last.fm creadas")
        except Exception as e:
            logger.warning(f"      ⚠️  Error creando features de Last.fm: {e}")
        
        return df
    
    def _get_music_era(self, year: float) -> Optional[str]:
        """
        Determina la era musical basada en el año.
        
        Args:
            year: Año de lanzamiento
            
        Returns:
            Nombre de la era
        """
        if pd.isna(year):
            return None
        
        year = int(year)
        for era_name, (start, end) in self.eras.items():
            if start <= year < end:
                return era_name
        
        return 'unknown'
    
    def get_stats(self) -> dict:
        """Retorna estadísticas de generación de features."""
        return self.stats.copy()
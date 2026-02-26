"""
Feature Embeddings Generator - Procesa y normaliza features numéricas
"""
import pandas as pd
import numpy as np
from typing import List, Optional
import logging
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler
from sklearn.decomposition import PCA

logger = logging.getLogger('embeddings')


class FeatureEmbeddingsGenerator:
    """
    Genera embeddings de features numéricas usando normalización y PCA.
    """
    
    def __init__(self, config: dict):
        """
        Inicializa el generador de feature embeddings.
        
        Args:
            config: Configuración de embeddings de features
        """
        self.config = config
        self.scaler_type = config.get('scaler', 'standard')
        self.target_dimension = config.get('dimension', 50)
        self.use_pca = config.get('use_pca', True)
        
        # Inicializar scaler
        if self.scaler_type == 'standard':
            self.scaler = StandardScaler()
        elif self.scaler_type == 'minmax':
            self.scaler = MinMaxScaler()
        elif self.scaler_type == 'robust':
            self.scaler = RobustScaler()
        else:
            self.scaler = StandardScaler()
        
        # Inicializar PCA si está habilitado
        self.pca = None
        if self.use_pca:
            self.pca = PCA(n_components=self.target_dimension)
        
        self.stats = {
            'features_processed': 0,
            'embeddings_generated': 0,
            'original_dimension': 0,
            'final_dimension': 0,
            'variance_explained': 0.0
        }
        
        logger.info(f"✅ FeatureEmbeddingsGenerator inicializado")
        logger.info(f"   Scaler: {self.scaler_type}")
        logger.info(f"   PCA habilitado: {self.use_pca}")
        if self.use_pca:
            logger.info(f"   Dimensión objetivo: {self.target_dimension}")
    
    def generate_embeddings(self, df: pd.DataFrame, 
                          numeric_features: List[str]) -> pd.DataFrame:
        """
        Genera embeddings de features numéricas.
        
        Args:
            df: DataFrame con datos de canciones
            numeric_features: Lista de columnas numéricas a usar
            
        Returns:
            DataFrame con embeddings de features agregados
        """
        logger.info("🔢 Generando embeddings de features numéricas...")
        
        if df is None or df.empty:
            logger.warning("⚠️  DataFrame vacío")
            return df
        
        df_embeddings = df.copy()
        
        # Filtrar solo las columnas que existen
        available_features = [f for f in numeric_features if f in df.columns]
        logger.info(f"   Features disponibles: {len(available_features)}/{len(numeric_features)}")
        
        if not available_features:
            logger.error("❌ No hay features numéricas disponibles")
            return df_embeddings
        
        # Extraer features numéricas
        X = df_embeddings[available_features].copy()
        
        # Manejar valores faltantes
        X = self._handle_missing_values(X)
        
        # Normalizar features
        logger.info(f"   Normalizando features con {self.scaler_type} scaler...")
        X_scaled = self.scaler.fit_transform(X)
        
        self.stats['original_dimension'] = X_scaled.shape[1]
        
        # Aplicar PCA si está habilitado
        if self.use_pca and X_scaled.shape[1] > self.target_dimension:
            logger.info(f"   Aplicando PCA: {X_scaled.shape[1]} → {self.target_dimension} dimensiones...")
            X_reduced = self.pca.fit_transform(X_scaled)
            
            variance_explained = self.pca.explained_variance_ratio_.sum()
            self.stats['variance_explained'] = variance_explained
            self.stats['final_dimension'] = X_reduced.shape[1]
            
            logger.info(f"   Varianza explicada: {variance_explained:.2%}")
            
            # Convertir a lista de arrays para almacenar en DataFrame
            feature_embeddings = [row for row in X_reduced]
        else:
            # Sin PCA, usar features normalizadas directamente
            self.stats['final_dimension'] = X_scaled.shape[1]
            feature_embeddings = [row for row in X_scaled]
        
        # Agregar embeddings al DataFrame
        df_embeddings['feature_embedding'] = feature_embeddings
        df_embeddings['feature_dimension'] = self.stats['final_dimension']
        
        self.stats['features_processed'] = len(available_features)
        self.stats['embeddings_generated'] = len(feature_embeddings)
        
        logger.info(f"✅ Embeddings de features generados: {self.stats['embeddings_generated']}")
        logger.info(f"   Dimensión final: {self.stats['final_dimension']}")
        
        return df_embeddings
    
    def _handle_missing_values(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Maneja valores faltantes en las features.
        
        Args:
            X: DataFrame con features
            
        Returns:
            DataFrame sin valores faltantes
        """
        # Rellenar NaN con la mediana de cada columna
        for col in X.columns:
            if X[col].isna().any():
                median_val = X[col].median()
                X[col].fillna(median_val, inplace=True)
                logger.info(f"   Rellenados {X[col].isna().sum()} valores faltantes en {col}")
        
        return X
    
    def get_stats(self) -> dict:
        """Retorna estadísticas de generación."""
        return self.stats.copy()
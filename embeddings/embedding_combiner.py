"""
Embedding Combiner - Combina embeddings de texto y features numéricas
"""
import pandas as pd
import numpy as np
from typing import Optional
import logging

logger = logging.getLogger('embeddings')


class EmbeddingCombiner:
    """
    Combina embeddings de texto y features en un solo vector.
    """
    
    def __init__(self, config: dict):
        """
        Inicializa el combinador de embeddings.
        
        Args:
            config: Configuración de combinación
        """
        self.config = config
        self.text_weight = config.get('text_weight', 0.7)
        self.features_weight = config.get('features_weight', 0.3)
        self.normalize = config.get('normalize', True)
        
        logger.info(f"✅ EmbeddingCombiner inicializado")
        logger.info(f"   Peso texto: {self.text_weight}")
        logger.info(f"   Peso features: {self.features_weight}")
        logger.info(f"   Normalizar: {self.normalize}")
    
    def combine_embeddings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Combina embeddings de texto y features.
        
        Args:
            df: DataFrame con ambos tipos de embeddings
            
        Returns:
            DataFrame con embeddings combinados
        """
        logger.info("🔗 Combinando embeddings...")
        
        if df is None or df.empty:
            logger.warning("⚠️  DataFrame vacío")
            return df
        
        df_combined = df.copy()
        
        # Verificar que ambos tipos de embeddings existan
        if 'text_embedding' not in df.columns or 'feature_embedding' not in df.columns:
            logger.error("❌ Faltan embeddings de texto o features")
            return df_combined
        
        combined_embeddings = []
        
        for idx, row in df_combined.iterrows():
            text_emb = row['text_embedding']
            feature_emb = row['feature_embedding']
            
            if text_emb is None or feature_emb is None:
                logger.warning(f"⚠️  Registro {idx}: embedding faltante")
                combined_embeddings.append(None)
                continue
            
            # Asegurar que sean numpy arrays
            text_emb = np.array(text_emb) if not isinstance(text_emb, np.ndarray) else text_emb
            feature_emb = np.array(feature_emb) if not isinstance(feature_emb, np.ndarray) else feature_emb
            
            # Normalizar embeddings individuales si es necesario
            if self.normalize:
                text_emb = self._normalize_vector(text_emb)
                feature_emb = self._normalize_vector(feature_emb)
            
            # Aplicar pesos y concatenar
            text_weighted = text_emb * self.text_weight
            features_weighted = feature_emb * self.features_weight
            
            # Concatenar ambos vectores
            combined = np.concatenate([text_weighted, features_weighted])
            
            # Normalizar el vector combinado
            if self.normalize:
                combined = self._normalize_vector(combined)
            
            combined_embeddings.append(combined)
        
        # Agregar embeddings combinados al DataFrame
        df_combined['combined_embedding'] = combined_embeddings
        df_combined['combined_dimension'] = len(combined_embeddings[0]) if combined_embeddings[0] is not None else 0
        
        valid_embeddings = len([e for e in combined_embeddings if e is not None])
        logger.info(f"✅ Embeddings combinados: {valid_embeddings}/{len(df_combined)}")
        
        return df_combined
    
    def _normalize_vector(self, vector: np.ndarray) -> np.ndarray:
        """
        Normaliza un vector usando norma L2.
        
        Args:
            vector: Vector a normalizar
            
        Returns:
            Vector normalizado
        """
        norm = np.linalg.norm(vector)
        if norm == 0:
            return vector
        return vector / norm
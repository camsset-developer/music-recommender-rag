"""
Text Embeddings Generator - Genera embeddings de texto usando Vertex AI
"""
import pandas as pd
import numpy as np
from typing import List, Optional
import logging
from vertexai.language_models import TextEmbeddingModel
import vertexai

logger = logging.getLogger('embeddings')


class TextEmbeddingsGenerator:
    """
    Genera embeddings de texto para canciones usando Vertex AI.
    """
    
    def __init__(self, config: dict, project_id: str, location: str):
        """
        Inicializa el generador de embeddings de texto.
        
        Args:
            config: Configuración de embeddings de texto
            project_id: ID del proyecto GCP
            location: Ubicación de Vertex AI
        """
        self.config = config
        self.project_id = project_id
        self.location = location
        self.model_name = config.get('model', 'text-embedding-004')
        self.dimension = config.get('dimension', 768)
        self.batch_size = config.get('batch_size', 25)

        
        # Inicializar Vertex AI
        vertexai.init(project=project_id, location=location)
        self.model = TextEmbeddingModel.from_pretrained(self.model_name)
        
        self.stats = {
            'texts_processed': 0,
            'embeddings_generated': 0,
            'errors': 0
        }
        
        logger.info(f"✅ TextEmbeddingsGenerator inicializado")
        logger.info(f"   Modelo: {self.model_name}")
        logger.info(f"   Dimensión: {self.dimension}")
    
    def generate_embeddings(self, df: pd.DataFrame, text_fields: List[str], 
                          tag_fields: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Genera embeddings para los textos en el DataFrame.
        
        Args:
            df: DataFrame con datos de canciones
            text_fields: Campos de texto a combinar
            tag_fields: Campos de tags de Last.fm (opcional)
            
        Returns:
            DataFrame con embeddings agregados
        """
        logger.info("📝 Generando embeddings de texto...")
        
        if df is None or df.empty:
            logger.warning("⚠️  DataFrame vacío")
            return df
        
        df_embeddings = df.copy()
        
        # Preparar textos combinados
        logger.info("   Preparando textos...")
        texts = self._prepare_texts(df_embeddings, text_fields, tag_fields)
        
        # Generar embeddings en batches
        logger.info(f"   Generando embeddings para {len(texts)} textos...")
        embeddings = self._generate_embeddings_batch(texts)
        
        # Agregar embeddings al DataFrame
        df_embeddings['text_embedding'] = embeddings
        df_embeddings['embedding_dimension'] = self.dimension
        
        self.stats['texts_processed'] = len(texts)
        self.stats['embeddings_generated'] = len([e for e in embeddings if e is not None])
        
        logger.info(f"✅ Embeddings de texto generados: {self.stats['embeddings_generated']}/{len(texts)}")
        
        return df_embeddings
    
    def _prepare_texts(self, df: pd.DataFrame, text_fields: List[str],
                      tag_fields: Optional[List[str]] = None) -> List[str]:
        """
        Prepara textos combinando múltiples campos.
        
        Args:
            df: DataFrame
            text_fields: Campos de texto básicos
            tag_fields: Campos de tags
            
        Returns:
            Lista de textos preparados
        """
        texts = []
        
        for _, row in df.iterrows():
            text_parts = []
            
            # Agregar campos de texto básicos
            for field in text_fields:
                if field in row and pd.notna(row[field]):
                    text_parts.append(str(row[field]))
            
            # Agregar tags de Last.fm si están disponibles
            if tag_fields:
                tags = []
                for tag_field in tag_fields:
                    if tag_field in row and pd.notna(row[tag_field]):
                        tags.append(str(row[tag_field]))
                
                if tags:
                    text_parts.append("Tags: " + ", ".join(tags))
            
            # Combinar todo en un solo texto
            combined_text = " | ".join(text_parts)
            texts.append(combined_text)
        
        return texts
    
    def _generate_embeddings_batch(self, texts: List[str]) -> List[Optional[np.ndarray]]:
        """
        Genera embeddings en batches para manejar límites de API.
        
        Args:
            texts: Lista de textos
            
        Returns:
            Lista de embeddings (arrays numpy)
        """
        all_embeddings = []
        total_batches = (len(texts) + self.batch_size - 1) // self.batch_size
        
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            
            logger.info(f"   Procesando batch {batch_num}/{total_batches} ({len(batch)} textos)...")
            
            try:
                # Llamar a la API de Vertex AI
                embeddings_response = self.model.get_embeddings(batch)
                                
                # Extraer vectores de embeddings
                batch_embeddings = [
                    np.array(emb.values) for emb in embeddings_response
                ]
                
                all_embeddings.extend(batch_embeddings)
                
            except Exception as e:
                logger.error(f"❌ Error generando embeddings para batch {batch_num}: {e}")
                # Agregar None para mantener la correspondencia con los textos
                all_embeddings.extend([None] * len(batch))
                self.stats['errors'] += len(batch)
        
        return all_embeddings
    
    def get_stats(self) -> dict:
        """Retorna estadísticas de generación."""
        return self.stats.copy()
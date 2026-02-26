"""
Recommender System - Sistema completo de recomendación de música
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Union
import logging
from google.cloud import bigquery
from difflib import SequenceMatcher

logger = logging.getLogger('recommender')


class MusicRecommender:
    """
    Sistema de recomendación de música basado en embeddings.
    """
    
    def __init__(
        self,
        similarity_engine,
        config: dict,
        project_id: str,
        table_embeddings: str,
        table_features: str
    ):
        """
        Inicializa el sistema de recomendación.
        
        Args:
            similarity_engine: Motor de similitud
            config: Configuración de recomendación
            project_id: ID del proyecto GCP
            table_embeddings: Tabla con embeddings
            table_features: Tabla con features
        """
        self.similarity_engine = similarity_engine
        self.config = config
        self.project_id = project_id
        self.table_embeddings = table_embeddings
        self.table_features = table_features
        
        self.client = bigquery.Client(project=project_id)
        
        # Cache
        self.cache_enabled = config.get('cache_embeddings', True)
        self._embeddings_cache = None
        self._metadata_cache = None
        
        logger.info("✅ MusicRecommender inicializado")
    
    def load_data(self):
        """Carga embeddings y metadata desde BigQuery."""
        logger.info("📥 Cargando datos desde BigQuery...")
        
        # Query para embeddings
        query = f"""
        SELECT 
            track_id,
            track_name,
            artist_name,
            album_name,
            combined_embedding,
            text_embedding,
            feature_embedding
        FROM `{self.table_embeddings}`
        """
        
        df = self.client.query(query).to_dataframe()
        
        logger.info(f"✅ Datos cargados: {len(df)} canciones")
        
        # Cachear si está habilitado
        if self.cache_enabled:
            self._embeddings_cache = df
            logger.info("💾 Datos cacheados en memoria")
        
        return df
    
    def get_recommendations(
        self,
        track_id: Optional[str] = None,
        track_name: Optional[str] = None,
        artist_name: Optional[str] = None,
        k: int = 10,
        embedding_type: str = 'combined'
    ) -> List[Dict]:
        """
        Obtiene recomendaciones para una canción.
        
        Args:
            track_id: ID de la canción (opcional)
            track_name: Nombre de la canción (opcional)
            artist_name: Nombre del artista (opcional, para desambiguar)
            k: Número de recomendaciones
            embedding_type: Tipo de embedding ('combined', 'text', 'feature')
            
        Returns:
            Lista de diccionarios con recomendaciones
        """
        # Cargar datos si no están en cache
        if self._embeddings_cache is None:
            df = self.load_data()
        else:
            df = self._embeddings_cache
        
        # Buscar la canción query
        query_row = self._find_track(df, track_id, track_name, artist_name)
        
        if query_row is None:
            logger.error("❌ Canción no encontrada")
            return []
        
        query_track_id = query_row['track_id'].values[0]
        query_track_name = query_row['track_name'].values[0]
        query_artist_name = query_row['artist_name'].values[0]
        
        logger.info(f"🎵 Buscando similares a: {query_track_name} - {query_artist_name}")
        
        # Obtener embedding de la query
        embedding_col = f'{embedding_type}_embedding'
        if embedding_col not in df.columns:
            logger.error(f"❌ Tipo de embedding no disponible: {embedding_type}")
            return []
        
        query_embedding = np.array(query_row[embedding_col].values[0])
        
        # Crear matriz de embeddings
        embeddings_matrix = np.vstack(df[embedding_col].values)
        
        # Encontrar índice de la query para excluirla
        query_index = df[df['track_id'] == query_track_id].index[0]
        
        # Buscar similares
        similar_indices = self.similarity_engine.find_similar(
            query_embedding=query_embedding,
            embeddings_matrix=embeddings_matrix,
            k=k + 1,  # +1 porque excluiremos la query
            exclude_indices=[query_index]
        )
        
        # Preparar resultados
        recommendations = []
        
        for idx, similarity_score in similar_indices:
            if idx == query_index:
                continue
            
            track = df.iloc[idx]
            
            # Aplicar filtros si están configurados
            if self.config.get('exclude_same_artist', False):
                if track['artist_name'] == query_artist_name:
                    continue
            
            if similarity_score < self.config.get('min_similarity_threshold', 0.0):
                continue
            
            recommendation = {
                'track_id': track['track_id'],
                'track_name': track['track_name'],
                'artist_name': track['artist_name'],
                'album_name': track['album_name'],
                'similarity_score': round(similarity_score, 4)
            }
            
            recommendations.append(recommendation)
            
            if len(recommendations) >= k:
                break
        
        logger.info(f"✅ Encontradas {len(recommendations)} recomendaciones")
        
        return recommendations
    
    def _find_track(
        self,
        df: pd.DataFrame,
        track_id: Optional[str],
        track_name: Optional[str],
        artist_name: Optional[str]
    ) -> Optional[pd.DataFrame]:
        """
        Busca una canción en el DataFrame.
        
        Args:
            df: DataFrame con canciones
            track_id: ID de la canción
            track_name: Nombre de la canción
            artist_name: Nombre del artista
            
        Returns:
            DataFrame con la canción encontrada o None
        """
        # Búsqueda por ID (más precisa)
        if track_id:
            result = df[df['track_id'] == track_id]
            if not result.empty:
                return result
        
        # Búsqueda por nombre
        if track_name:
            # Búsqueda exacta
            result = df[df['track_name'].str.lower() == track_name.lower()]
            
            if not result.empty:
                # Si hay múltiples resultados y se proporciona artista, filtrar
                if len(result) > 1 and artist_name:
                    result = result[result['artist_name'].str.lower() == artist_name.lower()]
                
                if not result.empty:
                    return result.head(1)
            
            # Búsqueda fuzzy si está habilitada
            if self.config.get('fuzzy_matching', True):
                result = self._fuzzy_search(df, track_name, artist_name)
                if result is not None:
                    return result
        
        return None
    
    def _fuzzy_search(
        self,
        df: pd.DataFrame,
        track_name: str,
        artist_name: Optional[str]
    ) -> Optional[pd.DataFrame]:
        """
        Búsqueda difusa por nombre de canción.
        
        Args:
            df: DataFrame con canciones
            track_name: Nombre de la canción
            artist_name: Nombre del artista (opcional)
            
        Returns:
            DataFrame con la mejor coincidencia o None
        """
        threshold = self.config.get('fuzzy_threshold', 0.8)
        
        best_match = None
        best_score = 0
        
        for idx, row in df.iterrows():
            # Calcular similitud de nombre
            name_similarity = SequenceMatcher(
                None,
                track_name.lower(),
                row['track_name'].lower()
            ).ratio()
            
            # Si se proporciona artista, considerar también su similitud
            if artist_name:
                artist_similarity = SequenceMatcher(
                    None,
                    artist_name.lower(),
                    row['artist_name'].lower()
                ).ratio()
                
                # Promedio ponderado
                combined_score = name_similarity * 0.7 + artist_similarity * 0.3
            else:
                combined_score = name_similarity
            
            if combined_score > best_score and combined_score >= threshold:
                best_score = combined_score
                best_match = row
        
        if best_match is not None:
            logger.info(f"🔍 Fuzzy match encontrado: {best_match['track_name']} - {best_match['artist_name']} (score: {best_score:.2f})")
            return pd.DataFrame([best_match])
        
        return None
    
    def get_batch_recommendations(
        self,
        track_ids: List[str],
        k: int = 10,
        embedding_type: str = 'combined'
    ) -> Dict[str, List[Dict]]:
        """
        Obtiene recomendaciones para múltiples canciones.
        
        Args:
            track_ids: Lista de IDs de canciones
            k: Número de recomendaciones por canción
            embedding_type: Tipo de embedding
            
        Returns:
            Diccionario con track_id como key y recomendaciones como value
        """
        results = {}
        
        for track_id in track_ids:
            recommendations = self.get_recommendations(
                track_id=track_id,
                k=k,
                embedding_type=embedding_type
            )
            results[track_id] = recommendations
        
        return results
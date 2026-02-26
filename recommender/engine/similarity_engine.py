"""
Similarity Engine - Motor de búsqueda de similitud basado en embeddings
"""
import numpy as np
from typing import List, Tuple, Optional
import logging
from sklearn.metrics.pairwise import cosine_similarity, euclidean_distances

logger = logging.getLogger('recommender')


class SimilarityEngine:
    """
    Motor de búsqueda de similitud entre embeddings.
    """
    
    def __init__(self, metric: str = 'cosine'):
        """
        Inicializa el motor de similitud.
        
        Args:
            metric: Métrica de similitud ('cosine', 'euclidean', 'dot')
        """
        self.metric = metric
        
        if metric not in ['cosine', 'euclidean', 'dot']:
            raise ValueError(f"Métrica no soportada: {metric}")
        
        logger.info(f"✅ SimilarityEngine inicializado con métrica: {metric}")
    
    def find_similar(
        self, 
        query_embedding: np.ndarray,
        embeddings_matrix: np.ndarray,
        k: int = 10,
        exclude_indices: Optional[List[int]] = None
    ) -> List[Tuple[int, float]]:
        """
        Encuentra los K embeddings más similares al query.
        
        Args:
            query_embedding: Embedding de la canción query (1D array)
            embeddings_matrix: Matrix de todos los embeddings (2D array)
            k: Número de resultados a retornar
            exclude_indices: Índices a excluir (ej: la misma canción)
            
        Returns:
            Lista de tuplas (index, similarity_score) ordenadas por similitud
        """
        # Asegurar que query_embedding sea 2D
        if query_embedding.ndim == 1:
            query_embedding = query_embedding.reshape(1, -1)
        
        # Calcular similitudes
        if self.metric == 'cosine':
            similarities = cosine_similarity(query_embedding, embeddings_matrix)[0]
        elif self.metric == 'euclidean':
            # Convertir distancia a similitud (invertir)
            distances = euclidean_distances(query_embedding, embeddings_matrix)[0]
            similarities = 1 / (1 + distances)  # Mayor distancia = menor similitud
        elif self.metric == 'dot':
            similarities = np.dot(embeddings_matrix, query_embedding.T).flatten()
        
        # Excluir índices si se especifican
        if exclude_indices:
            for idx in exclude_indices:
                if 0 <= idx < len(similarities):
                    similarities[idx] = -np.inf
        
        # Obtener top K índices
        top_k_indices = np.argsort(similarities)[::-1][:k]
        
        # Crear lista de resultados con scores
        results = [
            (int(idx), float(similarities[idx]))
            for idx in top_k_indices
        ]
        
        return results
    
    def compute_similarity(
        self,
        embedding1: np.ndarray,
        embedding2: np.ndarray
    ) -> float:
        """
        Calcula la similitud entre dos embeddings individuales.
        
        Args:
            embedding1: Primer embedding
            embedding2: Segundo embedding
            
        Returns:
            Score de similitud
        """
        # Asegurar forma correcta
        if embedding1.ndim == 1:
            embedding1 = embedding1.reshape(1, -1)
        if embedding2.ndim == 1:
            embedding2 = embedding2.reshape(1, -1)
        
        if self.metric == 'cosine':
            return float(cosine_similarity(embedding1, embedding2)[0][0])
        elif self.metric == 'euclidean':
            distance = euclidean_distances(embedding1, embedding2)[0][0]
            return float(1 / (1 + distance))
        elif self.metric == 'dot':
            return float(np.dot(embedding1, embedding2.T)[0][0])
    
    def batch_similarity(
        self,
        query_embeddings: np.ndarray,
        target_embeddings: np.ndarray
    ) -> np.ndarray:
        """
        Calcula similitud entre múltiples queries y targets.
        
        Args:
            query_embeddings: Matrix de queries (N x D)
            target_embeddings: Matrix de targets (M x D)
            
        Returns:
            Matrix de similitudes (N x M)
        """
        if self.metric == 'cosine':
            return cosine_similarity(query_embeddings, target_embeddings)
        elif self.metric == 'euclidean':
            distances = euclidean_distances(query_embeddings, target_embeddings)
            return 1 / (1 + distances)
        elif self.metric == 'dot':
            return np.dot(query_embeddings, target_embeddings.T)
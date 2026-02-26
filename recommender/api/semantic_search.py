"""
Semantic Search - Búsqueda semántica usando embeddings de texto libre
"""
import numpy as np
from typing import List, Dict, Optional
import logging
from vertexai.language_models import TextEmbeddingModel
import vertexai

logger = logging.getLogger('api')


class SemanticSearch:
    """
    Búsqueda semántica de canciones usando embeddings de texto libre.
    Permite buscar por conceptos, emociones, géneros, etc.
    """
    
    def __init__(self, project_id: str, location: str, model_name: str = 'text-embedding-004'):
        """
        Inicializa el buscador semántico.
        
        Args:
            project_id: ID del proyecto GCP
            location: Ubicación de Vertex AI
            model_name: Modelo de embeddings
        """
        self.project_id = project_id
        self.location = location
        self.model_name = model_name
        
        # Inicializar Vertex AI
        vertexai.init(project=project_id, location=location)
        self.model = TextEmbeddingModel.from_pretrained(model_name)
        
        logger.info(f"✅ SemanticSearch inicializado con modelo: {model_name}")
    
    def generate_query_embedding(self, query_text: str) -> np.ndarray:
        """
        Genera embedding para un texto de búsqueda.
        
        Args:
            query_text: Texto libre de búsqueda
            
        Returns:
            Embedding como numpy array
        """
        try:
            logger.info(f"🔍 Generando embedding para: '{query_text}'")
            
            # Generar embedding usando Vertex AI
            embeddings_response = self.model.get_embeddings([query_text])
            
            # Extraer vector
            embedding = np.array(embeddings_response[0].values)
            
            logger.info(f"✅ Embedding generado: {len(embedding)} dimensiones")
            
            return embedding
            
        except Exception as e:
            logger.error(f"❌ Error generando embedding: {e}")
            raise
    
    def search_by_concept(
        self,
        query_text: str,
        embeddings_matrix: np.ndarray,
        similarity_engine,
        k: int = 10,
        min_similarity: float = 0.0
    ) -> List[tuple]:
        """
        Busca canciones por concepto semántico.
        
        Args:
            query_text: Texto de búsqueda (ej: "canciones tristes")
            embeddings_matrix: Matriz de embeddings de todas las canciones
            similarity_engine: Motor de similitud
            k: Número de resultados
            min_similarity: Similitud mínima
            
        Returns:
            Lista de tuplas (index, similarity_score)
        """
        # Generar embedding del query
        query_embedding = self.generate_query_embedding(query_text)
        
        # Buscar similares
        results = similarity_engine.find_similar(
            query_embedding=query_embedding,
            embeddings_matrix=embeddings_matrix,
            k=k,
            exclude_indices=None
        )
        
        # Filtrar por similitud mínima
        filtered_results = [
            (idx, score) for idx, score in results
            if score >= min_similarity
        ]
        
        logger.info(f"✅ Encontrados {len(filtered_results)} resultados")
        
        return filtered_results
    
    def enhance_query(self, query_text: str) -> str:
        """
        Mejora el query agregando contexto musical.
        
        Args:
            query_text: Texto original
            
        Returns:
            Texto mejorado
        """
        # Agregar contexto si el query es muy corto
        if len(query_text.split()) <= 2:
            # Detectar tipo de búsqueda
            mood_keywords = ['triste', 'alegre', 'melancólico', 'energético', 'relajado', 'romántico']
            genre_keywords = ['pop', 'rock', 'jazz', 'country', 'hip hop', 'electronic']
            
            query_lower = query_text.lower()
            
            if any(keyword in query_lower for keyword in mood_keywords):
                return f"canciones con mood {query_text}"
            elif any(keyword in query_lower for keyword in genre_keywords):
                return f"música de género {query_text}"
        
        return query_text
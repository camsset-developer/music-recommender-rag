"""
Configuración del Sistema de Recomendación
"""
import os

# ==============================================================================
# CONFIGURACIÓN GCP
# ==============================================================================
PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'music-recommender-dev')
DATASET_EMBEDDINGS = os.getenv('DATASET_EMBEDDINGS', 'embeddings')
DATASET_CLEAN = os.getenv('DATASET_CLEAN', 'clean')

# ==============================================================================
# TABLAS
# ==============================================================================
TABLE_EMBEDDINGS = f'{PROJECT_ID}.{DATASET_EMBEDDINGS}.track_embeddings_combined'
TABLE_FEATURES = f'{PROJECT_ID}.{DATASET_CLEAN}.tracks_features'

# ==============================================================================
# CONFIGURACIÓN DE RECOMENDACIÓN
# ==============================================================================
RECOMMENDATION_CONFIG = {
    # Tipo de embedding a usar para similitud
    'embedding_type': 'combined',  # 'text', 'feature', o 'combined'
    
    # Número de recomendaciones por defecto
    'default_k': 10,
    
    # Métrica de similitud
    'similarity_metric': 'cosine',  # 'cosine', 'euclidean', o 'dot'
    
    # Filtros
    'exclude_same_artist': False,  # Excluir canciones del mismo artista
    'min_similarity_threshold': 0.0,  # Umbral mínimo de similitud (0-1)
    
    # Cache
    'cache_embeddings': True,  # Cachear embeddings en memoria
    'cache_ttl': 3600  # Tiempo de vida del cache (segundos)
}

# ==============================================================================
# CONFIGURACIÓN DE BÚSQUEDA
# ==============================================================================
SEARCH_CONFIG = {
    'search_by_name': True,  # Permitir búsqueda por nombre
    'fuzzy_matching': True,  # Coincidencia difusa en nombres
    'fuzzy_threshold': 0.8,  # Umbral para fuzzy matching (0-1)
}

# ==============================================================================
# CONFIGURACIÓN DE API (OPCIONAL)
# ==============================================================================
API_CONFIG = {
    'enabled': True,
    'host': '0.0.0.0',
    'port': 8000,
    'title': 'Music Recommender API',
    'description': 'API de recomendación de música basada en embeddings',
    'version': '1.0.0'
}

# ==============================================================================
# LOGGING
# ==============================================================================
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# ==============================================================================
# CAMPOS DE RESPUESTA
# ==============================================================================
# Campos a retornar en las recomendaciones
RESPONSE_FIELDS = [
    'track_id',
    'track_name',
    'artist_name',
    'album_name',
    'popularity',
    'release_year',
    'duration_minutes',
    'spotify_url'
]
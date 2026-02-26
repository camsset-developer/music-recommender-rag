"""
Configuración del Pipeline de Generación de Embeddings
"""
import os

# ==============================================================================
# CONFIGURACIÓN GCP
# ==============================================================================
PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'music-recommender-dev')
LOCATION = os.getenv('GCP_LOCATION', 'us-central1')
DATASET_CLEAN = os.getenv('DATASET_CLEAN', 'clean')
DATASET_EMBEDDINGS = os.getenv('DATASET_EMBEDDINGS', 'embeddings')

# ==============================================================================
# TABLAS
# ==============================================================================
TABLE_FEATURES = f'{PROJECT_ID}.{DATASET_CLEAN}.tracks_features'
TABLE_EMBEDDINGS = f'{PROJECT_ID}.{DATASET_EMBEDDINGS}.track_embeddings'
TABLE_COMBINED_EMBEDDINGS = f'{PROJECT_ID}.{DATASET_EMBEDDINGS}.track_embeddings_combined'

# ==============================================================================
# VERTEX AI - EMBEDDINGS
# ==============================================================================
# Modelo de embeddings de texto
TEXT_EMBEDDING_MODEL = 'text-embedding-004'  # Modelo de Google
EMBEDDING_DIMENSION = 768  # Dimensión del embedding de texto

# Configuración de embeddings
EMBEDDING_CONFIG = {
    'text': {
        'model': TEXT_EMBEDDING_MODEL,
        'dimension': EMBEDDING_DIMENSION,
        'task_type': 'RETRIEVAL_DOCUMENT',  # Para búsqueda semántica
        'batch_size': 25,  # Procesar de 25 en 25
    },
    'features': {
        'scaler': 'standard',  # 'standard', 'minmax', or 'robust'
        'dimension': 50,  # Reducir features a 50 dimensiones con PCA
        'use_pca': True
    },
    'combination': {
        'text_weight': 0.7,  # 70% peso a embeddings de texto
        'features_weight': 0.3,  # 30% peso a features numéricas
        'normalize': True
    }
}

# ==============================================================================
# CAMPOS PARA EMBEDDINGS
# ==============================================================================
# Campos de texto para combinar en un solo string
TEXT_FIELDS = [
    'track_name',
    'artist_name',
    'album_name'
]

# Tags de Last.fm
LASTFM_TAG_FIELDS = [
    'lastfm_tag_1',
    'lastfm_tag_2', 
    'lastfm_tag_3'
]

# Features numéricas para embeddings
NUMERIC_FEATURES = [
    'popularity',
    'duration_ms',
    'explicit',
    'lastfm_playcount',
    'popularity_normalized',
    'is_popular',
    'duration_seconds',
    'is_short_track',
    'is_long_track',
    'release_year',
    'track_age_years',
    'is_recent_release',
    'lastfm_num_tags',
    'lastfm_num_similar'
]

# ==============================================================================
# VECTOR SEARCH (OPCIONAL)
# ==============================================================================
VECTOR_SEARCH_CONFIG = {
    'enabled': False,  # Cambiar a True si quieres usar Vertex AI Vector Search
    'index_name': 'music-tracks-index',
    'index_endpoint': None,  # Se configurará después
    'dimensions': EMBEDDING_DIMENSION,
    'distance_measure': 'COSINE_DISTANCE',
    'approximate_neighbors_count': 10
}

# ==============================================================================
# ALTERNATIVA: PINECONE (OPCIONAL)
# ==============================================================================
PINECONE_CONFIG = {
    'enabled': False,  # Cambiar a True si prefieres Pinecone
    'api_key': os.getenv('PINECONE_API_KEY'),
    'environment': os.getenv('PINECONE_ENVIRONMENT', 'gcp-starter'),
    'index_name': 'music-recommender',
    'dimension': EMBEDDING_DIMENSION,
    'metric': 'cosine'
}

# ==============================================================================
# LOGGING
# ==============================================================================
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# ==============================================================================
# PERFORMANCE
# ==============================================================================
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 25))
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 4))

# ==============================================================================
# VALIDACIÓN
# ==============================================================================
REQUIRED_FIELDS = [
    'track_id',
    'track_name',
    'artist_name'
]
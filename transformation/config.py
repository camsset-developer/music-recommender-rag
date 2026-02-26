"""
Configuración del Pipeline de Transformación
"""
import os
from pathlib import Path

# ==============================================================================
# CONFIGURACIÓN GCP
# ==============================================================================
PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'music-recommender-dev')
DATASET_RAW = os.getenv('DATASET_RAW', 'raw')
DATASET_CLEAN = os.getenv('DATASET_CLEAN', 'clean')

# ==============================================================================
# TABLAS
# ==============================================================================
TABLE_RAW_TRACKS = f'{PROJECT_ID}.{DATASET_RAW}.tracks_unified'
TABLE_CLEAN_TRACKS = f'{PROJECT_ID}.{DATASET_CLEAN}.tracks_clean'
TABLE_FEATURES = f'{PROJECT_ID}.{DATASET_CLEAN}.tracks_features'

# ==============================================================================
# LIMPIEZA DE DATOS
# ==============================================================================
CLEANER_CONFIG = {
    'text': {
        'lowercase': True,
        'remove_special_chars': True,
        'remove_extra_spaces': True,
        'min_length': 1,
        'max_length': 500
    },
    'numeric': {
        'fill_missing_with_median': True,
        'remove_outliers': True,
        'outlier_std_threshold': 3.0
    },
    'dates': {
        'default_format': '%Y-%m-%d',
        'extract_year': True,
        'extract_month': True
    }
}

# ==============================================================================
# FEATURE ENGINEERING
# ==============================================================================
FEATURE_CONFIG = {
    'text_features': {
        'generate_word_count': True,
        'generate_char_count': True,
        'generate_avg_word_length': True,
        'combine_text_fields': ['track_name', 'artist_name', 'album_name']
    },
    'music_features': {
        'normalize_popularity': True,
        'create_popularity_bins': True,
        'popularity_bins': [0, 20, 40, 60, 80, 100],
        'create_era_from_year': True,
        'eras': {
            '2020s': (2020, 2030),
            '2010s': (2010, 2019),
            '2000s': (2000, 2009),
            '90s': (1990, 1999),
            'older': (0, 1989)
        }
    },
    'lastfm_features': {
        'normalize_playcount': True,
        'extract_top_tags': True,
        'max_tags': 10,
        'min_tag_weight': 0.0
    }
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
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 4))

# ==============================================================================
# VALIDACIÓN
# ==============================================================================
REQUIRED_FIELDS = [
    'track_id',
    'track_name',
    'artist_name'
]

OPTIONAL_FIELDS = [
    'album_name',
    'release_date',
    'popularity',
    'duration_ms',
    'explicit'
]

# ==============================================================================
# CALIDAD DE DATOS
# ==============================================================================
QUALITY_THRESHOLDS = {
    'min_completeness': 0.7,  # 70% de campos completos
    'max_duplicates': 0.05,   # Máximo 5% duplicados
    'min_valid_dates': 0.9     # 90% fechas válidas
}
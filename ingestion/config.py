"""
Configuración centralizada del sistema de ingesta
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional

# Carga variables de ambiente
load_dotenv()

class Config:
    """Configuración global del proyecto"""
    
    # ==========================================
    # API CREDENTIALS
    # ==========================================
    SPOTIFY_CLIENT_ID: str = os.getenv('SPOTIFY_CLIENT_ID', '')
    SPOTIFY_CLIENT_SECRET: str = os.getenv('SPOTIFY_CLIENT_SECRET', '')
    GENIUS_API_TOKEN: str = os.getenv('GENIUS_API_TOKEN', '')
    LASTFM_API_KEY: str = os.getenv('LASTFM_API_KEY', '')
    
    # ==========================================
    # GCP CONFIGURATION
    # ==========================================
    GCP_PROJECT_ID: str = os.getenv('GCP_PROJECT_ID', '')
    GCP_LOCATION: str = os.getenv('GCP_LOCATION', 'us-central1')
    BUCKET_NAME: str = os.getenv('BUCKET_NAME', '')
    
    # BigQuery
    BQ_DATASET_RAW: str = os.getenv('BQ_DATASET_RAW', 'raw')
    BQ_DATASET_CLEAN: str = os.getenv('BQ_DATASET_CLEAN', 'clean')
    
    # Tablas
    BQ_TABLE_SPOTIFY: str = 'spotify_tracks'
    BQ_TABLE_GENIUS: str = 'genius_lyrics'
    BQ_TABLE_LASTFM: str = 'lastfm_tags'
    BQ_TABLE_UNIFIED: str = 'tracks_unified'
    
    # ==========================================
    # INGESTION SETTINGS
    # ==========================================
    ENVIRONMENT: str = os.getenv('ENVIRONMENT', 'development')
    MAX_TRACKS_PER_RUN: int = int(os.getenv('MAX_TRACKS_PER_RUN', 500))
    ENABLE_GENIUS: bool = os.getenv('ENABLE_GENIUS', 'true').lower() == 'true'
    ENABLE_LASTFM: bool = os.getenv('ENABLE_LASTFM', 'true').lower() == 'true'
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    
    # Rate limiting
    REQUESTS_PER_SECOND: float = float(os.getenv('REQUESTS_PER_SECOND', 2))
    RETRY_MAX_ATTEMPTS: int = int(os.getenv('RETRY_MAX_ATTEMPTS', 3))
    RETRY_BACKOFF_SECONDS: int = int(os.getenv('RETRY_BACKOFF_SECONDS', 5))
    
    # ==========================================
    # PATHS
    # ==========================================
    ROOT_DIR: Path = Path(__file__).parent.parent
    LOGS_DIR: Path = ROOT_DIR / 'logs'
    
    @classmethod
    def validate(cls) -> None:
        """
        Valida que todas las configuraciones necesarias estén presentes
        
        Raises:
            ValueError: Si faltan configuraciones críticas
        """
        required_configs = {
            'SPOTIFY_CLIENT_ID': cls.SPOTIFY_CLIENT_ID,
            'SPOTIFY_CLIENT_SECRET': cls.SPOTIFY_CLIENT_SECRET,
            'GCP_PROJECT_ID': cls.GCP_PROJECT_ID,
        }
        
        missing = [key for key, value in required_configs.items() if not value]
        
        if missing:
            raise ValueError(
                f"Configuraciones faltantes: {', '.join(missing)}\n"
                f"Asegúrate de tener un archivo .env con todas las variables necesarias"
            )
        
        # Validaciones opcionales con warnings
        optional_configs = {
            'GENIUS_API_TOKEN': cls.GENIUS_API_TOKEN,
            'LASTFM_API_KEY': cls.LASTFM_API_KEY,
        }
        
        missing_optional = [key for key, value in optional_configs.items() if not value]
        if missing_optional:
            print(f"⚠️  Configuraciones opcionales faltantes: {', '.join(missing_optional)}")
            print("   Las funcionalidades correspondientes estarán deshabilitadas")
    
    @classmethod
    def get_bigquery_table_id(cls, table_name: str, dataset: str = None) -> str:
        """
        Retorna el ID completo de una tabla de BigQuery
        
        Args:
            table_name: Nombre de la tabla
            dataset: Dataset (por defecto usa BQ_DATASET_RAW)
        
        Returns:
            str: ID completo en formato project.dataset.table
        """
        dataset = dataset or cls.BQ_DATASET_RAW
        return f"{cls.GCP_PROJECT_ID}.{dataset}.{table_name}"
    
    @classmethod
    def get_gcs_path(cls, prefix: str, filename: str) -> str:
        """
        Retorna path completo para Cloud Storage
        
        Args:
            prefix: Prefijo/carpeta (ej: 'spotify', 'genius')
            filename: Nombre del archivo
        
        Returns:
            str: Path completo
        """
        return f"{prefix}/{filename}"
    
    @classmethod
    def print_config(cls) -> None:
        """Imprime configuración actual (sin mostrar secretos)"""
        print("\n" + "="*60)
        print("CONFIGURACIÓN DEL SISTEMA")
        print("="*60)
        print(f"Proyecto GCP:        {cls.GCP_PROJECT_ID}")
        print(f"Bucket:              {cls.BUCKET_NAME}")
        print(f"Dataset RAW:         {cls.BQ_DATASET_RAW}")
        print(f"Dataset CLEAN:       {cls.BQ_DATASET_CLEAN}")
        print(f"Ambiente:            {cls.ENVIRONMENT}")
        print(f"Max tracks/run:      {cls.MAX_TRACKS_PER_RUN}")
        print(f"Genius habilitado:   {cls.ENABLE_GENIUS}")
        print(f"Last.fm habilitado:  {cls.ENABLE_LASTFM}")
        print(f"Log level:           {cls.LOG_LEVEL}")
        print("="*60 + "\n")


# Validar configuración al importar
try:
    Config.validate()
except ValueError as e:
    print(f"\n❌ Error de configuración:\n{e}\n")
    raise
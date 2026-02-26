"""
Gestor para Cloud Storage (GCS)
"""
from google.cloud import storage
from typing import List, Dict
import json
from datetime import datetime
from pathlib import Path

from config import Config
from utils.logger import logger


class GCSManager:
    """Gestiona operaciones con Google Cloud Storage"""
    
    def __init__(self):
        """Inicializa el cliente de Cloud Storage"""
        try:
            self.client = storage.Client(project=Config.GCP_PROJECT_ID)
            self.bucket_name = Config.BUCKET_NAME
            logger.info(f"✅ GCS Manager inicializado para bucket: {self.bucket_name}")
        except Exception as e:
            logger.error(f"❌ Error inicializando GCS Manager: {e}")
            raise
    
    def create_bucket_if_not_exists(self) -> bool:
        """
        Crea el bucket si no existe
        
        Returns:
            bool: True si se creó o ya existía, False si hubo error
        """
        try:
            # Intenta obtener el bucket
            bucket = self.client.bucket(self.bucket_name)
            
            if bucket.exists():
                logger.info(f"ℹ️  Bucket ya existe: {self.bucket_name}")
                return True
            
            # Si no existe, créalo
            bucket = self.client.create_bucket(
                self.bucket_name,
                location=Config.GCP_LOCATION
            )
            logger.info(f"✅ Bucket creado: {self.bucket_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creando/verificando bucket: {e}")
            return False
    
    def save_json(
        self, 
        data: List[Dict], 
        prefix: str = 'spotify',
        filename: str = None
    ) -> str:
        """
        Guarda datos en formato JSON en Cloud Storage
        
        Args:
            data: Lista de diccionarios con datos
            prefix: Carpeta/prefijo (ej: 'spotify', 'genius', 'lastfm')
            filename: Nombre del archivo (si no se provee, se genera automático)
        
        Returns:
            str: Path completo del archivo guardado (gs://bucket/path)
        
        Example:
            >>> gcs = GCSManager()
            >>> path = gcs.save_json(tracks, prefix='spotify')
            >>> print(path)
            gs://music-recommender-raw-data/spotify/tracks_20241027_162345.json
        """
        try:
            # Genera nombre de archivo con timestamp si no se provee
            if not filename:
                timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                filename = f"tracks_{timestamp}.json"
            
            # Path completo: prefix/filename
            blob_path = f"{prefix}/{filename}"
            
            # Obtiene el bucket
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(blob_path)
            
            # Convierte datos a JSON con formato legible
            json_data = json.dumps(data, indent=2, ensure_ascii=False)
            
            # Sube a Cloud Storage
            blob.upload_from_string(
                json_data,
                content_type='application/json'
            )
            
            # Path completo estilo GCS
            full_path = f"gs://{self.bucket_name}/{blob_path}"
            
            logger.info(f"✅ Datos guardados en GCS: {full_path}")
            logger.info(f"   📊 Registros: {len(data)}")
            logger.info(f"   📦 Tamaño: {len(json_data) / 1024:.2f} KB")
            
            return full_path
            
        except Exception as e:
            logger.error(f"❌ Error guardando en GCS: {e}")
            raise
    
    def list_files(self, prefix: str = '') -> List[str]:
        """
        Lista archivos en el bucket
        
        Args:
            prefix: Prefijo para filtrar (ej: 'spotify/')
        
        Returns:
            Lista de nombres de archivos
        """
        try:
            bucket = self.client.bucket(self.bucket_name)
            blobs = bucket.list_blobs(prefix=prefix)
            
            files = [blob.name for blob in blobs]
            logger.info(f"📂 Encontrados {len(files)} archivos con prefijo '{prefix}'")
            
            return files
            
        except Exception as e:
            logger.error(f"❌ Error listando archivos: {e}")
            return []
    
    def download_json(self, blob_path: str) -> List[Dict]:
        """
        Descarga y parsea un archivo JSON desde GCS
        
        Args:
            blob_path: Path del archivo en GCS (ej: 'spotify/tracks_20241027.json')
        
        Returns:
            Lista de diccionarios con los datos
        """
        try:
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(blob_path)
            
            # Descarga contenido
            content = blob.download_as_string()
            data = json.loads(content)
            
            logger.info(f"✅ Archivo descargado: {blob_path}")
            logger.info(f"   📊 Registros: {len(data)}")
            
            return data
            
        except Exception as e:
            logger.error(f"❌ Error descargando de GCS: {e}")
            return []
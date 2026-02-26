"""
Módulos de almacenamiento
"""
from .gcs_manager import GCSManager
from .bigquery_loader import BigQueryLoader

__all__ = ['GCSManager', 'BigQueryLoader']
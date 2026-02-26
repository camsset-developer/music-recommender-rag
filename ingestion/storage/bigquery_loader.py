"""
Cargador de datos a BigQuery
"""
from google.cloud import bigquery
from typing import List, Dict
from datetime import datetime

from config import Config
from utils.logger import logger


class BigQueryLoader:
    """Gestiona carga de datos a BigQuery"""
    
    def __init__(self):
        """Inicializa el cliente de BigQuery"""
        try:
            self.client = bigquery.Client(project=Config.GCP_PROJECT_ID)
            self.project_id = Config.GCP_PROJECT_ID
            logger.info("✅ BigQuery Loader inicializado")
        except Exception as e:
            logger.error(f"❌ Error inicializando BigQuery Loader: {e}")
            raise
    
    def create_dataset_if_not_exists(self, dataset_id: str = None) -> bool:
        """
        Crea un dataset en BigQuery si no existe
        
        Args:
            dataset_id: ID del dataset (por defecto usa Config.BQ_DATASET_RAW)
        
        Returns:
            bool: True si se creó o ya existía
        
        Example:
            >>> loader = BigQueryLoader()
            >>> loader.create_dataset_if_not_exists('raw')
        """
        dataset_id = dataset_id or Config.BQ_DATASET_RAW
        full_dataset_id = f"{self.project_id}.{dataset_id}"
        
        try:
            # Intenta obtener el dataset
            dataset = self.client.get_dataset(full_dataset_id)
            logger.info(f"ℹ️  Dataset ya existe: {full_dataset_id}")
            return True
            
        except Exception:
            # Si no existe, créalo
            try:
                dataset = bigquery.Dataset(full_dataset_id)
                dataset.location = "US"  # Ubicación de los datos
                
                dataset = self.client.create_dataset(dataset, exists_ok=True)
                logger.info(f"✅ Dataset creado: {full_dataset_id}")
                return True
                
            except Exception as e:
                logger.error(f"❌ Error creando dataset: {e}")
                return False
    
    def load_json_to_table(
        self,
        data: List[Dict],
        table_name: str,
        dataset_id: str = None,
        write_disposition: str = 'WRITE_APPEND'
    ) -> bool:
        """
        Carga datos JSON a una tabla de BigQuery
        
        Args:
            data: Lista de diccionarios con datos
            table_name: Nombre de la tabla
            dataset_id: ID del dataset (por defecto Config.BQ_DATASET_RAW)
            write_disposition: 'WRITE_APPEND' (agregar) o 'WRITE_TRUNCATE' (reemplazar)
        
        Returns:
            bool: True si la carga fue exitosa
        
        Example:
            >>> loader = BigQueryLoader()
            >>> tracks = [{'track_id': '123', 'name': 'Song', ...}]
            >>> loader.load_json_to_table(tracks, 'spotify_tracks')
        """
        dataset_id = dataset_id or Config.BQ_DATASET_RAW
        table_id = f"{self.project_id}.{dataset_id}.{table_name}"
        
        try:
            logger.info(f"📤 Cargando {len(data)} registros a BigQuery...")
            logger.info(f"   Tabla destino: {table_id}")
            
            # Agrega timestamp de carga si no existe
            for record in data:
                if 'loaded_at' not in record:
                    record['loaded_at'] = datetime.utcnow().isoformat()
            
            # Configuración de carga
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=write_disposition,
                autodetect=True,  # Auto-detecta schema
                create_disposition='CREATE_IF_NEEDED'  # Crea tabla si no existe
            )
            
            # Carga datos
            load_job = self.client.load_table_from_json(
                data,
                table_id,
                job_config=job_config
            )
            
            # Espera a que termine
            load_job.result()
            
            # Verifica resultado
            table = self.client.get_table(table_id)
            
            logger.info(f"✅ Datos cargados a BigQuery exitosamente")
            logger.info(f"   📊 Registros cargados: {len(data)}")
            logger.info(f"   📈 Total en tabla: {table.num_rows:,}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error cargando a BigQuery: {e}")
            return False
    
    def query(self, sql: str) -> List[Dict]:
        """
        Ejecuta una query en BigQuery
        
        Args:
            sql: Query SQL a ejecutar
        
        Returns:
            Lista de diccionarios con resultados
        
        Example:
            >>> loader = BigQueryLoader()
            >>> results = loader.query("SELECT * FROM `project.raw.spotify_tracks` LIMIT 5")
            >>> for row in results:
            >>>     print(row['name'], row['artist'])
        """
        try:
            logger.info(f"🔍 Ejecutando query...")
            
            query_job = self.client.query(sql)
            results = query_job.result()
            
            # Convierte a lista de diccionarios
            data = [dict(row) for row in results]
            
            logger.info(f"✅ Query ejecutada: {len(data)} resultados")
            return data
            
        except Exception as e:
            logger.error(f"❌ Error ejecutando query: {e}")
            return []
    
    def get_table_info(self, table_name: str, dataset_id: str = None) -> Dict:
        """
        Obtiene información de una tabla
        
        Args:
            table_name: Nombre de la tabla
            dataset_id: ID del dataset
        
        Returns:
            Dict con información de la tabla
        """
        dataset_id = dataset_id or Config.BQ_DATASET_RAW
        table_id = f"{self.project_id}.{dataset_id}.{table_name}"
        
        try:
            table = self.client.get_table(table_id)
            
            info = {
                'table_id': table.table_id,
                'num_rows': table.num_rows,
                'size_mb': table.num_bytes / (1024 * 1024),
                'created': table.created,
                'modified': table.modified,
                'schema': [field.name for field in table.schema]
            }
            
            logger.info(f"📊 Info de tabla {table_name}:")
            logger.info(f"   Registros: {info['num_rows']:,}")
            logger.info(f"   Tamaño: {info['size_mb']:.2f} MB")
            
            return info
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo info de tabla: {e}")
            return {}
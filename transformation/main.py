"""
Pipeline de Transformación y Limpieza de Datos
Music Recommender System

Este script orquesta todo el proceso de transformación:
1. Extracción de datos raw desde BigQuery
2. Limpieza de datos (texto y numéricos)
3. Feature engineering
4. Validación de calidad
5. Carga a BigQuery (dataset clean)
"""

import sys
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd
from google.cloud import bigquery

# Importar configuración
# Importar configuración
sys.path.append(str(Path(__file__).parent))
import config

# Importar cleaners
from cleaners.text_cleaner import TextCleaner
from cleaners.numeric_cleaner import NumericCleaner

# Importar feature generators
from feature_engineering.music_features import MusicFeatureGenerator
# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT,
    datefmt=config.LOG_DATE_FORMAT
)
logger = logging.getLogger('transformation')


class TransformationPipeline:
    """
    Pipeline completo de transformación de datos musicales.
    """
    
    def __init__(self):
        """Inicializa el pipeline de transformación."""
        self.client = bigquery.Client(project=config.PROJECT_ID)
        
        # Inicializar cleaners
        self.text_cleaner = TextCleaner(config.CLEANER_CONFIG['text'])
        self.numeric_cleaner = NumericCleaner(config.CLEANER_CONFIG['numeric'])
        
        # Inicializar feature generators
        self.music_features = MusicFeatureGenerator(config.FEATURE_CONFIG['music_features'])
        
        logger.info("✅ Pipeline de transformación inicializado")
    
    def run(self):
        """Ejecuta el pipeline completo."""
        try:
            logger.info("\n" + "="*70)
            logger.info("🚀 INICIANDO PIPELINE DE TRANSFORMACIÓN")
            logger.info("="*70)
            
            # Paso 1: Extraer datos raw
            df_raw = self._extract_raw_data()
            if df_raw is None or df_raw.empty:
                logger.error("❌ No hay datos para procesar")
                return False
            
            logger.info(f"\n📊 Datos raw extraídos: {len(df_raw)} registros")
            
            # Paso 2: Limpieza de datos
            df_clean = self._clean_data(df_raw)
            logger.info(f"📊 Datos limpios: {len(df_clean)} registros")
            
            # Paso 3: Feature engineering
            df_features = self._generate_features(df_clean)
            logger.info(f"📊 Features generados: {len(df_features.columns)} columnas")
            
            # Paso 4: Validación de calidad
            if not self._validate_quality(df_features):
                logger.warning("⚠️  Los datos no cumplen con los estándares de calidad")
            
            # Paso 5: Guardar datos limpios
            self._save_clean_data(df_clean)
            
            # Paso 6: Guardar features
            self._save_features(df_features)
            
            # Resumen final
            self._print_summary(df_raw, df_clean, df_features)
            
            logger.info("\n" + "="*70)
            logger.info("✅ PIPELINE DE TRANSFORMACIÓN COMPLETADO")
            logger.info("="*70)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en el pipeline: {e}", exc_info=True)
            return False

    def _extract_raw_data(self) -> pd.DataFrame:
        """
        Extrae datos raw desde BigQuery.
        
        Returns:
            DataFrame con datos raw
        """
        logger.info("\n" + "="*70)
        logger.info("PASO 1: EXTRACCIÓN DE DATOS RAW")
        logger.info("="*70)
        
        query = f"""
        SELECT *
        FROM `{config.TABLE_RAW_TRACKS}`
        """
        
        logger.info(f"📥 Extrayendo datos de: {config.TABLE_RAW_TRACKS}")
        
        try:
            df = self.client.query(query).to_dataframe()
            logger.info(f"✅ Datos extraídos: {len(df)} registros, {len(df.columns)} columnas")
            
            # AGREGAR ESTAS LÍNEAS PARA RENOMBRAR COLUMNAS
            df = self._rename_columns(df)
            logger.info(f"✅ Columnas renombradas")
            
            return df
        except Exception as e:
            logger.error(f"❌ Error extrayendo datos: {e}")
            return None

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Renombra columnas a nombres estándar.
        
        Args:
            df: DataFrame con nombres originales
            
        Returns:
            DataFrame con nombres estándar
        """
        rename_map = {
            'name': 'track_name',
            'artist': 'artist_name',
            'album': 'album_name'
        }
        
        return df.rename(columns=rename_map)




    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia los datos usando los cleaners.
        
        Args:
            df: DataFrame raw
            
        Returns:
            DataFrame limpio
        """
        logger.info("\n" + "="*70)
        logger.info("PASO 2: LIMPIEZA DE DATOS")
        logger.info("="*70)
        
        # Limpieza de texto
        df = self.text_cleaner.clean(df)
        
        # Limpieza numérica
        df = self.numeric_cleaner.clean(df)
        
        return df
    
    def _generate_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Genera features para el modelo.
        
        Args:
            df: DataFrame limpio
            
        Returns:
            DataFrame con features
        """
        logger.info("\n" + "="*70)
        logger.info("PASO 3: GENERACIÓN DE FEATURES")
        logger.info("="*70)
        
        df = self.music_features.generate_features(df)
        
        return df
    
    def _validate_quality(self, df: pd.DataFrame) -> bool:
        """
        Valida la calidad de los datos procesados.
        
        Args:
            df: DataFrame a validar
            
        Returns:
            True si pasa las validaciones
        """
        logger.info("\n" + "="*70)
        logger.info("PASO 4: VALIDACIÓN DE CALIDAD")
        logger.info("="*70)
        
        passed = True
        
        # 1. Verificar campos requeridos
        missing_fields = [f for f in config.REQUIRED_FIELDS if f not in df.columns]
        if missing_fields:
            logger.error(f"❌ Campos requeridos faltantes: {missing_fields}")
            passed = False
        else:
            logger.info("✅ Todos los campos requeridos presentes")
        
        # 2. Verificar completitud
        for field in config.REQUIRED_FIELDS:
            if field in df.columns:
                completeness = 1 - (df[field].isna().sum() / len(df))
                if completeness < config.QUALITY_THRESHOLDS['min_completeness']:
                    logger.warning(f"⚠️  Campo {field}: completitud {completeness:.2%} < {config.QUALITY_THRESHOLDS['min_completeness']:.2%}")
                    passed = False
                else:
                    logger.info(f"✅ Campo {field}: completitud {completeness:.2%}")
        
        # 3. Verificar duplicados
        duplicates = df.duplicated(subset=['track_id']).sum()
        dup_rate = duplicates / len(df)
        if dup_rate > config.QUALITY_THRESHOLDS['max_duplicates']:
            logger.warning(f"⚠️  Tasa de duplicados {dup_rate:.2%} > {config.QUALITY_THRESHOLDS['max_duplicates']:.2%}")
            passed = False
        else:
            logger.info(f"✅ Tasa de duplicados: {dup_rate:.2%}")
        
        return passed
    
    def _save_clean_data(self, df: pd.DataFrame):
        """
        Guarda datos limpios en BigQuery.
        
        Args:
            df: DataFrame limpio
        """
        logger.info("\n" + "="*70)
        logger.info("PASO 5: GUARDANDO DATOS LIMPIOS")
        logger.info("="*70)
        
        # Asegurar que el dataset existe
        self._ensure_dataset_exists(config.DATASET_CLEAN)
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        
        logger.info(f"📤 Cargando {len(df)} registros a {config.TABLE_CLEAN_TRACKS}")
        
        try:
            job = self.client.load_table_from_dataframe(
                df, config.TABLE_CLEAN_TRACKS, job_config=job_config
            )
            job.result()
            
            logger.info(f"✅ Datos limpios guardados exitosamente")
            logger.info(f"   Tabla: {config.TABLE_CLEAN_TRACKS}")
            logger.info(f"   Registros: {len(df)}")
            
        except Exception as e:
            logger.error(f"❌ Error guardando datos limpios: {e}")
    
    def _save_features(self, df: pd.DataFrame):
        """
        Guarda features en BigQuery.
        
        Args:
            df: DataFrame con features
        """
        logger.info("\n" + "="*70)
        logger.info("PASO 6: GUARDANDO FEATURES")
        logger.info("="*70)
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        
        logger.info(f"📤 Cargando {len(df)} registros a {config.TABLE_FEATURES}")
        
        try:
            job = self.client.load_table_from_dataframe(
                df, config.TABLE_FEATURES, job_config=job_config
            )
            job.result()
            
            logger.info(f"✅ Features guardados exitosamente")
            logger.info(f"   Tabla: {config.TABLE_FEATURES}")
            logger.info(f"   Registros: {len(df)}")
            logger.info(f"   Features: {len(df.columns)}")
            
        except Exception as e:
            logger.error(f"❌ Error guardando features: {e}")
    
    def _ensure_dataset_exists(self, dataset_id: str):
        """
        Asegura que un dataset exista en BigQuery.
        
        Args:
            dataset_id: ID del dataset
        """
        dataset_ref = f"{config.PROJECT_ID}.{dataset_id}"
        
        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"ℹ️  Dataset ya existe: {dataset_ref}")
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset)
            logger.info(f"✅ Dataset creado: {dataset_ref}")
    
    def _print_summary(self, df_raw: pd.DataFrame, df_clean: pd.DataFrame, df_features: pd.DataFrame):
        """
        Imprime resumen del pipeline.
        
        Args:
            df_raw: DataFrame raw
            df_clean: DataFrame limpio
            df_features: DataFrame con features
        """
        logger.info("\n" + "="*70)
        logger.info("📊 RESUMEN DEL PIPELINE")
        logger.info("="*70)
        
        logger.info(f"\n📥 Datos de entrada (RAW):")
        logger.info(f"   Registros: {len(df_raw)}")
        logger.info(f"   Columnas: {len(df_raw.columns)}")
        
        logger.info(f"\n🧹 Datos limpios (CLEAN):")
        logger.info(f"   Registros: {len(df_clean)}")
        logger.info(f"   Registros removidos: {len(df_raw) - len(df_clean)}")
        logger.info(f"   Tasa de retención: {len(df_clean)/len(df_raw):.2%}")
        
        logger.info(f"\n🎵 Features generados:")
        logger.info(f"   Total columnas: {len(df_features.columns)}")
        logger.info(f"   Features nuevos: {len(df_features.columns) - len(df_raw.columns)}")
        
        logger.info(f"\n💾 Datos guardados en:")
        logger.info(f"   {config.TABLE_CLEAN_TRACKS}")
        logger.info(f"   {config.TABLE_FEATURES}")
        
        logger.info(f"\n🔗 Consulta tus datos en:")
        logger.info(f"   https://console.cloud.google.com/bigquery?project={config.PROJECT_ID}")


def main():
    """Función principal."""
    pipeline = TransformationPipeline()
    success = pipeline.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
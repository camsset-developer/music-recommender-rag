"""
Pipeline de Generación de Embeddings
Music Recommender System

Este script orquesta la generación de embeddings:
1. Extracción de datos transformados desde BigQuery
2. Generación de embeddings de texto (Vertex AI)
3. Generación de embeddings de features numéricas
4. Combinación de ambos tipos de embeddings
5. Almacenamiento en BigQuery
"""

import sys
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from google.cloud import bigquery

# Importar configuración
sys.path.append(str(Path(__file__).parent))
import config

# Importar generadores
from generators.text_embeddings import TextEmbeddingsGenerator
from generators.feature_embeddings import FeatureEmbeddingsGenerator
from embedding_combiner import EmbeddingCombiner
# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT,
    datefmt=config.LOG_DATE_FORMAT
)
logger = logging.getLogger('embeddings')


class EmbeddingsPipeline:
    """
    Pipeline completo de generación de embeddings.
    """
    
    def __init__(self):
        """Inicializa el pipeline de embeddings."""
        self.client = bigquery.Client(project=config.PROJECT_ID)
        
        # Inicializar generadores
        self.text_generator = TextEmbeddingsGenerator(
            config=config.EMBEDDING_CONFIG['text'],
            project_id=config.PROJECT_ID,
            location=config.LOCATION
        )
        
        self.feature_generator = FeatureEmbeddingsGenerator(
            config=config.EMBEDDING_CONFIG['features']
        )
        
        self.combiner = EmbeddingCombiner(
            config=config.EMBEDDING_CONFIG['combination']
        )
        
        logger.info("✅ Pipeline de embeddings inicializado")
    
    def run(self):
        """Ejecuta el pipeline completo."""
        try:
            logger.info("\n" + "="*70)
            logger.info("🚀 INICIANDO PIPELINE DE EMBEDDINGS")
            logger.info("="*70)
            
            # Paso 1: Extraer datos transformados
            df_features = self._extract_features()
            if df_features is None or df_features.empty:
                logger.error("❌ No hay datos para procesar")
                return False
            
            logger.info(f"\n📊 Datos extraídos: {len(df_features)} registros")
            
            # Paso 2: Generar embeddings de texto
            df_text = self._generate_text_embeddings(df_features)
            
            # Paso 3: Generar embeddings de features
            df_all = self._generate_feature_embeddings(df_text)
            
            # Paso 4: Combinar embeddings
            df_combined = self._combine_embeddings(df_all)
            
            # Paso 5: Guardar embeddings en BigQuery
            self._save_embeddings(df_combined)
            
            # Resumen final
            self._print_summary(df_features, df_combined)
            
            logger.info("\n" + "="*70)
            logger.info("✅ PIPELINE DE EMBEDDINGS COMPLETADO")
            logger.info("="*70)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en el pipeline: {e}", exc_info=True)
            return False
    
    def _extract_features(self) -> pd.DataFrame:
        """
        Extrae datos transformados desde BigQuery.
        
        Returns:
            DataFrame con features
        """
        logger.info("\n" + "="*70)
        logger.info("PASO 1: EXTRACCIÓN DE FEATURES")
        logger.info("="*70)
        
        query = f"""
        SELECT *
        FROM `{config.TABLE_FEATURES}`
        """
        
        logger.info(f"📥 Extrayendo datos de: {config.TABLE_FEATURES}")
        
        try:
            df = self.client.query(query).to_dataframe()
            logger.info(f"✅ Datos extraídos: {len(df)} registros, {len(df.columns)} columnas")
            return df
        except Exception as e:
            logger.error(f"❌ Error extrayendo datos: {e}")
            return None
    
    def _generate_text_embeddings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Genera embeddings de texto.
        
        Args:
            df: DataFrame con datos
            
        Returns:
            DataFrame con embeddings de texto
        """
        logger.info("\n" + "="*70)
        logger.info("PASO 2: GENERACIÓN DE EMBEDDINGS DE TEXTO")
        logger.info("="*70)
        
        df_text = self.text_generator.generate_embeddings(
            df=df,
            text_fields=config.TEXT_FIELDS,
            tag_fields=config.LASTFM_TAG_FIELDS
        )
        
        stats = self.text_generator.get_stats()
        logger.info(f"\n📊 Estadísticas:")
        logger.info(f"   Textos procesados: {stats['texts_processed']}")
        logger.info(f"   Embeddings generados: {stats['embeddings_generated']}")
        logger.info(f"   Errores: {stats['errors']}")
        
        return df_text
    
    def _generate_feature_embeddings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Genera embeddings de features numéricas.
        
        Args:
            df: DataFrame con datos
            
        Returns:
            DataFrame con embeddings de features
        """
        logger.info("\n" + "="*70)
        logger.info("PASO 3: GENERACIÓN DE EMBEDDINGS DE FEATURES")
        logger.info("="*70)
        
        df_features = self.feature_generator.generate_embeddings(
            df=df,
            numeric_features=config.NUMERIC_FEATURES
        )
        
        stats = self.feature_generator.get_stats()
        logger.info(f"\n📊 Estadísticas:")
        logger.info(f"   Features procesadas: {stats['features_processed']}")
        logger.info(f"   Dimensión original: {stats['original_dimension']}")
        logger.info(f"   Dimensión final: {stats['final_dimension']}")
        if stats['variance_explained'] > 0:
            logger.info(f"   Varianza explicada: {stats['variance_explained']:.2%}")
        
        return df_features
    
    def _combine_embeddings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Combina embeddings de texto y features.
        
        Args:
            df: DataFrame con ambos embeddings
            
        Returns:
            DataFrame con embeddings combinados
        """
        logger.info("\n" + "="*70)
        logger.info("PASO 4: COMBINACIÓN DE EMBEDDINGS")
        logger.info("="*70)
        
        df_combined = self.combiner.combine_embeddings(df)
        
        return df_combined
    
    def _save_embeddings(self, df: pd.DataFrame):
        """
        Guarda embeddings en BigQuery.
        
        Args:
            df: DataFrame con embeddings
        """
        logger.info("\n" + "="*70)
        logger.info("PASO 5: GUARDANDO EMBEDDINGS")
        logger.info("="*70)
        
        # Asegurar que el dataset existe
        self._ensure_dataset_exists(config.DATASET_EMBEDDINGS)
        
        # Preparar datos para BigQuery
        df_to_save = self._prepare_for_bigquery(df)
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        
        logger.info(f"📤 Cargando {len(df_to_save)} registros a {config.TABLE_COMBINED_EMBEDDINGS}")
        
        try:
            job = self.client.load_table_from_dataframe(
                df_to_save, config.TABLE_COMBINED_EMBEDDINGS, job_config=job_config
            )
            job.result()
            
            logger.info(f"✅ Embeddings guardados exitosamente")
            logger.info(f"   Tabla: {config.TABLE_COMBINED_EMBEDDINGS}")
            logger.info(f"   Registros: {len(df_to_save)}")
            
        except Exception as e:
            logger.error(f"❌ Error guardando embeddings: {e}")
    
    def _prepare_for_bigquery(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepara el DataFrame para BigQuery convirtiendo arrays a listas.
        
        Args:
            df: DataFrame con embeddings
            
        Returns:
            DataFrame preparado
        """
        df_prep = df.copy()
        
        # Convertir embeddings numpy a listas
        embedding_cols = ['text_embedding', 'feature_embedding', 'combined_embedding']
        
        for col in embedding_cols:
            if col in df_prep.columns:
                df_prep[col] = df_prep[col].apply(
                    lambda x: x.tolist() if isinstance(x, np.ndarray) else x
                )
        
        return df_prep
    
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
    
    def _print_summary(self, df_input: pd.DataFrame, df_output: pd.DataFrame):
        """
        Imprime resumen del pipeline.
        
        Args:
            df_input: DataFrame de entrada
            df_output: DataFrame de salida
        """
        logger.info("\n" + "="*70)
        logger.info("📊 RESUMEN DEL PIPELINE")
        logger.info("="*70)
        
        logger.info(f"\n📥 Datos de entrada:")
        logger.info(f"   Registros: {len(df_input)}")
        
        logger.info(f"\n🎯 Embeddings generados:")
        text_emb_count = df_output['text_embedding'].notna().sum()
        feature_emb_count = df_output['feature_embedding'].notna().sum()
        combined_emb_count = df_output['combined_embedding'].notna().sum()
        
        logger.info(f"   Embeddings de texto: {text_emb_count}")
        logger.info(f"   Embeddings de features: {feature_emb_count}")
        logger.info(f"   Embeddings combinados: {combined_emb_count}")
        
        if combined_emb_count > 0:
            sample_dim = len(df_output['combined_embedding'].iloc[0])
            logger.info(f"   Dimensión combinada: {sample_dim}")
        
        logger.info(f"\n💾 Datos guardados en:")
        logger.info(f"   {config.TABLE_COMBINED_EMBEDDINGS}")
        
        logger.info(f"\n🔗 Consulta tus datos en:")
        logger.info(f"   https://console.cloud.google.com/bigquery?project={config.PROJECT_ID}")


def main():
    """Función principal."""
    pipeline = EmbeddingsPipeline()
    success = pipeline.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
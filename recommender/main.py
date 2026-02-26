"""
Script de prueba del Sistema de Recomendación
"""
import sys
import logging
from pathlib import Path

# Importar configuración
# Importar configuración
sys.path.append(str(Path(__file__).parent))
import config
from engine.similarity_engine import SimilarityEngine
from engine.recommender import MusicRecommender
# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT,
    datefmt=config.LOG_DATE_FORMAT
)
logger = logging.getLogger('recommender')


def test_recommendations():
    """Prueba el sistema de recomendación con ejemplos."""
    
    logger.info("\n" + "="*70)
    logger.info("🎵 SISTEMA DE RECOMENDACIÓN DE MÚSICA")
    logger.info("="*70)
    
    # Inicializar motor de similitud
    similarity_engine = SimilarityEngine(
        metric=config.RECOMMENDATION_CONFIG['similarity_metric']
    )
    
    # Inicializar sistema de recomendación
    recommender = MusicRecommender(
        similarity_engine=similarity_engine,
        config=config.RECOMMENDATION_CONFIG,
        project_id=config.PROJECT_ID,
        table_embeddings=config.TABLE_EMBEDDINGS,
        table_features=config.TABLE_FEATURES
    )
    
    # Cargar datos
    logger.info("\n📥 Cargando datos...")
    df = recommender.load_data()
    
    if df.empty:
        logger.error("❌ No hay datos disponibles")
        return
    
    logger.info(f"✅ Datos cargados: {len(df)} canciones")
    
    # Mostrar algunas canciones disponibles
    logger.info("\n" + "="*70)
    logger.info("🎵 CANCIONES DISPONIBLES (primeras 5):")
    logger.info("="*70)
    
    for idx, row in df.head(5).iterrows():
        logger.info(f"{idx+1}. {row['track_name']} - {row['artist_name']}")
    
    # Ejemplo 1: Recomendaciones por track_id
    logger.info("\n" + "="*70)
    logger.info("EJEMPLO 1: Recomendaciones por Track ID")
    logger.info("="*70)
    
    first_track_id = df.iloc[0]['track_id']
    first_track_name = df.iloc[0]['track_name']
    first_artist = df.iloc[0]['artist_name']
    
    logger.info(f"\n🎵 Canción base: {first_track_name} - {first_artist}")
    logger.info(f"   Track ID: {first_track_id}")
    
    recommendations = recommender.get_recommendations(
        track_id=first_track_id,
        k=5,
        embedding_type='combined'
    )
    
    if recommendations:
        logger.info(f"\n✅ Top 5 recomendaciones:")
        for i, rec in enumerate(recommendations, 1):
            logger.info(f"{i}. {rec['track_name']} - {rec['artist_name']}")
            logger.info(f"   Similitud: {rec['similarity_score']:.4f}")
    
    # Ejemplo 2: Recomendaciones por nombre
    logger.info("\n" + "="*70)
    logger.info("EJEMPLO 2: Recomendaciones por Nombre de Canción")
    logger.info("="*70)
    
    # Usar la segunda canción para más variedad
    if len(df) > 1:
        second_track_name = df.iloc[1]['track_name']
        second_artist = df.iloc[1]['artist_name']
        
        logger.info(f"\n🎵 Buscando: {second_track_name} - {second_artist}")
        
        recommendations = recommender.get_recommendations(
            track_name=second_track_name,
            artist_name=second_artist,
            k=3,
            embedding_type='combined'
        )
        
        if recommendations:
            logger.info(f"\n✅ Top 3 recomendaciones:")
            for i, rec in enumerate(recommendations, 1):
                logger.info(f"{i}. {rec['track_name']} - {rec['artist_name']}")
                logger.info(f"   Similitud: {rec['similarity_score']:.4f}")
    
    # Ejemplo 3: Comparar diferentes tipos de embeddings
    logger.info("\n" + "="*70)
    logger.info("EJEMPLO 3: Comparación de Tipos de Embeddings")
    logger.info("="*70)
    
    test_track_id = df.iloc[0]['track_id']
    
    for emb_type in ['text', 'feature', 'combined']:
        logger.info(f"\n🔍 Usando embedding: {emb_type.upper()}")
        
        try:
            recs = recommender.get_recommendations(
                track_id=test_track_id,
                k=3,
                embedding_type=emb_type
            )
            
            if recs:
                logger.info(f"   Top 3:")
                for i, rec in enumerate(recs, 1):
                    logger.info(f"   {i}. {rec['track_name']} ({rec['similarity_score']:.4f})")
        except Exception as e:
            logger.warning(f"   ⚠️  Error con {emb_type}: {e}")
    
    # Resumen final
    logger.info("\n" + "="*70)
    logger.info("📊 RESUMEN")
    logger.info("="*70)
    logger.info(f"✅ Sistema funcionando correctamente")
    logger.info(f"📊 Canciones en el sistema: {len(df)}")
    logger.info(f"🎯 Métrica de similitud: {config.RECOMMENDATION_CONFIG['similarity_metric']}")
    logger.info(f"🔗 Datos en: {config.TABLE_EMBEDDINGS}")
    
    logger.info("\n" + "="*70)
    logger.info("✅ PRUEBAS COMPLETADAS")
    logger.info("="*70)


def interactive_mode():
    """Modo interactivo para hacer consultas."""
    
    logger.info("\n" + "="*70)
    logger.info("🎵 MODO INTERACTIVO - Sistema de Recomendación")
    logger.info("="*70)
    
    # Inicializar sistema
    similarity_engine = SimilarityEngine(
        metric=config.RECOMMENDATION_CONFIG['similarity_metric']
    )
    
    recommender = MusicRecommender(
        similarity_engine=similarity_engine,
        config=config.RECOMMENDATION_CONFIG,
        project_id=config.PROJECT_ID,
        table_embeddings=config.TABLE_EMBEDDINGS,
        table_features=config.TABLE_FEATURES
    )
    
    # Cargar datos
    df = recommender.load_data()
    
    logger.info("\n💡 Ingresa el nombre de una canción para obtener recomendaciones")
    logger.info("💡 Escribe 'exit' para salir")
    logger.info("💡 Escribe 'list' para ver canciones disponibles\n")
    
    while True:
        try:
            track_name = input("\n🎵 Canción: ").strip()
            
            if track_name.lower() == 'exit':
                logger.info("👋 ¡Hasta luego!")
                break
            
            if track_name.lower() == 'list':
                logger.info("\n📋 Canciones disponibles:")
                for idx, row in df.iterrows():
                    logger.info(f"  • {row['track_name']} - {row['artist_name']}")
                continue
            
            if not track_name:
                continue
            
            # Obtener recomendaciones
            recommendations = recommender.get_recommendations(
                track_name=track_name,
                k=5,
                embedding_type='combined'
            )
            
            if recommendations:
                logger.info(f"\n✅ Top 5 recomendaciones similares a '{track_name}':")
                for i, rec in enumerate(recommendations, 1):
                    logger.info(f"{i}. {rec['track_name']} - {rec['artist_name']}")
                    logger.info(f"   Similitud: {rec['similarity_score']:.4f}")
            else:
                logger.info(f"❌ No se encontró '{track_name}'")
                logger.info("💡 Intenta con otro nombre o usa 'list' para ver opciones")
        
        except KeyboardInterrupt:
            logger.info("\n👋 ¡Hasta luego!")
            break
        except Exception as e:
            logger.error(f"❌ Error: {e}")


def main():
    """Función principal."""
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'interactive':
        interactive_mode()
    else:
        test_recommendations()


if __name__ == "__main__":
    main()
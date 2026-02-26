"""
Music Recommender API
FastAPI application para el sistema de recomendación
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
import sys
from pathlib import Path

# Agregar path del proyecto
sys.path.append(str(Path(__file__).parent.parent))

# Importar componentes
# Importar componentes
import config
from engine.similarity_engine import SimilarityEngine
from engine.recommender import MusicRecommender
from api.endpoints import router, set_recommender# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format=config.LOG_FORMAT,
    datefmt=config.LOG_DATE_FORMAT
)
logger = logging.getLogger('api')

# ==============================================================================
# CREAR APLICACIÓN FASTAPI
# ==============================================================================
app = FastAPI(
    title=config.API_CONFIG['title'],
    description=config.API_CONFIG['description'],
    version=config.API_CONFIG['version'],
    docs_url="/docs",
    redoc_url="/redoc"
)

# ==============================================================================
# CONFIGURAR CORS (para permitir acceso desde frontend)
# ==============================================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción, especifica dominios exactos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==============================================================================
# INICIALIZAR SISTEMA DE RECOMENDACIÓN
# ==============================================================================
@app.on_event("startup")
async def startup_event():
    """Inicializa el sistema al arrancar la API."""
    logger.info("=" * 70)
    logger.info("🚀 INICIANDO MUSIC RECOMMENDER API")
    logger.info("=" * 70)
    
    try:
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
        
        # Cargar datos en memoria (cache)
        logger.info("📥 Cargando datos en memoria...")
        df = recommender.load_data()
        logger.info(f"✅ Datos cargados: {len(df)} canciones")
        
        # Establecer instancia global
        set_recommender(recommender)
        
        logger.info("=" * 70)
        logger.info("✅ API LISTA")
        logger.info(f"📊 Canciones disponibles: {len(df)}")
        logger.info(f"🔗 Documentación: http://{config.API_CONFIG['host']}:{config.API_CONFIG['port']}/docs")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"❌ Error al inicializar API: {e}", exc_info=True)
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Limpieza al cerrar la API."""
    logger.info("👋 Cerrando Music Recommender API...")


# ==============================================================================
# INCLUIR RUTAS
# ==============================================================================
app.include_router(router, prefix="", tags=["recommender"])


# ==============================================================================
# EJECUTAR API
# ==============================================================================
if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app:app",
        host=config.API_CONFIG['host'],
        port=config.API_CONFIG['port'],
        reload=True,  # Auto-reload en desarrollo
        log_level="info"
    )
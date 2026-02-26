"""
API Endpoints - Rutas y lógica de la API
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import logging
import numpy as np
from api.semantic_search import SemanticSearch

logger = logging.getLogger('api')

# Router
router = APIRouter()

# Variable global para el recommender (se inicializa en app.py)
recommender_instance = None


def set_recommender(recommender):
    """Establece la instancia del recommender."""
    global recommender_instance
    recommender_instance = recommender


@router.get("/")
async def root():
    """Endpoint raíz."""
    return {
        "message": "🎵 Music Recommender API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }


@router.get("/health")
async def health_check():
    """Health check - Estado del sistema."""
    if recommender_instance is None:
        raise HTTPException(status_code=503, detail="Recommender not initialized")
    
    try:
        cache = recommender_instance._embeddings_cache
        total_tracks = len(cache) if cache is not None else 0
        
        return {
            "status": "healthy",
            "total_tracks": total_tracks,
            "embeddings_cached": cache is not None
        }
    except Exception as e:
        logger.error(f"Health check error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tracks")
async def list_tracks(
    limit: int = Query(default=50, ge=1, le=100, description="Número de tracks a listar"),
    offset: int = Query(default=0, ge=0, description="Offset para paginación")
):
    """Lista todas las canciones disponibles."""
    if recommender_instance is None:
        raise HTTPException(status_code=503, detail="Recommender not initialized")
    
    try:
        # Cargar datos si no están en cache
        if recommender_instance._embeddings_cache is None:
            df = recommender_instance.load_data()
        else:
            df = recommender_instance._embeddings_cache
        
        # Paginar
        tracks = df[['track_id', 'track_name', 'artist_name', 'album_name']].iloc[offset:offset+limit]
        
        results = [
            {
                "track_id": row['track_id'],
                "track_name": row['track_name'],
                "artist_name": row['artist_name'],
                "album_name": row['album_name']
            }
            for _, row in tracks.iterrows()
        ]
        
        return {
            "tracks": results,
            "total": len(df),
            "limit": limit,
            "offset": offset
        }
    
    except Exception as e:
        logger.error(f"Error listing tracks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search")
async def search_tracks(
    query: str = Query(..., min_length=1, description="Término de búsqueda"),
    limit: int = Query(default=10, ge=1, le=50, description="Número máximo de resultados")
):
    """Busca canciones por nombre o artista."""
    if recommender_instance is None:
        raise HTTPException(status_code=503, detail="Recommender not initialized")
    
    try:
        # Cargar datos si no están en cache
        if recommender_instance._embeddings_cache is None:
            df = recommender_instance.load_data()
        else:
            df = recommender_instance._embeddings_cache
        
        # Buscar en nombre de canción o artista
        query_lower = query.lower()
        mask = (
            df['track_name'].str.lower().str.contains(query_lower, na=False) |
            df['artist_name'].str.lower().str.contains(query_lower, na=False)
        )
        
        results_df = df[mask].head(limit)
        
        results = [
            {
                "track_id": row['track_id'],
                "track_name": row['track_name'],
                "artist_name": row['artist_name'],
                "album_name": row['album_name']
            }
            for _, row in results_df.iterrows()
        ]
        
        return {
            "query": query,
            "results": results,
            "total": len(results)
        }
    
    except Exception as e:
        logger.error(f"Error searching tracks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recommend")
async def get_recommendations(
    track_id: Optional[str] = Query(None, description="ID de la canción"),
    track_name: Optional[str] = Query(None, description="Nombre de la canción"),
    artist_name: Optional[str] = Query(None, description="Nombre del artista"),
    k: int = Query(default=10, ge=1, le=50, description="Número de recomendaciones"),
    embedding_type: str = Query(default="combined", description="Tipo: text, feature, combined")
):
    """
    Obtiene recomendaciones para una canción.
    
    Puedes buscar por:
    - track_id: ID exacto de la canción
    - track_name + artist_name: Nombre de la canción y artista
    - track_name: Solo nombre (puede ser ambiguo)
    """
    if recommender_instance is None:
        raise HTTPException(status_code=503, detail="Recommender not initialized")
    
    # Validar que al menos un parámetro esté presente
    if not track_id and not track_name:
        raise HTTPException(
            status_code=400,
            detail="Debe proporcionar track_id o track_name"
        )
    
    # Validar embedding_type
    if embedding_type not in ['text', 'feature', 'combined']:
        raise HTTPException(
            status_code=400,
            detail="embedding_type debe ser: text, feature, o combined"
        )
    
    try:
        # Obtener recomendaciones
        recommendations = recommender_instance.get_recommendations(
            track_id=track_id,
            track_name=track_name,
            artist_name=artist_name,
            k=k,
            embedding_type=embedding_type
        )
        
        if not recommendations:
            raise HTTPException(
                status_code=404,
                detail="Canción no encontrada o sin recomendaciones disponibles"
            )
        
        # Obtener info de la canción query
        df = recommender_instance._embeddings_cache
        if track_id:
            query_track = df[df['track_id'] == track_id].iloc[0]
        else:
            # Buscar por nombre
            query_result = recommender_instance._find_track(df, None, track_name, artist_name)
            if query_result is None or query_result.empty:
                raise HTTPException(status_code=404, detail="Canción no encontrada")
            query_track = query_result.iloc[0]
        
        # Formatear response
        response = {
            "query": {
                "track_id": query_track['track_id'],
                "track_name": query_track['track_name'],
                "artist_name": query_track['artist_name'],
                "album_name": query_track['album_name']
            },
            "recommendations": recommendations,
            "total": len(recommendations),
            "embedding_type": embedding_type
        }
        
        return response
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting recommendations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/recommend")
async def post_recommendations(request: dict):
    """
    Obtiene recomendaciones (método POST).
    Permite enviar el request en el body como JSON.
    """
    return await get_recommendations(
        track_id=request.get('track_id'),
        track_name=request.get('track_name'),
        artist_name=request.get('artist_name'),
        k=request.get('k', 10),
        embedding_type=request.get('embedding_type', 'combined')
    )


@router.post("/recommend/semantic")
async def semantic_search(
    query: str = Query(..., description="Búsqueda por concepto (ej: 'canciones tristes', 'música alegre')"),
    k: int = Query(default=10, ge=1, le=50, description="Número de resultados"),
    min_similarity: float = Query(default=0.3, ge=0.0, le=1.0, description="Similitud mínima")
):
    """
    Búsqueda semántica de canciones por concepto.
    
    Ejemplos de búsqueda:
    - "canciones tristes de pop"
    - "música alegre para bailar"
    - "algo como Billie Eilish pero más relajado"
    - "rock suave de los 90s"
    - "canciones románticas"
    """
    if recommender_instance is None:
        raise HTTPException(status_code=503, detail="Recommender not initialized")
    
    try:
        # Importar SemanticSearch
        
        
        # Inicializar buscador semántico
        semantic_searcher = SemanticSearch(
            project_id="music-recommender-dev",
            location="us-central1"
        )
        
        # Cargar datos si no están en cache
        if recommender_instance._embeddings_cache is None:
            df = recommender_instance.load_data()
        else:
            df = recommender_instance._embeddings_cache
        
        # Usar embeddings de texto para búsqueda semántica
        embeddings_matrix = np.vstack(df['text_embedding'].values)
        
        # Buscar por concepto
        results = semantic_searcher.search_by_concept(
            query_text=query,
            embeddings_matrix=embeddings_matrix,
            similarity_engine=recommender_instance.similarity_engine,
            k=k,
            min_similarity=min_similarity
        )
        
        if not results:
            raise HTTPException(
                status_code=404,
                detail="No se encontraron canciones con ese concepto"
            )
        
        # Formatear resultados
        recommendations = []
        for idx, similarity_score in results:
            track = df.iloc[idx]
            recommendations.append({
                "track_id": track['track_id'],
                "track_name": track['track_name'],
                "artist_name": track['artist_name'],
                "album_name": track['album_name'],
                "similarity_score": round(similarity_score, 4)
            })
        
        return {
            "query": query,
            "results": recommendations,
            "total": len(recommendations)
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error en búsqueda semántica: {e}")
        raise HTTPException(status_code=500, detail=str(e))
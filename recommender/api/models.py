"""
API Models - Schemas de request/response usando Pydantic
"""
from pydantic import BaseModel, Field
from typing import List, Optional


class TrackInfo(BaseModel):
    """Información básica de una canción."""
    track_id: str
    track_name: str
    artist_name: str
    album_name: Optional[str] = None


class RecommendationItem(BaseModel):
    """Una recomendación individual."""
    track_id: str
    track_name: str
    artist_name: str
    album_name: Optional[str] = None
    similarity_score: float = Field(..., ge=0.0, le=1.0, description="Score de similitud (0-1)")


class RecommendationRequest(BaseModel):
    """Request para obtener recomendaciones."""
    track_id: Optional[str] = Field(None, description="ID de la canción")
    track_name: Optional[str] = Field(None, description="Nombre de la canción")
    artist_name: Optional[str] = Field(None, description="Nombre del artista (opcional)")
    k: int = Field(default=10, ge=1, le=50, description="Número de recomendaciones (1-50)")
    embedding_type: str = Field(default="combined", description="Tipo de embedding: text, feature, o combined")


class RecommendationResponse(BaseModel):
    """Response con recomendaciones."""
    query: TrackInfo
    recommendations: List[RecommendationItem]
    total: int
    embedding_type: str


class SearchRequest(BaseModel):
    """Request para buscar canciones."""
    query: str = Field(..., min_length=1, description="Término de búsqueda")
    limit: int = Field(default=10, ge=1, le=50, description="Número máximo de resultados")


class SearchResponse(BaseModel):
    """Response con resultados de búsqueda."""
    query: str
    results: List[TrackInfo]
    total: int


class HealthResponse(BaseModel):
    """Response del health check."""
    status: str
    total_tracks: int
    embeddings_cached: bool


class ErrorResponse(BaseModel):
    """Response de error."""
    error: str
    detail: Optional[str] = None


class SemanticSearchRequest(BaseModel):
    """Request para búsqueda semántica."""
    query: str = Field(..., min_length=1, description="Búsqueda por concepto (ej: 'canciones tristes', 'música alegre')")
    k: int = Field(default=10, ge=1, le=50, description="Número de resultados (1-50)")
    min_similarity: float = Field(default=0.0, ge=0.0, le=1.0, description="Similitud mínima (0-1)")


class SemanticSearchResponse(BaseModel):
    """Response de búsqueda semántica."""
    query: str
    results: List[RecommendationItem]
    total: int
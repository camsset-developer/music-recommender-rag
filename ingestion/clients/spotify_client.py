"""
Cliente para interactuar con Spotify API
"""
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from typing import List, Dict, Optional
from datetime import datetime
import time

from config import Config
from utils.logger import logger
from utils.retry import retry_on_api_error


class SpotifyClient:
    """Cliente para Spotify Web API"""
    
    def __init__(self):
        """Inicializa el cliente de Spotify"""
        try:
            auth_manager = SpotifyClientCredentials(
                client_id=Config.SPOTIFY_CLIENT_ID,
                client_secret=Config.SPOTIFY_CLIENT_SECRET
            )
            self.sp = spotipy.Spotify(auth_manager=auth_manager)
            logger.info("✅ Spotify client inicializado correctamente")
        except Exception as e:
            logger.error(f"❌ Error inicializando Spotify client: {e}")
            raise
    
    @retry_on_api_error()
    def search_tracks(
        self, 
        query: str = 'year:2024', 
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict]:
        """
        Busca tracks en Spotify con paginación automática
        
        Args:
            query: Query de búsqueda
            limit: Número total de resultados deseados
            offset: Offset inicial para paginación
        
        Returns:
            List de tracks con información completa
        """
        all_tracks = []
        max_per_request = 50  # Límite máximo de Spotify API
        
        # Calcular cuántos requests necesitamos
        num_requests = (limit + max_per_request - 1) // max_per_request
        
        for i in range(num_requests):
            current_offset = offset + (i * max_per_request)
            current_limit = min(max_per_request, limit - len(all_tracks))
            
            if current_limit <= 0:
                break
            
            logger.info(
                f"Buscando tracks en Spotify: query='{query}', "
                f"limit={current_limit}, offset={current_offset} "
                f"(Request {i+1}/{num_requests})"
            )
            
            try:
                results = self.sp.search(
                    q=query,
                    type='track',
                    limit=current_limit,
                    offset=current_offset,
                    market='US'
                )
                
                tracks_items = results['tracks']['items']
                
                for item in tracks_items:
                    track = self._parse_track(item)
                    if track:
                        all_tracks.append(track)
                    
                    # Rate limiting
                    time.sleep(1.0 / Config.REQUESTS_PER_SECOND)
                
                logger.info(f"✅ Obtenidos {len(tracks_items)} tracks en este batch")
                
                # Si no hay más resultados, salir
                if len(tracks_items) < current_limit:
                    logger.info("ℹ️  No hay más resultados disponibles")
                    break
                    
            except Exception as e:
                logger.error(f"❌ Error en request {i+1}: {e}")
                # Continuar con los siguientes requests
                continue
        
        logger.info(f"✅ Total de tracks obtenidos: {len(all_tracks)}")
        return all_tracks





    @retry_on_api_error()
    def get_audio_features(self, track_id: str) -> Optional[Dict]:
        """
        Obtiene características de audio de un track
        
        Args:
            track_id: ID del track en Spotify
        
        Returns:
            Dict con audio features o None si hay error
        """
        try:
            features = self.sp.audio_features([track_id])
            if features and features[0]:
                return features[0]
            return None
        except Exception as e:
            logger.warning(f"⚠️  No se pudieron obtener audio features para {track_id}: {e}")
            return None
    
    def _parse_track(self, track_data: Dict) -> Optional[Dict]:
        """
        Parsea y estructura datos de un track
        
        Args:
            track_data: Datos raw del track desde Spotify API
        
        Returns:
            Dict estructurado con información del track
        """
        try:
            track_id = track_data['id']
            
            # Información básica
            parsed = {
                'track_id': track_id,
                'name': track_data['name'],
                'artist': track_data['artists'][0]['name'],
                'artist_id': track_data['artists'][0]['id'],
                'album': track_data['album']['name'],
                'album_id': track_data['album']['id'],
                'release_date': track_data['album']['release_date'],
                'popularity': track_data['popularity'],
                'duration_ms': track_data['duration_ms'],
                'explicit': track_data['explicit'],
                'preview_url': track_data.get('preview_url'),
                'spotify_url': track_data['external_urls']['spotify'],
                
                # Metadata
                'isrc': track_data['external_ids'].get('isrc'),
                'track_number': track_data['track_number'],
                'disc_number': track_data['disc_number'],
            }
            
            # Obtener audio features
            audio_features = self.get_audio_features(track_id)
            if audio_features:
                parsed.update({
                    'danceability': audio_features.get('danceability'),
                    'energy': audio_features.get('energy'),
                    'key': audio_features.get('key'),
                    'loudness': audio_features.get('loudness'),
                    'mode': audio_features.get('mode'),
                    'speechiness': audio_features.get('speechiness'),
                    'acousticness': audio_features.get('acousticness'),
                    'instrumentalness': audio_features.get('instrumentalness'),
                    'liveness': audio_features.get('liveness'),
                    'valence': audio_features.get('valence'),
                    'tempo': audio_features.get('tempo'),
                    'time_signature': audio_features.get('time_signature'),
                })
            
            # Timestamp de ingesta
            parsed['ingested_at'] = datetime.utcnow().isoformat()
            parsed['source'] = 'spotify'
            
            return parsed
            
        except Exception as e:
            logger.error(f"❌ Error parseando track: {e}")
            return None
    
    def get_multiple_tracks(self, track_ids: List[str]) -> List[Dict]:
        """
        Obtiene múltiples tracks por sus IDs
        
        Args:
            track_ids: Lista de IDs de tracks
        
        Returns:
            Lista de tracks parseados
        """
        try:
            # Spotify API permite hasta 50 tracks por request
            batch_size = 50
            all_tracks = []
            
            for i in range(0, len(track_ids), batch_size):
                batch = track_ids[i:i + batch_size]
                results = self.sp.tracks(batch)
                
                for track_data in results['tracks']:
                    if track_data:
                        track = self._parse_track(track_data)
                        if track:
                            all_tracks.append(track)
                
                time.sleep(1.0 / Config.REQUESTS_PER_SECOND)
            
            return all_tracks
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo múltiples tracks: {e}")
            return []
    
    def get_top_tracks_by_genre(self, genre: str = 'pop', limit: int = 50) -> List[Dict]:
        """
        Obtiene top tracks por género
        
        Args:
            genre: Género musical
            limit: Número de tracks
        
        Returns:
            Lista de tracks
        """
        query = f"genre:{genre}"
        return self.search_tracks(query=query, limit=limit)
    
    def get_new_releases(self, limit: int = 50) -> List[Dict]:
        """
        Obtiene lanzamientos recientes
        
        Args:
            limit: Número de tracks
        
        Returns:
            Lista de tracks recientes
        """
        query = f"year:{datetime.now().year}"
        return self.search_tracks(query=query, limit=limit)
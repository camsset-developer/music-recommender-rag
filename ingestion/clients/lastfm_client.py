"""
Cliente para interactuar con Last.fm API (tags y metadata)
"""
import pylast
from typing import List, Dict, Optional
import time

from config import Config
from utils.logger import logger
from utils.retry import retry_on_api_error


class LastFMClient:
    """Cliente para Last.fm API"""
    
    def __init__(self):
        """Inicializa el cliente de Last.fm"""
        try:
            if not Config.LASTFM_API_KEY:
                logger.warning("⚠️  LASTFM_API_KEY no configurado, cliente deshabilitado")
                self.network = None
                return
            
            self.network = pylast.LastFMNetwork(
                api_key=Config.LASTFM_API_KEY
            )
            logger.info("✅ Last.fm client inicializado correctamente")
        except Exception as e:
            logger.error(f"❌ Error inicializando Last.fm client: {e}")
            self.network = None
    
    def is_enabled(self) -> bool:
        """Verifica si el cliente está habilitado"""
        return self.network is not None
    
    @retry_on_api_error()
    def get_track_tags(
        self, 
        track_name: str, 
        artist_name: str,
        limit: int = 10
    ) -> Optional[Dict]:
        """
        Obtiene tags y metadata de una canción
        
        Args:
            track_name: Nombre de la canción
            artist_name: Nombre del artista
            limit: Número máximo de tags a obtener
        
        Returns:
            Dict con tags y metadata, o None si no se encuentra
        
        Example:
            >>> lastfm = LastFMClient()
            >>> info = lastfm.get_track_tags("Mr. Brightside", "The Killers")
            >>> print(info['tags'])
            ['indie rock', 'alternative', 'energetic']
        """
        if not self.is_enabled():
            return None
        
        try:
            logger.debug(f"🔍 Buscando en Last.fm: '{track_name}' - {artist_name}")
            
            # Obtiene el track
            track = self.network.get_track(artist_name, track_name)
            
            # Tags generados por usuarios
            tags_data = []
            try:
                top_tags = track.get_top_tags(limit=limit)
                tags_data = [
                    {
                        'name': tag.item.name,
                        'weight': int(tag.weight) if tag.weight else 0,
                        'url': tag.item.url
                    }
                    for tag in top_tags
                ]
            except Exception as e:
                logger.debug(f"No se pudieron obtener tags: {e}")
            
            # Canciones similares
            similar_tracks = []
            try:
                similar = track.get_similar(limit=5)
                similar_tracks = [
                    {
                        'name': s.item.title,
                        'artist': s.item.artist.name,
                        'match_score': float(s.match) if s.match else 0
                    }
                    for s in similar
                ]
            except Exception as e:
                logger.debug(f"No se pudieron obtener similares: {e}")
            
            # Estadísticas
            try:
                playcount = track.get_playcount()
                listeners = track.get_listener_count()
            except:
                playcount = 0
                listeners = 0
            
            track_info = {
                'track_name': track.title,
                'artist_name': track.artist.name,
                'tags': [t['name'] for t in tags_data],
                'tags_detailed': tags_data,
                'similar_tracks': similar_tracks,
                'playcount': playcount,
                'listeners': listeners,
                'lastfm_url': track.get_url()
            }
            
            logger.debug(f"✅ Encontrado en Last.fm: {track_name} ({len(tags_data)} tags)")
            
            # Rate limiting
            time.sleep(1.0 / Config.REQUESTS_PER_SECOND)
            
            return track_info
            
        except pylast.WSError as e:
            logger.debug(f"⚠️  Track no encontrado en Last.fm: {track_name}")
            return None
        except Exception as e:
            logger.warning(f"⚠️  Error obteniendo datos de Last.fm para '{track_name}': {e}")
            return None
    
    def get_artist_tags(
        self, 
        artist_name: str,
        limit: int = 10
    ) -> Optional[List[Dict]]:
        """
        Obtiene tags de un artista
        
        Args:
            artist_name: Nombre del artista
            limit: Número máximo de tags
        
        Returns:
            Lista de tags o None
        """
        if not self.is_enabled():
            return None
        
        try:
            artist = self.network.get_artist(artist_name)
            top_tags = artist.get_top_tags(limit=limit)
            
            tags = [
                {
                    'name': tag.item.name,
                    'weight': int(tag.weight) if tag.weight else 0
                }
                for tag in top_tags
            ]
            
            return tags
            
        except Exception as e:
            logger.debug(f"⚠️  No se pudieron obtener tags del artista {artist_name}: {e}")
            return None
    
    def search_multiple(
        self, 
        tracks: list
    ) -> Dict[str, Dict]:
        """
        Busca tags para múltiples canciones
        
        Args:
            tracks: Lista de dicts con 'name' y 'artist'
        
        Returns:
            Dict con track_id como key y tags_info como value
        """
        if not self.is_enabled():
            logger.warning("⚠️  Last.fm client no habilitado")
            return {}
        
        results = {}
        total = len(tracks)
        
        logger.info(f"🔍 Buscando {total} canciones en Last.fm...")
        
        for i, track in enumerate(tracks, 1):
            track_id = track.get('track_id')
            track_name = track.get('name')
            artist_name = track.get('artist')
            
            if not track_name or not artist_name:
                continue
            
            if i % 10 == 0:
                logger.info(f"   Progreso: {i}/{total} canciones procesadas")
            
            tags_info = self.get_track_tags(track_name, artist_name)
            
            if tags_info:
                results[track_id] = tags_info
        
        found = len(results)
        logger.info(f"✅ Last.fm: {found}/{total} canciones encontradas ({found/total*100:.1f}%)")
        
        return results
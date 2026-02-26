"""
Cliente para interactuar con Genius API (letras y descripciones)
"""
import lyricsgenius
from typing import Dict, Optional
import time

from config import Config
from utils.logger import logger
from utils.retry import retry_on_api_error


class GeniusClient:
    """Cliente para Genius API"""
    
    def __init__(self):
        """Inicializa el cliente de Genius"""
        try:
            if not Config.GENIUS_API_TOKEN:
                logger.warning("⚠️  GENIUS_API_TOKEN no configurado, cliente deshabilitado")
                self.genius = None
                return
            
            self.genius = lyricsgenius.Genius(
                Config.GENIUS_API_TOKEN,
                verbose=False,  # Desactiva logs internos
                remove_section_headers=True,  # Limpia headers como [Verse 1]
                skip_non_songs=True,  # Solo canciones, no artículos
                timeout=15
            )
            logger.info("✅ Genius client inicializado correctamente")
        except Exception as e:
            logger.error(f"❌ Error inicializando Genius client: {e}")
            self.genius = None
    
    def is_enabled(self) -> bool:
        """Verifica si el cliente está habilitado"""
        return self.genius is not None
    
    @retry_on_api_error()
    def get_lyrics_and_info(
        self, 
        track_name: str, 
        artist_name: str
    ) -> Optional[Dict]:
        """
        Obtiene letras y información de una canción
        
        Args:
            track_name: Nombre de la canción
            artist_name: Nombre del artista
        
        Returns:
            Dict con letras e información, o None si no se encuentra
        
        Example:
            >>> genius = GeniusClient()
            >>> info = genius.get_lyrics_and_info("Mr. Brightside", "The Killers")
            >>> print(info['lyrics'][:100])
            >>> print(info['description'])
        """
        if not self.is_enabled():
            return None
        
        try:
            logger.debug(f"🔍 Buscando en Genius: '{track_name}' - {artist_name}")
            
            # Busca la canción
            song = self.genius.search_song(track_name, artist_name)
            
            if not song:
                logger.debug(f"⚠️  No se encontró en Genius: {track_name}")
                return None
            
            # Extrae información
            lyrics_info = {
                'genius_id': song.id,
                'title': song.title,
                'artist': song.artist,
                'lyrics': self._clean_lyrics(song.lyrics),
                'lyrics_length': len(song.lyrics) if song.lyrics else 0,
                'url': song.url,
                'genius_popularity': song.stats.get('pageviews', 0) if hasattr(song, 'stats') else 0,
                'release_date': getattr(song, 'release_date', None),
                'description': self._extract_description(song),
                'has_lyrics': bool(song.lyrics and len(song.lyrics) > 50)
            }
            
            logger.debug(f"✅ Encontrado en Genius: {track_name} (lyrics: {lyrics_info['lyrics_length']} chars)")
            
            # Rate limiting
            time.sleep(1.0 / Config.REQUESTS_PER_SECOND)
            
            return lyrics_info
            
        except Exception as e:
            logger.warning(f"⚠️  Error obteniendo datos de Genius para '{track_name}': {e}")
            return None
    
    def _clean_lyrics(self, lyrics: str) -> str:
        """
        Limpia las letras removiendo metadata de Genius
        
        Args:
            lyrics: Letras raw de Genius
        
        Returns:
            Letras limpias
        """
        if not lyrics:
            return ""
        
        # Remueve el sufijo de Genius (ej: "23Embed")
        lyrics = lyrics.strip()
        
        # Remueve números al final (metadata de Genius)
        if lyrics and lyrics[-1].isdigit():
            # Busca dónde terminan las letras reales
            for i in range(len(lyrics) - 1, -1, -1):
                if not lyrics[i].isdigit() and lyrics[i] not in ['E', 'm', 'b', 'e', 'd']:
                    lyrics = lyrics[:i+1]
                    break
        
        return lyrics.strip()
    
    def _extract_description(self, song) -> Optional[str]:
        """
        Extrae descripción/contexto de la canción
        
        Args:
            song: Objeto Song de lyricsgenius
        
        Returns:
            Descripción o None
        """
        try:
            if hasattr(song, 'description'):
                desc = song.description
                if isinstance(desc, dict):
                    return desc.get('plain', '')
                return str(desc)
            return None
        except:
            return None
    
    def search_multiple(
        self, 
        tracks: list
    ) -> Dict[str, Dict]:
        """
        Busca múltiples canciones en batch
        
        Args:
            tracks: Lista de dicts con 'name' y 'artist'
        
        Returns:
            Dict con track_id como key y lyrics_info como value
        
        Example:
            >>> tracks = [
            ...     {'track_id': '123', 'name': 'Song A', 'artist': 'Artist A'},
            ...     {'track_id': '456', 'name': 'Song B', 'artist': 'Artist B'}
            ... ]
            >>> results = genius.search_multiple(tracks)
            >>> print(results['123']['lyrics'])
        """
        if not self.is_enabled():
            logger.warning("⚠️  Genius client no habilitado")
            return {}
        
        results = {}
        total = len(tracks)
        
        logger.info(f"🔍 Buscando {total} canciones en Genius...")
        
        for i, track in enumerate(tracks, 1):
            track_id = track.get('track_id')
            track_name = track.get('name')
            artist_name = track.get('artist')
            
            if not track_name or not artist_name:
                continue
            
            if i % 10 == 0:
                logger.info(f"   Progreso: {i}/{total} canciones procesadas")
            
            lyrics_info = self.get_lyrics_and_info(track_name, artist_name)
            
            if lyrics_info:
                results[track_id] = lyrics_info
        
        found = len(results)
        logger.info(f"✅ Genius: {found}/{total} canciones encontradas ({found/total*100:.1f}%)")
        
        return results
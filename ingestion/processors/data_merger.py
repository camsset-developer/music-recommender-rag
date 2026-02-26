"""
Combina datos de múltiples fuentes (Spotify + Genius + Last.fm)
"""
from typing import List, Dict
from datetime import datetime

from utils.logger import logger


class DataMerger:
    """Combina datos de diferentes APIs en un formato unificado"""
    
    @staticmethod
    def merge_track_data(
        spotify_track: Dict,
        genius_data: Dict = None,
        lastfm_data: Dict = None
    ) -> Dict:
        """
        Combina datos de una canción desde múltiples fuentes
        
        Args:
            spotify_track: Datos de Spotify
            genius_data: Datos de Genius (opcional)
            lastfm_data: Datos de Last.fm (opcional)
        
        Returns:
            Dict unificado con todos los datos
        """
        # Base de Spotify
        unified = spotify_track.copy()
        
        # Agregar datos de Genius
        if genius_data:
            unified['genius'] = {
                'genius_id': genius_data.get('genius_id'),
                'lyrics': genius_data.get('lyrics'),
                'lyrics_length': genius_data.get('lyrics_length', 0),
                'description': genius_data.get('description'),
                'genius_url': genius_data.get('url'),
                'genius_popularity': genius_data.get('genius_popularity', 0),
                'has_lyrics': genius_data.get('has_lyrics', False)
            }
        else:
            unified['genius'] = None
        
        # Agregar datos de Last.fm
        if lastfm_data:
            unified['lastfm'] = {
                'tags': lastfm_data.get('tags', []),
                'tags_detailed': lastfm_data.get('tags_detailed', []),
                'similar_tracks': lastfm_data.get('similar_tracks', []),
                'playcount': lastfm_data.get('playcount', 0),
                'listeners': lastfm_data.get('listeners', 0),
                'lastfm_url': lastfm_data.get('lastfm_url')
            }
        else:
            unified['lastfm'] = None
        
        # Metadata de merge
        unified['data_sources'] = {
            'spotify': True,
            'genius': genius_data is not None,
            'lastfm': lastfm_data is not None
        }
        
        unified['merged_at'] = datetime.utcnow().isoformat()
        
        return unified
    
    @staticmethod
    def merge_multiple(
        spotify_tracks: List[Dict],
        genius_results: Dict[str, Dict],
        lastfm_results: Dict[str, Dict]
    ) -> List[Dict]:
        """
        Combina múltiples tracks
        
        Args:
            spotify_tracks: Lista de tracks de Spotify
            genius_results: Dict {track_id: genius_data}
            lastfm_results: Dict {track_id: lastfm_data}
        
        Returns:
            Lista de tracks unificados
        """
        unified_tracks = []
        
        for track in spotify_tracks:
            track_id = track['track_id']
            
            genius_data = genius_results.get(track_id)
            lastfm_data = lastfm_results.get(track_id)
            
            unified = DataMerger.merge_track_data(
                spotify_track=track,
                genius_data=genius_data,
                lastfm_data=lastfm_data
            )
            
            unified_tracks.append(unified)
        
        # Estadísticas
        total = len(unified_tracks)
        with_genius = sum(1 for t in unified_tracks if t['genius'] is not None)
        with_lastfm = sum(1 for t in unified_tracks if t['lastfm'] is not None)
        complete = sum(1 for t in unified_tracks if t['genius'] and t['lastfm'])
        
        logger.info(f"\n📊 Estadísticas de merge:")
        logger.info(f"   Total tracks: {total}")
        logger.info(f"   Con Genius: {with_genius} ({with_genius/total*100:.1f}%)")
        logger.info(f"   Con Last.fm: {with_lastfm} ({with_lastfm/total*100:.1f}%)")
        logger.info(f"   Completos (todas las fuentes): {complete} ({complete/total*100:.1f}%)")
        
        return unified_tracks
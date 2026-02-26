# ingestion/clients/__init__.py
"""
Clientes para APIs externas
"""
from .spotify_client import SpotifyClient
from .genius_client import GeniusClient
from .lastfm_client import LastFMClient

__all__ = ['SpotifyClient', 'GeniusClient', 'LastFMClient']
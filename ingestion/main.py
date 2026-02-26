"""
Script principal de ingesta de datos musicales con múltiples fuentes
"""
from config import Config
from utils.logger import logger
from clients.spotify_client import SpotifyClient
from clients.genius_client import GeniusClient
from clients.lastfm_client import LastFMClient
from storage.gcs_manager import GCSManager
from storage.bigquery_loader import BigQueryLoader
from processors.data_merger import DataMerger


def main():
    """
    Función principal de ingesta con múltiples fuentes
    """
    logger.info("🚀 Iniciando sistema de ingesta multi-fuente...")
    
    # ========================================
    # 1. VALIDAR CONFIGURACIÓN
    # ========================================
    try:
        Config.validate()
        Config.print_config()
    except ValueError as e:
        logger.error(f"❌ Error de configuración: {e}")
        return
    
    # ========================================
    # 2. INICIALIZAR CLIENTES DE APIS
    # ========================================
    
    # Cliente de Spotify (obligatorio)
    try:
        spotify = SpotifyClient()
    except Exception as e:
        logger.error(f"❌ Error inicializando Spotify: {e}")
        return
    
    # Cliente de Genius (opcional)
    genius = GeniusClient()
    
    # Cliente de Last.fm (opcional)
    lastfm = LastFMClient()
    
    # ========================================
    # 3. INICIALIZAR CLIENTES DE GCP
    # ========================================
    
    # Cloud Storage
    try:
        gcs = GCSManager()
        if not gcs.create_bucket_if_not_exists():
            logger.error("❌ No se pudo crear/verificar bucket en GCS")
            return
    except Exception as e:
        logger.error(f"❌ Error inicializando GCS: {e}")
        return
    
    # BigQuery
    try:
        bq = BigQueryLoader()
        if not bq.create_dataset_if_not_exists(Config.BQ_DATASET_RAW):
            logger.error("❌ No se pudo crear/verificar dataset en BigQuery")
            return
    except Exception as e:
        logger.error(f"❌ Error inicializando BigQuery: {e}")
        return
    

    # ========================================
    # 4. OBTENER DATOS DE SPOTIFY
    # ========================================
    import time

    logger.info("\n" + "="*60)
    logger.info("FASE 1: OBTENCIÓN DE DATOS DE SPOTIFY")
    logger.info("="*60)

    # Múltiples queries para obtener variedad
    queries = [
        'year:2024',
        'year:2023',
        'year:2022',
        'genre:pop',
        'genre:rock',
        'genre:hip-hop',
        'genre:electronic',
        'genre:r&b',
        'genre:country',
        'genre:latin',
    ]

    all_spotify_tracks = []
    tracks_per_query = Config.MAX_TRACKS_PER_RUN // len(queries)

    for query in queries:
        logger.info(f"🔍 Query: '{query}' (buscando {tracks_per_query} tracks)")
        tracks = spotify.search_tracks(query=query, limit=tracks_per_query)
        all_spotify_tracks.extend(tracks)
        logger.info(f"✅ Obtenidos {len(tracks)} tracks con query '{query}'")
        
        # Pausa entre queries
        time.sleep(1)

    # Eliminar duplicados por track_id
    seen_ids = set()
    spotify_tracks = []
    for track in all_spotify_tracks:
        if track['track_id'] not in seen_ids:
            seen_ids.add(track['track_id'])
            spotify_tracks.append(track)

    logger.info(f"✅ Total de tracks únicos obtenidos: {len(spotify_tracks)}")

    if not spotify_tracks:
        logger.warning("⚠️  No se obtuvieron tracks de Spotify")
        return




    # ========================================
    # 5. ENRIQUECER CON GENIUS (LETRAS)
    # ========================================
    logger.info("\n" + "="*60)
    logger.info("FASE 2: OBTENCIÓN DE LETRAS (GENIUS)")
    logger.info("="*60)
    
    genius_results = {}
    
    if genius.is_enabled() and Config.ENABLE_GENIUS:
        genius_results = genius.search_multiple(spotify_tracks)
    else:
        logger.info("⚠️  Genius API deshabilitada, saltando...")
    
    # ========================================
    # 6. ENRIQUECER CON LAST.FM (TAGS)
    # ========================================
    logger.info("\n" + "="*60)
    logger.info("FASE 3: OBTENCIÓN DE TAGS (LAST.FM)")
    logger.info("="*60)
    
    lastfm_results = {}
    
    if lastfm.is_enabled() and Config.ENABLE_LASTFM:
        lastfm_results = lastfm.search_multiple(spotify_tracks)
    else:
        logger.info("⚠️  Last.fm API deshabilitada, saltando...")
    
    # ========================================
    # 7. COMBINAR TODOS LOS DATOS
    # ========================================
    logger.info("\n" + "="*60)
    logger.info("FASE 4: COMBINACIÓN DE DATOS")
    logger.info("="*60)
    
    unified_tracks = DataMerger.merge_multiple(
        spotify_tracks=spotify_tracks,
        genius_results=genius_results,
        lastfm_results=lastfm_results
    )
    
    # ========================================
    # 8. GUARDAR EN CLOUD STORAGE
    # ========================================
    logger.info("\n" + "="*60)
    logger.info("FASE 5: ALMACENAMIENTO EN GCS")
    logger.info("="*60)
    
    try:
        # Guardar datos unificados
        gcs_path = gcs.save_json(
            data=unified_tracks,
            prefix='unified',
            filename=None
        )
        logger.info(f"✅ Datos unificados guardados en: {gcs_path}")
        
        # También guardar raw de Spotify (backup)
        gcs.save_json(
            data=spotify_tracks,
            prefix='spotify',
            filename=None
        )
        
    except Exception as e:
        logger.error(f"❌ Error guardando en GCS: {e}")
    
    # ========================================
    # 9. CARGAR A BIGQUERY
    # ========================================
    logger.info("\n" + "="*60)
    logger.info("FASE 6: CARGA A BIGQUERY")
    logger.info("="*60)
    
    # Tabla de datos unificados
    try:
        success = bq.load_json_to_table(
            data=unified_tracks,
            table_name=Config.BQ_TABLE_UNIFIED,
            dataset_id=Config.BQ_DATASET_RAW,
            write_disposition='WRITE_APPEND'
        )
        
        if not success:
            logger.error("❌ Falló la carga a BigQuery")
            return
            
    except Exception as e:
        logger.error(f"❌ Error cargando a BigQuery: {e}")
        return
    
    # ========================================
    # 10. VERIFICAR Y MOSTRAR RESULTADOS
    # ========================================
    logger.info("\n" + "="*60)
    logger.info("FASE 7: VERIFICACIÓN DE DATOS")
    logger.info("="*60)
    
    try:
        # Info de la tabla
        table_info = bq.get_table_info(
            table_name=Config.BQ_TABLE_UNIFIED,
            dataset_id=Config.BQ_DATASET_RAW
        )
        
        # Query de ejemplo con datos enriquecidos
        query = f"""
        SELECT 
            name,
            artist,
            popularity,
            CASE 
                WHEN genius IS NOT NULL THEN 'Sí'
                ELSE 'No'
            END as tiene_letras,
            CASE 
                WHEN lastfm IS NOT NULL THEN ARRAY_LENGTH(JSON_EXTRACT_ARRAY(lastfm, '$.tags'))
                ELSE 0
            END as num_tags
        FROM `{Config.GCP_PROJECT_ID}.{Config.BQ_DATASET_RAW}.{Config.BQ_TABLE_UNIFIED}`
        ORDER BY popularity DESC
        LIMIT 5
        """
        
        results = bq.query(query)
        
        if results:
            logger.info(f"\n📊 Top 5 tracks con datos enriquecidos:")
            logger.info(f"{'Canción':<30} {'Artista':<25} {'Pop.':<5} {'Letras':<8} {'Tags':<5}")
            logger.info("-" * 75)
            for track in results:
                logger.info(
                    f"{track['name'][:29]:<30} "
                    f"{track['artist'][:24]:<25} "
                    f"{track['popularity']:<5} "
                    f"{track['tiene_letras']:<8} "
                    f"{track['num_tags']:<5}"
                )
        
    except Exception as e:
        logger.warning(f"⚠️  No se pudo verificar datos: {e}")
    
    # ========================================
    # 11. RESUMEN FINAL
    # ========================================
    logger.info("\n" + "="*60)
    logger.info("✅ INGESTA MULTI-FUENTE COMPLETADA")
    logger.info("="*60)
    logger.info(f"📊 Tracks procesados: {len(unified_tracks)}")
    logger.info(f"🎵 Fuente: Spotify - 100%")
    logger.info(f"📝 Con letras (Genius): {len(genius_results)} ({len(genius_results)/len(unified_tracks)*100:.1f}%)")
    logger.info(f"🏷️  Con tags (Last.fm): {len(lastfm_results)} ({len(lastfm_results)/len(unified_tracks)*100:.1f}%)")
    logger.info(f"💾 Guardado en GCS: ✅")
    logger.info(f"📊 Cargado en BigQuery: ✅")
    logger.info(f"\n🔗 Consulta tus datos en:")
    logger.info(f"   https://console.cloud.google.com/bigquery?project={Config.GCP_PROJECT_ID}")
    logger.info("="*60)


if __name__ == "__main__":
    main()
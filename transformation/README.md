# 🔄 Transformation Pipeline

Pipeline de transformación y limpieza de datos para el Music Recommender System.

## 📋 Descripción

Este módulo se encarga de:
1. ✅ Extraer datos raw desde BigQuery
2. 🧹 Limpiar y normalizar datos (texto y numéricos)
3. 🎵 Generar features musicales
4. ✔️ Validar calidad de datos
5. 💾 Guardar datos procesados en BigQuery

## 📁 Estructura

```
transformation/
├── config.py                      # Configuración del pipeline
├── main.py                        # Orquestador principal
├── cleaners/
│   ├── __init__.py
│   ├── base_cleaner.py           # Clase base para cleaners
│   ├── text_cleaner.py           # Limpieza de texto
│   └── numeric_cleaner.py        # Limpieza de datos numéricos
├── feature_engineering/
│   ├── __init__.py
│   ├── base_feature.py           # Clase base para features
│   ├── text_features.py          # Features de texto
│   └── music_features.py         # Features musicales
└── requirements.txt
```

## 🚀 Uso

### 1. Instalar dependencias

```bash
cd transformation
pip install -r requirements.txt
```

### 2. Configurar variables de entorno

```bash
# .env
GCP_PROJECT_ID=music-recommender-dev
DATASET_RAW=raw
DATASET_CLEAN=clean
LOG_LEVEL=INFO
```

### 3. Ejecutar el pipeline

```bash
python main.py
```

## 📊 Proceso de Transformación

### Fase 1: Extracción
- Lee datos desde `{project}.raw.tracks_unified`
- Valida estructura de datos

### Fase 2: Limpieza
- **Texto**: Normalización, eliminación de espacios extra
- **Numéricos**: Manejo de valores faltantes, detección de outliers
- **Fechas**: Conversión y validación

### Fase 3: Feature Engineering
- **Popularidad**: Normalización, bins, clasificación
- **Duración**: Conversión a segundos/minutos, categorías
- **Temporal**: Año, década, era musical, antigüedad
- **Last.fm**: Playcount (log), tags, tracks similares

### Fase 4: Validación
- Completitud de campos requeridos
- Tasa de duplicados
- Rango de valores válidos

### Fase 5: Almacenamiento
- Guarda en `{project}.clean.tracks_clean`
- Guarda en `{project}.clean.tracks_features`

## 🎯 Features Generados

### Popularidad
- `popularity_normalized`: 0-1
- `popularity_bin`: very_low, low, medium, high, very_high
- `is_popular`: 1 si popularity >= 60

### Duración
- `duration_seconds`, `duration_minutes`
- `duration_category`: very_short, short, medium, long, very_long
- `is_short_track`, `is_long_track`

### Temporales
- `release_year`, `release_month`, `release_day`
- `release_decade`: 1990s, 2000s, etc.
- `music_era`: 2020s, 2010s, 2000s, 90s, older
- `track_age_years`
- `is_recent_release`

### Last.fm
- `lastfm_playcount`, `lastfm_playcount_log`
- `lastfm_num_tags`, `lastfm_num_similar`
- `lastfm_tag_1`, `lastfm_tag_2`, `lastfm_tag_3`

## ⚙️ Configuración

Edita `config.py` para ajustar:
- Umbrales de limpieza
- Features a generar
- Validaciones de calidad
- Parámetros de normalización

## 📈 Métricas

El pipeline reporta:
- Registros procesados vs. eliminados
- Tasa de retención
- Campos faltantes rellenados
- Outliers detectados
- Features generados

## 🔍 Validación de Calidad

Umbrales por defecto:
- Min. completitud: 70%
- Max. duplicados: 5%
- Min. fechas válidas: 90%

## 🐛 Troubleshooting

### Error: "No hay datos para procesar"
- Verifica que existan datos en `{project}.raw.tracks_unified`
- Ejecuta primero el pipeline de ingesta

### Error: "Dataset no existe"
- El pipeline crea automáticamente el dataset `clean`
- Verifica permisos en GCP

### Warning: "No cumplen estándares de calidad"
- Revisa los logs para ver qué validación falló
- Ajusta umbrales en `config.py` si es necesario

## 📝 Logs

Los logs incluyen:
- Fase del pipeline
- Registros procesados
- Features generados
- Errores y warnings
- Estadísticas finales

## 🔗 Enlaces

- BigQuery Console: https://console.cloud.google.com/bigquery?project=music-recommender-dev
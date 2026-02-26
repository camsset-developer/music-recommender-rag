# 🎵 Music Recommender System

Sistema de recomendación musical con RAG (Retrieval Augmented Generation) usando GCP.

## 📋 Requisitos

- Python 3.11+
- Cuenta en GCP
- API keys: Spotify, Genius, Last.fm

## 🚀 Setup

### 1. Clonar proyecto
```bash
git clone <tu-repo>
cd music-recommender
```

### 2. Crear ambiente virtual
```bash
python -m venv venv
source venv/bin/activate  # Mac/Linux
venv\Scripts\activate     # Windows
```

### 3. Instalar dependencias
```bash
cd ingestion
pip install -r requirements.txt
```

### 4. Configurar .env
Copia `.env.example` a `.env` y llena las credenciales.

### 5. Autenticar en GCP
```bash
gcloud auth login
gcloud config set project music-recommender-dev
gcloud auth application-default login
```


## 🎯 Uso

### Ejecutar ingesta local
```bash
cd ingestion
python main.py
```

## 📝 Licencia

MIT




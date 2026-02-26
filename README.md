# 🎵 Music Recommender System — RAG Architecture on GCP

A production-grade music recommendation system built with Retrieval-Augmented Generation (RAG), combining semantic similarity search with multi-source music data ingestion and deployed on Google Cloud Platform.

> Developed as part of the DMC Institute Applied AI Diploma (2025)

---

## 🧠 Architecture Overview

This system implements a full RAG pipeline for music recommendation:

```
┌─────────────────────────────────────────────────────────────┐
│                        RAG PIPELINE                         │
│                                                             │
│  [Spotify + Last.fm + Genius APIs]                          │
│           │                                                 │
│           ▼                                                 │
│     [ Ingestion Layer ]  ──► BigQuery + GCS                 │
│           │                                                 │
│           ▼                                                 │
│   [ Transformation Layer ]  ──► Feature Engineering        │
│           │                                                 │
│           ▼                                                 │
│   [ Embedding Layer ]  ──► Vertex AI Embeddings            │
│           │               (text + audio features)          │
│           ▼                                                 │
│   [ Vector Store ]  ──► Semantic Similarity Search         │
│           │                                                 │
│           ▼                                                 │
│   [ Recommender Engine ]  ──► REST API (Flask)             │
│           │                                                 │
│           ▼                                                 │
│      [ Frontend ]  ──► Web Interface                       │
└─────────────────────────────────────────────────────────────┘
```

**Key design decisions:**
- Hybrid embeddings combining text features (lyrics, genre tags) with audio features (tempo, energy, danceability) for richer semantic representation
- Multi-source data fusion from Spotify, Last.fm, and Genius APIs to overcome single-source limitations
- Production deployment via Cloud Run with Docker containerization
- Achieved **60–80% semantic similarity rates** across a 392-song dataset

---

## 🗂️ Project Structure

```
music-recommender/
│
├── ingestion/                  # Multi-source data collection
│   ├── clients/                # API clients (Spotify, Last.fm, Genius)
│   ├── processors/             # Data merging and normalization
│   ├── storage/                # BigQuery loader + GCS manager
│   └── utils/                  # Logger, retry logic
│
├── transformation/             # Feature engineering pipeline
│   ├── cleaners/               # Text and numeric data cleaners
│   └── feature_engineering/    # Music-specific feature extraction
│
├── embeddings/                 # Embedding generation
│   ├── generators/             # Text + feature embedding generators
│   ├── storage/                # Vector store management
│   └── embedding_combiner.py   # Hybrid embedding fusion
│
├── recommender/                # Core recommendation engine
│   ├── engine/                 # Similarity engine + recommender logic
│   ├── api/                    # REST API (Flask) + semantic search
│   └── frontend/               # Web interface
│
├── Dockerfile                  # Container configuration
├── deploy.ps1                  # GCP Cloud Run deployment script
└── dbt/                        # Data transformation (dbt)
```

---

## ⚙️ Tech Stack

| Layer | Technology |
|---|---|
| Data Ingestion | Python, Spotify API, Last.fm API, Genius API |
| Storage | Google Cloud Storage, BigQuery |
| Embeddings | Vertex AI Embeddings, Vector Store |
| Orchestration | LangChain |
| API | Flask, REST |
| Deployment | Docker, Google Cloud Run |
| Data Transformation | dbt |

---

## 📊 Results

| Metric | Value |
|---|---|
| Dataset size | 392 songs |
| Semantic similarity rate | 60–80% |
| Deployment | Google Cloud Run |
| Embedding strategy | Hybrid (text + audio features) |

---

## 🚀 Setup & Deployment

### Prerequisites
- Python 3.11+
- GCP account with billing enabled
- API keys: Spotify, Genius, Last.fm

### Local Setup

```bash
# Clone repository
git clone https://github.com/camsset-developer/music-recommender-rag.git
cd music-recommender-rag

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Mac/Linux
venv\Scripts\activate     # Windows

# Install dependencies
cd ingestion && pip install -r requirements.txt
cd ../embeddings && pip install -r requirements.txt
cd ../recommender && pip install -r requirements.txt
```

### Configure Environment

```bash
cp .env.example .env
# Fill in your API keys and GCP project credentials
```

### GCP Authentication

```bash
gcloud auth login
gcloud config set project music-recommender-dev
gcloud auth application-default login
```

### Run Pipeline

```bash
# 1. Data ingestion
cd ingestion && python main.py

# 2. Transformation
cd transformation && python main.py

# 3. Generate embeddings
cd embeddings && python main.py

# 4. Start recommender API
cd recommender && python main.py
```

### Deploy to Cloud Run

```bash
./deploy.ps1
```

---

## 🔬 Research Context

This project explores the application of RAG architectures to the music domain, where recommendation quality depends on combining structured audio features with unstructured semantic data (lyrics, tags, descriptions).

The hybrid embedding approach draws inspiration from recent work in retrieval-augmented generation for knowledge-intensive tasks, adapting these techniques to a domain where "knowledge" spans both numerical audio features and natural language metadata.

**Research interests this project connects to:**
- RAG architecture optimization for domain-specific retrieval
- Efficient embedding strategies for multimodal data
- AI-assisted workflow automation with practical deployment constraints

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

*Part of a broader research trajectory toward AI-assisted laboratory and scientific workflow systems.*

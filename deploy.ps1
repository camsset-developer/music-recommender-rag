# Script de despliegue a Google Cloud Run (Windows PowerShell)

Write-Host "========================================" -ForegroundColor Green
Write-Host "🚀 DESPLEGANDO MUSIC RECOMMENDER API" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green

# Variables
$PROJECT_ID = "music-recommender-dev"
$REGION = "us-central1"
$SERVICE_NAME = "music-recommender-api"
$IMAGE_NAME = "gcr.io/$PROJECT_ID/$SERVICE_NAME"

# Verificar que gcloud esté configurado
Write-Host "`n📋 Verificando configuración..." -ForegroundColor Yellow
gcloud config set project $PROJECT_ID

# Construir imagen Docker
Write-Host "`n🔨 Construyendo imagen Docker..." -ForegroundColor Yellow
docker build -t "${IMAGE_NAME}:latest" .

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Error construyendo imagen" -ForegroundColor Red
    exit 1
}

# Subir imagen a Google Container Registry
Write-Host "`n📤 Subiendo imagen a GCR..." -ForegroundColor Yellow
docker push "${IMAGE_NAME}:latest"

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Error subiendo imagen" -ForegroundColor Red
    exit 1
}

# Desplegar en Cloud Run
Write-Host "`n☁️  Desplegando en Cloud Run..." -ForegroundColor Yellow
gcloud run deploy $SERVICE_NAME `
  --image "${IMAGE_NAME}:latest" `
  --platform managed `
  --region $REGION `
  --allow-unauthenticated `
  --memory 1Gi `
  --cpu 1 `
  --timeout 300 `
  --max-instances 10 `
  --set-env-vars "GCP_PROJECT_ID=$PROJECT_ID" `
  --set-env-vars "DATASET_EMBEDDINGS=embeddings" `
  --set-env-vars "DATASET_CLEAN=clean"

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Error desplegando en Cloud Run" -ForegroundColor Red
    exit 1
}

# Obtener URL del servicio
Write-Host "`n========================================" -ForegroundColor Green
Write-Host "✅ DESPLIEGUE COMPLETADO" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green

$SERVICE_URL = gcloud run services describe $SERVICE_NAME --region $REGION --format "value(status.url)"

Write-Host "`n🌐 URL de la API:" -ForegroundColor Green
Write-Host $SERVICE_URL -ForegroundColor Green

Write-Host "`n📚 Documentación:" -ForegroundColor Green
Write-Host "$SERVICE_URL/docs" -ForegroundColor Green

Write-Host "`n💡 Prueba la API:" -ForegroundColor Green
Write-Host "$SERVICE_URL/health" -ForegroundColor Green

Write-Host "`n📝 Siguiente paso:" -ForegroundColor Yellow
Write-Host "Actualiza tu frontend con esta URL en app.js:"
Write-Host "const API_URL = '$SERVICE_URL';" -ForegroundColor Cyan

Write-Host "`n========================================" -ForegroundColor Green

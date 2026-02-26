$PROJECT_ID = "music-recommender-dev"
$REGION = "us-central1"
$SERVICE_NAME = "music-recommender-api"
$IMAGE_NAME = "gcr.io/$PROJECT_ID/$SERVICE_NAME"

Write-Output "========================================="
Write-Output "Desplegando Music Recommender API"
Write-Output "========================================="

Write-Output ""
Write-Output "Paso 1: Configurando proyecto..."
gcloud config set project $PROJECT_ID

Write-Output ""
Write-Output "Paso 2: Construyendo imagen Docker..."
docker build -t "${IMAGE_NAME}:latest" .

if ($LASTEXITCODE -ne 0) {
    Write-Output "ERROR: Fallo al construir imagen"
    exit 1
}

Write-Output ""
Write-Output "Paso 3: Subiendo imagen a GCR..."
docker push "${IMAGE_NAME}:latest"

if ($LASTEXITCODE -ne 0) {
    Write-Output "ERROR: Fallo al subir imagen"
    exit 1
}

Write-Output ""
Write-Output "Paso 4: Desplegando en Cloud Run..."
gcloud run deploy $SERVICE_NAME --image "${IMAGE_NAME}:latest" --platform managed --region $REGION --allow-unauthenticated --memory 1Gi --cpu 1 --timeout 300 --max-instances 10 --set-env-vars "GCP_PROJECT_ID=$PROJECT_ID,DATASET_EMBEDDINGS=embeddings,DATASET_CLEAN=clean"

if ($LASTEXITCODE -ne 0) {
    Write-Output "ERROR: Fallo al desplegar"
    exit 1
}

Write-Output ""
Write-Output "========================================="
Write-Output "DESPLIEGUE COMPLETADO"
Write-Output "========================================="

$SERVICE_URL = gcloud run services describe $SERVICE_NAME --region $REGION --format "value(status.url)"

Write-Output ""
Write-Output "URL de la API: $SERVICE_URL"
Write-Output "Documentacion: $SERVICE_URL/docs"
Write-Output "Health check: $SERVICE_URL/health"
Write-Output ""
Write-Output "Actualiza tu frontend en app.js:"
Write-Output "const API_URL = '$SERVICE_URL';"
Write-Output ""
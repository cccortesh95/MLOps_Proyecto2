#!/bin/bash
# =============================================================================
# Script de despliegue de Airflow en Kubernetes
# Construye la imagen, la publica en DockerHub e instala con Helm.
# =============================================================================

set -e

# --- Configuración ---
NAMESPACE="${NAMESPACE:-mlops}"
RELEASE_NAME="${RELEASE_NAME:-airflow}"
AIRFLOW_BASE_TAG="${AIRFLOW_BASE_TAG:-3.1.8}"
DOCKERHUB_USER="${DOCKERHUB_USER:-cccortesh}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
IMAGE_NAME="$DOCKERHUB_USER/mlops-airflow"

echo "============================================="
echo " Despliegue de Airflow en Kubernetes"
echo "============================================="
echo "Namespace:     $NAMESPACE"
echo "Release:       $RELEASE_NAME"
echo "Base tag:      $AIRFLOW_BASE_TAG"
echo "Image:         $IMAGE_NAME:$IMAGE_TAG"
echo "============================================="

# --- Paso 1: Verificar prerrequisitos ---
echo ""
echo "[1/5] Verificando prerrequisitos..."
docker --version
kubectl version --client
helm version

# --- Paso 2: Construir imagen custom ---
echo ""
echo "[2/5] Construyendo imagen Docker custom..."
docker build --pull \
  --build-arg AIRFLOW_BASE_TAG=$AIRFLOW_BASE_TAG \
  --tag $IMAGE_NAME:$IMAGE_TAG .

# --- Paso 3: Push a DockerHub ---
echo ""
echo "[3/5] Publicando imagen en DockerHub..."
docker push $IMAGE_NAME:$IMAGE_TAG

# --- Paso 4: Crear namespace ---
echo ""
echo "[4/5] Creando namespace $NAMESPACE..."
kubectl create namespace $NAMESPACE 2>/dev/null || echo "Namespace ya existe."

# --- Paso 5: Instalar/Actualizar Airflow con Helm ---
echo ""
echo "[5/5] Instalando/Actualizando Airflow con Helm..."
helm repo add apache-airflow https://airflow.apache.org 2>/dev/null || true
helm repo update

helm upgrade --install $RELEASE_NAME apache-airflow/airflow \
  --namespace $NAMESPACE \
  --set images.airflow.repository=$IMAGE_NAME \
  --set images.airflow.tag=$IMAGE_TAG \
  -f values/values-local.yaml \
  --wait --timeout 20m

echo ""
echo "============================================="
echo " Despliegue completado!"
echo "============================================="
echo ""
echo "Verificar pods:"
echo "  kubectl get pods -n $NAMESPACE"
echo ""
echo "Exponer UI de Airflow:"
echo "  kubectl port-forward svc/$RELEASE_NAME-api-server 8080:8080 -n $NAMESPACE"
echo ""
echo "Abrir: http://localhost:8080"
echo ""

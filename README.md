# 🏥 MLOps Proyecto 2 - Diabetes 130-US Hospitals

## 📑 Tabla de contenido

- [Descripción general](#descripción-general)
- [Arquitectura de la solución](#arquitectura-de-la-solución)
- [Servicios desplegados](#servicios-desplegados)
- [Estructura del proyecto](#estructura-del-proyecto)
- [Imágenes en DockerHub](#imágenes-en-dockerhub)
- [Variables de entorno](#variables-de-entorno)
- [Requisitos previos](#requisitos-previos)
- [Creación del clúster](#creación-del-clúster)
- [Build y push de imágenes](#build-y-push-de-imágenes)
- [Despliegue de la solución](#despliegue-de-la-solución)
- [Despliegue de Airflow](#despliegue-de-airflow)
- [Actualización de DAGs](#actualización-de-dags)
- [DAGs incluidos](#dags-incluidos)
- [Troubleshooting](#troubleshooting)
- [Limpieza](#limpieza)
- [Colaboradores](#-colaboradores)

## Descripción general

Arquitectura integral de MLOps sobre Kubernetes que cubre el ciclo completo de vida de un modelo de Machine Learning: ingesta incremental de datos clínicos de pacientes con diabetes, almacenamiento en capas (raw, clean, inference logs), entrenamiento periódico con registro en MLflow, selección automática del mejor modelo, inferencia productiva mediante API, interfaz gráfica, pruebas de carga y observabilidad.

## Arquitectura de la solución

La solución está compuesta por los siguientes servicios:

- **PostgreSQL**: almacena datos crudos, datos procesados, registros de inferencia y metadata de MLflow
- **MinIO**: almacena artifacts y modelos generados por MLflow
- **MLflow Tracking Server**: registra experimentos, métricas, parámetros y modelos
- **Apache Airflow**: orquesta el flujo de ingesta, procesamiento, entrenamiento y registro
- **FastAPI**: API de inferencia que carga el mejor modelo desde MLflow Registry
- **Streamlit**: interfaz gráfica para consumir la API de inferencia
- **Locust**: genera pruebas de carga sobre la API
- **Prometheus + Grafana**: recolección y visualización de métricas

---

## Servicios desplegados

| Servicio | Propósito | Puerto |
|----------|-----------|--------|
| `postgres` | Base de datos (raw, clean, inference logs, metadata MLflow) | `5432` |
| `minio` | Artifact store para MLflow | `9000` |
| `minio console` | Consola web de MinIO | `9001` |
| `mlflow` | Tracking server y model registry | `5000` |
| `airflow webserver` | Interfaz web de Airflow | `8080` |
| `airflow scheduler` | Planificador de DAGs | — |
| `api` | API de inferencia (FastAPI) | `8000` |
| `streamlit` | Interfaz gráfica | `8501` |
| `locust` | Pruebas de carga | `8089` |
| `prometheus` | Recolección de métricas | `9090` |
| `grafana` | Dashboards de observabilidad | `3000` |

---

## Estructura del proyecto

```bash
mlops-proyecto2/
├── airflow/
│   ├── dags/
│   │   ├── hello_level3.py
│   │   └── diabetes_pipeline.py
│   ├── values/
│   │   └── values-local.yaml
│   ├── plugins/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── deploy.sh
├── api/
│   ├── app/
│   │   ├── main.py
│   │   ├── schemas.py
│   │   ├── model_loader.py
│   │   ├── database.py
│   │   └── metrics.py
│   ├── Dockerfile
│   └── requirements.txt
├── streamlit/
│   ├── app/
│   │   └── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── locust/
│   ├── locustfile.py
│   ├── Dockerfile
│   └── requirements.txt
├── k8s/
│   ├── namespace/
│   ├── postgres/
│   ├── minio/
│   ├── mlflow/
│   ├── api/
│   ├── streamlit/
│   ├── locust/
│   ├── prometheus/
│   └── grafana/
├── grafana/
│   ├── dashboards/
│   └── provisioning/
├── prometheus/
│   └── prometheus.yml
└── README.md
```

---

## Imágenes en DockerHub

Todas las imágenes custom del proyecto se publican en DockerHub. Los manifiestos de Kubernetes las referencian directamente desde el registro, sin depender de imágenes locales.

| Imagen | Dockerfile | Descripción |
|--------|------------|-------------|
| `cccortesh/mlops-airflow:0.0.1` | `airflow/Dockerfile` | Airflow con DAGs y dependencias del proyecto |
| `cccortesh/mlops-api:0.0.1` | `api/Dockerfile` | API de inferencia FastAPI |
| `cccortesh/mlops-streamlit:0.0.1` | `streamlit/Dockerfile` | Interfaz gráfica Streamlit |
| `cccortesh/mlops-locust:0.0.1` | `locust/Dockerfile` | Pruebas de carga Locust |

---

## Variables de entorno

### PostgreSQL

| Variable | Valor |
|----------|-------|
| `POSTGRES_USER` | `mlops_user` |
| `POSTGRES_PASSWORD` | `mlops_password` |
| `POSTGRES_DB` | `mlops_db` |

### MinIO

| Variable | Valor |
|----------|-------|
| `MINIO_ROOT_USER` | `minioadmin` |
| `MINIO_ROOT_PASSWORD` | `minioadmin123` |
| `AWS_ACCESS_KEY_ID` | `minioadmin` |
| `AWS_SECRET_ACCESS_KEY` | `minioadmin123` |

### MLflow

| Variable | Valor |
|----------|-------|
| `MLFLOW_TRACKING_URI` | `http://mlflow-service:5000` |
| `MLFLOW_S3_ENDPOINT_URL` | `http://minio-service:9000` |
| `MLFLOW_ARTIFACT_ROOT` | `s3://mlflow-artifacts/` |

### Airflow (conexión a datos del proyecto)

| Variable | Valor |
|----------|-------|
| `DATA_DB_HOST` | `postgres-service.mlops.svc.cluster.local` |
| `DATA_DB_PORT` | `5432` |
| `DATA_DB_NAME` | `mlops_db` |
| `DATA_DB_USER` | `mlops_user` |
| `DATA_DB_PASSWORD` | `mlops_password` |

---

## Requisitos previos

- [Docker](https://docs.docker.com/get-docker/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Helm](https://helm.sh/docs/intro/install/)
- [kind](https://kind.sigs.k8s.io/)
- Cuenta en [DockerHub](https://hub.docker.com/)

Verificar instalación:

```bash
docker --version
kubectl version --client
helm version
kind version
```

Login en DockerHub:

```bash
docker login
```

---

## Creación del clúster

```bash
kind create cluster --image kindest/node:v1.30.13
kubectl cluster-info --context kind-kind
kubectl get nodes
```

---

## Build y push de imágenes

Todas las imágenes custom se construyen y publican en DockerHub antes de desplegar en Kubernetes.

```bash
export DOCKERHUB_USER=cccortesh
export TAG=0.0.1
```

### Airflow

```bash
cd airflow
docker build --pull --build-arg AIRFLOW_BASE_TAG=3.1.8 \
  -t $DOCKERHUB_USER/mlops-airflow:$TAG .
docker push $DOCKERHUB_USER/mlops-airflow:$TAG
cd ..
```

### API de inferencia

```bash
cd api
docker build -t $DOCKERHUB_USER/mlops-api:$TAG .
docker push $DOCKERHUB_USER/mlops-api:$TAG
cd ..
```

### Streamlit

```bash
cd streamlit
docker build -t $DOCKERHUB_USER/mlops-streamlit:$TAG .
docker push $DOCKERHUB_USER/mlops-streamlit:$TAG
cd ..
```

### Locust

```bash
cd locust
docker build -t $DOCKERHUB_USER/mlops-locust:$TAG .
docker push $DOCKERHUB_USER/mlops-locust:$TAG
cd ..
```

---

## Despliegue de la solución

> **Antes de desplegar**, todas las imágenes custom deben estar publicadas en DockerHub. Ver sección [Build y push de imágenes](#build-y-push-de-imágenes).

### 1. Infraestructura base

```bash
kubectl apply -f k8s/namespace/
kubectl apply -f k8s/postgres/
kubectl apply -f k8s/minio/
```

### 2. MLflow

```bash
kubectl apply -f k8s/mlflow/
```

### 3. Airflow

Ver sección [Despliegue de Airflow](#despliegue-de-airflow). La imagen `cccortesh95/mlops-airflow` se jala directamente desde DockerHub.

### 4. Servicios de inferencia

Las imágenes `cccortesh95/mlops-api` y `cccortesh95/mlops-streamlit` se jalan desde DockerHub.

```bash
kubectl apply -f k8s/api/
kubectl apply -f k8s/streamlit/
```

### 5. Observabilidad y pruebas de carga

La imagen `cccortesh95/mlops-locust` se jala desde DockerHub. Prometheus y Grafana usan imágenes oficiales.

```bash
kubectl apply -f k8s/prometheus/
kubectl apply -f k8s/grafana/
kubectl apply -f k8s/locust/
```

---

## Despliegue de Airflow

Airflow se despliega con el Helm Chart oficial usando la imagen custom publicada en DockerHub.

### Paso 1: Agregar repositorio Helm

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

### Paso 2: Crear namespace

```bash
export NAMESPACE=airflow-local
export RELEASE_NAME=airflow
kubectl create namespace $NAMESPACE
```

### Paso 3: Instalar con Helm

La imagen ya está en DockerHub, no hace falta cargarla en kind manualmente.

```bash
cd airflow
helm upgrade --install $RELEASE_NAME apache-airflow/airflow \
  --namespace $NAMESPACE \
  --set images.airflow.repository=cccortesh/mlops-airflow \
  --set images.airflow.tag=0.0.1 \
  -f values/values-local.yaml \
  --wait --timeout 20m
```

### Paso 4: Verificar

```bash
kubectl get pods -n $NAMESPACE
helm list -n $NAMESPACE
```

### Paso 5: Exponer UI

```bash
kubectl port-forward svc/$RELEASE_NAME-api-server 8080:8080 -n $NAMESPACE
```

Abrir: http://localhost:8080

---

## Actualización de DAGs

1. Editar archivos en `airflow/dags/`
2. Actualizar `airflow/requirements.txt` si hay nuevas librerías
3. Reconstruir y publicar con nuevo tag:
   ```bash
   cd airflow
   export TAG=0.0.2
   docker build --pull --build-arg AIRFLOW_BASE_TAG=3.1.8 \
     -t cccortesh/mlops-airflow:$TAG .
   docker push cccortesh/mlops-airflow:$TAG
   ```
4. Actualizar Helm:
   ```bash
   helm upgrade --install $RELEASE_NAME apache-airflow/airflow \
     --namespace $NAMESPACE \
     --set images.airflow.repository=cccortesh/mlops-airflow \
     --set images.airflow.tag=$TAG \
     -f values/values-local.yaml \
     --wait --timeout 20m
   ```

---

## DAGs incluidos

| DAG | Descripción |
|-----|-------------|
| `hello_level3` | DAG de prueba para verificar que Airflow funciona en Kubernetes |
| `diabetes_pipeline` | Pipeline MLOps completo con 10 tareas secuenciales |

### Tareas del DAG `diabetes_pipeline`

| # | Task ID | Descripción |
|---|---------|-------------|
| 1 | `validate_source` | Descarga y valida el archivo CSV fuente |
| 2 | `load_raw_batch` | Carga incremental por lotes (máx 15,000 registros) |
| 3 | `validate_quality` | Validación de calidad: conteo, duplicados, límites |
| 4 | `preprocess_data` | Limpieza, nulos, ingeniería de features, encoding |
| 5 | `store_clean_data` | Almacena datos procesados en tabla `clean_data` |
| 6 | `split_data` | Separación 70/15/15 estratificada (train/val/test) |
| 7 | `train_models` | Entrena LogisticRegression, XGBoost y LightGBM |
| 8 | `compare_models` | Compara contra el modelo productivo actual en MLflow |
| 9 | `promote_best_model` | Asigna alias `champion` al mejor modelo por ROC-AUC |

---

## Troubleshooting

| Problema | Causa | Solución |
|----------|-------|----------|
| CrashLoopBackOff en `airflow-api-server` | Incompatibilidad entre versión del chart y la imagen | Alinear `AIRFLOW_BASE_TAG` con `APP VERSION` del chart |
| ImagePullBackOff | Imagen no existe en DockerHub o tag incorrecto | Verificar con `docker pull cccortesh/mlops-airflow:0.0.1` |
| No aparece el DAG nuevo | Imagen vieja en los pods | Reconstruir, push a DockerHub con nuevo tag y `helm upgrade` |
| Port-forward no funciona | Servicio no encontrado | Verificar con `kubectl get svc -n $NAMESPACE` |

---

## Limpieza

```bash
# Desinstalar Airflow
helm uninstall $RELEASE_NAME -n $NAMESPACE
kubectl delete namespace $NAMESPACE

# Eliminar componentes
kubectl delete -f k8s/grafana/
kubectl delete -f k8s/prometheus/
kubectl delete -f k8s/locust/
kubectl delete -f k8s/streamlit/
kubectl delete -f k8s/api/
kubectl delete -f k8s/mlflow/
kubectl delete -f k8s/minio/
kubectl delete -f k8s/postgres/
kubectl delete -f k8s/namespace/

# Eliminar clúster
kind delete cluster
```

---

## 👥 Colaboradores

- 🧑‍💻 **Camilo Cortés** — [![GitHub](https://img.shields.io/badge/GitHub-@cccortesh-181717?logo=github)](https://github.com/cccortesh)

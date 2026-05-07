# 🏥 MLOps Proyecto 2 - Diabetes 130-US Hospitals

## 📑 Tabla de contenido

- [Descripción general](#descripción-general)
- [Arquitectura de la solución](#arquitectura-de-la-solución)
- [Servicios desplegados](#servicios-desplegados)
- [Estructura del proyecto](#estructura-del-proyecto)
- [Imágenes en DockerHub](#imágenes-en-dockerhub)
- [Variables de entorno](#variables-de-entorno)
- [Requisitos previos](#requisitos-previos)
- [Despliegue paso a paso](#despliegue-paso-a-paso)
  - [1. Crear el clúster](#1-crear-el-clúster)
  - [2. Build y push de imágenes](#2-build-y-push-de-imágenes)
  - [3. Infraestructura base](#3-infraestructura-base-namespace--postgres--minio)
  - [4. MLflow](#4-mlflow)
  - [5. Airflow](#5-airflow)
  - [6. Capa de inferencia](#6-capa-de-inferencia-api--streamlit--locust)
  - [7. Observabilidad](#7-observabilidad-prometheus--grafana)
  - [8. Verificación y port-forwards](#8-verificación-y-port-forwards)
- [Detalles por servicio](#detalles-por-servicio)
  - [PostgreSQL](#postgresql)
  - [MinIO](#minio)
  - [MLflow](#mlflow)
  - [Airflow](#airflow)
  - [Capa de inferencia (API, Streamlit y Locust)](#capa-de-inferencia-api-streamlit-y-locust)
- [Implementación de los DAGs](#implementación-de-los-dags)
- [Troubleshooting](#troubleshooting)
- [Limpieza](#limpieza)
- [Colaboradores](#-colaboradores)

---

## Descripción general

Arquitectura integral de MLOps sobre Kubernetes que cubre el ciclo completo de vida de un modelo de Machine Learning: ingesta incremental de datos clínicos de pacientes con diabetes, almacenamiento en capas (raw, clean, inference logs), entrenamiento periódico con registro en MLflow, selección automática del mejor modelo, inferencia productiva mediante API, interfaz gráfica, pruebas de carga y observabilidad.

## Arquitectura de la solución

- **PostgreSQL**: datos crudos, datos procesados, registros de inferencia y metadata de MLflow
- **MinIO**: artifact store para modelos y artefactos de MLflow
- **MLflow Tracking Server**: experimentos, métricas, parámetros y model registry
- **Apache Airflow**: orquesta ingesta, procesamiento, entrenamiento y promoción
- **FastAPI**: API de inferencia que carga el mejor modelo desde MLflow Registry
- **Streamlit**: interfaz gráfica que consume la API de inferencia
- **Locust**: pruebas de carga sobre la API
- **Prometheus + Grafana**: recolección y visualización de métricas

---

## Servicios desplegados

| Servicio | Propósito | Puerto |
|----------|-----------|--------|
| `postgres` | Base de datos del proyecto (raw, clean, inference logs) | `5432` |
| `mlflow-postgres` | Base de datos de metadata de MLflow | `5432` |
| `minio` | Artifact store para MLflow | `9000` |
| `minio console` | Consola web de MinIO | `9001` |
| `mlflow` | Tracking server y model registry | `5000` |
| `airflow api-server` | Interfaz web de Airflow | `8080` |
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
│   │   ├── ingestion_pipeline.py
│   │   ├── training_pipeline.py
│   │   └── tasks/
│   ├── values/values-local.yaml
│   ├── plugins/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── deploy.sh
├── api/
│   ├── app/
│   ├── Dockerfile
│   └── requirements.txt
├── streamlit/
│   ├── app/app.py
│   ├── Dockerfile
│   └── requirements.txt
├── locust/
│   ├── locustfile.py
│   ├── Dockerfile
│   └── requirements.txt
├── mlflow/
│   └── Dockerfile
├── training/
│   ├── scripts/
│   ├── Dockerfile
│   └── requirements.txt
├── k8s/
│   ├── namespace/
│   ├── postgres/
│   ├── mlflow-postgres/
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
├── prometheus/prometheus.yml
└── README.md
```

---

## Imágenes en DockerHub

Todas las imágenes custom del proyecto se publican en DockerHub. Los manifiestos de Kubernetes las referencian directamente desde el registro, sin depender de imágenes locales.

| Imagen | Dockerfile | Descripción |
|--------|------------|-------------|
| `cccortesh/mlops-airflow:latest` | `airflow/Dockerfile` | Airflow con DAGs y dependencias del proyecto |
| `cccortesh/mlops-mlflow:latest` | `mlflow/Dockerfile` | MLflow server con `psycopg2` y `boto3` |
| `cccortesh/mlops-api:latest` | `api/Dockerfile` | API de inferencia FastAPI |
| `cccortesh/mlops-streamlit:latest` | `streamlit/Dockerfile` | Interfaz gráfica Streamlit |
| `cccortesh/mlops-locust:latest` | `locust/Dockerfile` | Pruebas de carga Locust |

---

## Variables de entorno

### PostgreSQL

| Variable | Valor |
|----------|-------|
| `POSTGRES_USER` | `mlops_user` |
| `POSTGRES_PASSWORD` | `mlops1234` |
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
| `DATA_DB_HOST` | `postgres-service` |
| `DATA_DB_PORT` | `5432` |
| `DATA_DB_NAME` | `raw_data_db` |
| `CLEAN_DB_NAME` | `clean_data_db` |
| `INFERENCE_DB_NAME` | `inference_db` |
| `DATA_DB_USER` | `mlops_user` |
| `DATA_DB_PASSWORD` | `mlops1234` |

### API, Streamlit y Locust

Inyectadas por los manifiestos en `k8s/api/`, `k8s/streamlit/` y `k8s/locust/` (ConfigMap / Secret).

**API (`api-config` + `api-secret`)**

| Variable | Descripción |
|----------|-------------|
| `MLFLOW_TRACKING_URI` | URI del servidor MLflow (`http://mlflow-service:5000`) |
| `MLFLOW_S3_ENDPOINT_URL` | Endpoint S3-compatible de MinIO para descargar artefactos |
| `MODEL_NAME` | Nombre del modelo en el registry (por defecto `diabetes-model`) |
| `DB_HOST` / `DB_PORT` / `DB_NAME` | Conexión a `inference_db` |
| `DB_USER` / `DB_PASSWORD` | Credenciales (Secret `api-secret`) |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | Credenciales MinIO |

**Streamlit (`streamlit-config`)**

| Variable | Descripción |
|----------|-------------|
| `API_URL` | URL base de la API (`http://api-service:8000`). La UI solo habla con FastAPI. |

**Locust (`locust-config`)**

| Variable | Descripción |
|----------|-------------|
| `LOCUST_HOST` | URL base contra la que se genera carga (`http://api-service:8000`). |

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

## Despliegue paso a paso

Este es el **único flujo oficial** de despliegue. Las secciones por servicio más abajo solo describen manifiestos, credenciales y cómo acceder a cada UI, sin repetir comandos.

> El orden importa: PostgreSQL y MinIO deben estar listos antes de MLflow; MLflow debe estar listo antes de Airflow y antes de la API.

### 1. Crear el clúster

```bash
kind create cluster --image kindest/node:v1.30.13
kubectl cluster-info --context kind-kind
kubectl get nodes
```

### 2. Build y push de imágenes

```bash
export DOCKERHUB_USER=cccortesh

# Airflow
docker build --pull --build-arg AIRFLOW_BASE_TAG=3.2.0 \
  -t $DOCKERHUB_USER/mlops-airflow:latest airflow/
docker push $DOCKERHUB_USER/mlops-airflow:latest

# MLflow
docker build -t $DOCKERHUB_USER/mlops-mlflow:latest mlflow/
docker push $DOCKERHUB_USER/mlops-mlflow:latest

# API
docker build -t $DOCKERHUB_USER/mlops-api:latest api/
docker push $DOCKERHUB_USER/mlops-api:latest

# Streamlit
docker build -t $DOCKERHUB_USER/mlops-streamlit:latest streamlit/
docker push $DOCKERHUB_USER/mlops-streamlit:latest

# Locust
docker build -t $DOCKERHUB_USER/mlops-locust:latest locust/
docker push $DOCKERHUB_USER/mlops-locust:latest
```

### 3. Infraestructura base (namespace + postgres + minio)

```bash
kubectl apply -f k8s/namespace/
kubectl apply -f k8s/postgres/
kubectl apply -f k8s/mlflow-postgres/
kubectl apply -f k8s/minio/
```

Esperar a que los pods estén `Running` antes de continuar:

```bash
kubectl get pods -n mlops -w
```

### 4. MLflow

```bash
kubectl apply -f k8s/mlflow/
kubectl get pods -n mlops -l app=mlflow -w
```

### 5. Airflow

Se despliega con el Helm Chart oficial, usando la imagen custom publicada en DockerHub. El chart ya corre las migraciones y crea el usuario `admin/admin` automáticamente (configurado en `airflow/values/values-local.yaml`).

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update

helm upgrade --install airflow apache-airflow/airflow \
  --namespace mlops \
  --set images.airflow.repository=cccortesh/mlops-airflow \
  --set images.airflow.tag=latest \
  --set images.airflow.pullPolicy=Always \
  -f airflow/values/values-local.yaml \
  --timeout 20m
```

> **No usar `--wait`**: Helm se bloquea esperando que todos los pods estén Ready, y si la migración tarda más de lo esperado el release no se registra, dejando recursos huérfanos. Sin `--wait`, Helm registra el release inmediatamente y puedes monitorear los pods aparte.

Monitorear hasta que todos estén `Running`:

```bash
kubectl get pods -n mlops -w
```

Alternativa con script:

```bash
cd airflow && ./deploy.sh
```

### 6. Capa de inferencia (API + Streamlit + Locust)

```bash
kubectl apply -f k8s/api/
kubectl apply -f k8s/streamlit/
kubectl apply -f k8s/locust/
```

> La API arranca en estado `degraded` hasta que el DAG `training_pipeline` registre un modelo con alias `champion` en MLflow. Ver [Capa de inferencia](#capa-de-inferencia-api-streamlit-y-locust).

### 7. Observabilidad (Prometheus + Grafana)

```bash
kubectl apply -f k8s/prometheus/
kubectl apply -f k8s/grafana/
```

### 8. Verificación y port-forwards

```bash
kubectl get pods -n mlops
kubectl get svc -n mlops
```

Todos los servicios deben estar `Running`. Para acceder a las UIs:

| Servicio | Comando | URL |
|----------|---------|-----|
| Airflow | `kubectl port-forward svc/airflow-api-server 8080:8080 -n mlops` | http://localhost:8080 |
| MLflow | `kubectl port-forward svc/mlflow-service 5000:5000 -n mlops` | http://localhost:5000 |
| MinIO Console | `kubectl port-forward svc/minio-service 9001:9001 -n mlops` | http://localhost:9001 |
| API | `kubectl port-forward svc/api-service 8000:8000 -n mlops` | http://localhost:8000/docs |
| Streamlit | `kubectl port-forward svc/streamlit-service 8501:8501 -n mlops` | http://localhost:8501 |
| Locust | `kubectl port-forward svc/locust-service 8089:8089 -n mlops` | http://localhost:8089 |
| Grafana | `kubectl port-forward svc/grafana-service 3000:3000 -n mlops` | http://localhost:3000 |
| Prometheus | `kubectl port-forward svc/prometheus-service 9090:9090 -n mlops` | http://localhost:9090 |

> Si el puerto local ya está ocupado (ej. un PostgreSQL del sistema en `5432`), cambia el primer número del port-forward (ej. `5433:5432`).

---

## Detalles por servicio

Esta sección describe qué compone cada servicio y cómo verificarlo. **Los comandos de despliegue están únicamente en [Despliegue paso a paso](#despliegue-paso-a-paso).**

### PostgreSQL

Se despliegan dos instancias en el namespace `mlops`. Airflow cuenta con su propia instancia interna gestionada por Helm.

| Instancia | Base de datos | Esquema | Tabla | Propósito |
|---|---|---|---|---|
| `postgres` | `raw_data_db` | `raw` | `diabetes_raw` | Datos crudos sin transformar |
| `postgres` | `clean_data_db` | `clean` | `diabetes_clean` | Datos procesados para entrenamiento |
| `postgres` | `inference_db` | `inference` | `inference_logs` | Registros de inferencia de la API |
| `mlflow-postgres` | `mlflow_db` | `public` | — | Metadata de MLflow (backend store) |
| `airflow-postgresql` | `postgres` | `public` | — | Metadata de Airflow (viene con Helm) |

La separación de MLflow en su propia instancia permite que las cargas del DAG no afecten el tracking server.

**Manifiestos**

`k8s/postgres/`: `secret.yaml`, `configmap.yaml`, `configmap-init.yaml` (crea las 3 BDs, esquemas y la tabla `inference_logs`), `pvc.yaml` (5Gi), `statefulset.yaml`, `service.yaml`.

`k8s/mlflow-postgres/`: `secret.yaml`, `configmap.yaml`, `pvc.yaml` (2Gi), `statefulset.yaml`, `service.yaml`.

**Verificar**

```bash
kubectl exec -it postgres-0 -n mlops -- psql -U mlops_user -d mlops_db -c "\l"
kubectl exec -it postgres-0 -n mlops -- psql -U mlops_user -d raw_data_db -c "\dt raw.*"
kubectl exec -it postgres-0 -n mlops -- psql -U mlops_user -d clean_data_db -c "\dt clean.*"
kubectl exec -it postgres-0 -n mlops -- psql -U mlops_user -d inference_db -c "\dt inference.*"
```

**Conexión local**

| Instancia | Port-forward | Host | Port | User | Password |
|-----------|-------------|------|------|------|----------|
| postgres | `kubectl port-forward svc/postgres-service 5432:5432 -n mlops` | localhost | 5432 | `mlops_user` | `mlops1234` |
| mlflow-postgres | `kubectl port-forward svc/mlflow-postgres-service 5433:5432 -n mlops` | localhost | 5433 | `mlflow_user` | `mlflow1234` |

### MinIO

Artifact store de MLflow. Almacena los modelos serializados y artefactos de experimentación.

**Manifiestos** (`k8s/minio/`): `secret.yaml`, `configmap.yaml`, `pvc.yaml` (10Gi), `statefulset.yaml`, `service.yaml` (API `9000`, consola `9001`), `job-create-bucket.yaml` (crea el bucket `mlflow-artifacts` automáticamente).

**Verificar**

```bash
kubectl get pods -n mlops
kubectl get jobs -n mlops
kubectl logs job/minio-create-bucket -n mlops
```

**Consola web**: usuario `minioadmin`, contraseña `minioadmin123`.

### MLflow

Tracking server y model registry. Usa `mlflow-postgres` como backend y MinIO como artifact store.

Se usa una imagen custom (`cccortesh/mlops-mlflow`) porque la imagen oficial no incluye `psycopg2` ni `boto3`.

**Manifiestos** (`k8s/mlflow/`): `secret.yaml` (credenciales S3 y URI a PostgreSQL), `configmap.yaml` (tracking URI, endpoint S3, artifact root), `deployment.yaml`, `service.yaml` (puerto `5000`).

### Airflow

Airflow usa el chart oficial de Apache con la imagen custom de DockerHub. Toda la configuración vive en `airflow/values/values-local.yaml`:

- `executor: LocalExecutor` (no requiere Redis/Celery).
- `migrateDatabaseJob.enabled: true`: corre las migraciones de BD en la instalación.
- `createUserJob.enabled: true` + `defaultUser`: crea `admin/admin` automáticamente.
- PostgreSQL interno habilitado (metadata de Airflow).
- Variables de entorno globales con credenciales MLflow, MinIO y PostgreSQL del proyecto.

**Actualización de DAGs**

1. Editar archivos en `airflow/dags/`.
2. Actualizar `airflow/requirements.txt` si hay nuevas librerías.
3. Reconstruir la imagen y publicar:
   ```bash
   docker build --pull --build-arg AIRFLOW_BASE_TAG=3.2.0 \
     -t cccortesh/mlops-airflow:latest airflow/
   docker push cccortesh/mlops-airflow:latest
   ```
4. Volver a ejecutar el `helm upgrade` del [paso 5](#5-airflow) (o `cd airflow && ./deploy.sh`). Los pods se recrean con la nueva imagen.

### Capa de inferencia (API, Streamlit y Locust)

**FastAPI** sirve inferencia y métricas, **Streamlit** ofrece una UI que solo llama a la API, y **Locust** ejerce carga sobre los mismos endpoints.

#### Requisitos previos de inferencia

1. PostgreSQL desplegado con `inference_db` y la tabla `inference.inference_logs`.
2. MLflow y MinIO operativos, con al menos una versión del modelo registrada como `diabetes-model` y el alias `champion` asignado por el DAG `training_pipeline`. Sin `champion`, la API queda en `degraded`.
3. Manifiestos aplicados (`k8s/api/`, `k8s/streamlit/`, `k8s/locust/`).

#### API de inferencia (FastAPI)

| Módulo (`api/app/`) | Rol |
|---------------------|-----|
| `main.py` | Rutas HTTP, instrumentación Prometheus |
| `schemas.py` | Modelos Pydantic |
| `model_loader.py` | Carga `models:/<MODEL_NAME>@champion` y orden de columnas |
| `database.py` | Inserción en `inference.inference_logs` |
| `metrics.py` | Métricas `mlops_predict_latency_seconds`, `mlops_predict_requests_total`, `mlops_predict_errors_total` |

**Endpoints principales**

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET` | `/` | Metadatos del servicio |
| `GET` | `/health` | Estado `ok`/`degraded` y versión actual |
| `GET` | `/model-info` | Nombre, versión, alias y `feature_names` |
| `GET` | `/example-features` | Plantilla JSON con todas las columnas en `0.0` |
| `POST` | `/predict` | Inferencia |
| `GET` | `/metrics` | Métricas Prometheus |

**Contrato de `/predict`**: JSON con `features` (mapa columna → número). Faltan claves o sobran → `422` con `missing_features` / `extra_features` en el detalle. Respuesta exitosa incluye `prediction`, `probability`, `model_name`, `model_version`, `response_time_ms` y `request_id`.

**Recarga del modelo**: cada ~60 s bajo tráfico se comprueba si el alias `champion` apunta a una nueva versión y se recarga sin reiniciar el pod.

**Prueba local**:

```bash
cd api
uvicorn app.main:app --host 0.0.0.0 --port 8000
# Docs: http://localhost:8000/docs
```

#### Streamlit

- Código: `streamlit/app/app.py`.
- Solo HTTP hacia la API (`requests`); no consume MLflow.
- Configurado con `API_URL` (ConfigMap `streamlit-config`).
- Flujo sugerido: `/health` → `/model-info` → `/example-features` → `/predict`.

```bash
cd streamlit
streamlit run app/app.py --server.port 8501
```

#### Locust

- Código: `locust/locustfile.py`.
- `on_start` solicita `/example-features` para construir el payload de `/predict`.
- Mezcla `POST /predict`, `GET /health`, `GET /model-info`.

**Modo headless**:

```bash
docker run --rm -e LOCUST_HOST=http://api-service:8000 cccortesh/mlops-locust:latest \
  locust -f locustfile.py --host http://api-service:8000 --headless -u 10 -r 2 -t 60s
```

---

## Implementación de los DAGs

El proyecto usa dos DAGs coordinados mediante **Datasets** (data-aware scheduling) de Airflow 2.4+. Esto garantiza que el entrenamiento solo se ejecute cuando hay datos nuevos, eliminando ejecuciones innecesarias.

### Arquitectura

```
ingestion_pipeline (cada 5 min)          training_pipeline (disparado por Dataset)
┌──────────────────────┐                 ┌──────────────────────────┐
│ validate_source      │                 │ preprocess_and_store     │
│        ↓             │                 │        ↓                 │
│ load_raw_batch       │  ──Dataset──→   │ train_and_promote        │
│        ↓             │   (outlet)      │                          │
│ validate_quality     │                 │                          │
│   [outlet: Dataset]  │                 │                          │
└──────────────────────┘                 └──────────────────────────┘
     raw_data_db                          clean_data_db    MLflow
```

### Orquestación con Datasets

```python
from airflow import Dataset
DIABETES_RAW_DATASET = Dataset("postgres://mlops/raw/diabetes_raw")
```

**Por qué Datasets en lugar de cron + sensores:**

- **Sin ejecuciones vacías**: el training solo se dispara cuando la ingesta realmente insertó registros.
- **Sin polling**: no se necesita `ExternalTaskSensor` ni `SqlSensor` consumiendo slots.
- **Acoplamiento semántico**: la dependencia se expresa en términos del dato producido (`raw.diabetes_raw`), no de un horario arbitrario.
- **Protección contra batches vacíos**: cuando `load_raw_batch` detecta que todos los registros ya fueron cargados, lanza `AirflowSkipException`, el skip se propaga a `validate_quality`, el Dataset no se marca actualizado y el training no corre.

**Flujo:**

1. `ingestion_pipeline` corre cada 5 min (`*/5 * * * *`).
2. `load_raw_batch` inserta un lote nuevo en `raw.diabetes_raw`.
3. `validate_quality` valida y, al completarse con `outlets=[DIABETES_RAW_DATASET]`, marca el Dataset actualizado.
4. `training_pipeline` (con `schedule=[DIABETES_RAW_DATASET]`) se dispara.
5. Si no hay datos nuevos → skip → Dataset sin actualizar → no hay training.

La columna `status` en `raw.diabetes_raw` (`loaded` → `processed`) coordina el estado entre DAGs.

### Estructura de archivos

```
dags/
├── ingestion_pipeline.py
├── training_pipeline.py
└── tasks/
    ├── config.py               # Engines de BD y configuración compartida
    ├── validate_source.py      # Descarga y valida el CSV fuente
    ├── load_raw_batch.py       # Carga incremental a raw
    ├── validate_quality.py     # Validación del batch
    ├── preprocess_data.py      # Limpieza, encoding y split
    ├── store_clean_data.py     # Guarda en clean y marca raw como processed
    └── train_and_promote.py    # Entrena, registra en MLflow y promueve
```

### DAG 1: `ingestion_pipeline`

Ejecuta cada 5 minutos. Carga lotes de máximo 15,000 registros del CSV fuente a `raw.diabetes_raw`. Se detiene automáticamente cuando todo el dataset está cargado.

| Tarea | Descripción |
|-------|-------------|
| `validate_source` | Verifica que el CSV existe en `/tmp/`. Si no, lo descarga desde Google Drive |
| `load_raw_batch` | Inserta el siguiente lote con metadatos: `batch_id`, `load_timestamp`, `source_file`, `row_hash`, `status` |
| `validate_quality` | Verifica conteo del batch, límite de registros y duplicados por `row_hash` |

La tabla `raw.diabetes_raw` se crea en la primera ejecución con los nombres exactos del CSV.

### DAG 2: `training_pipeline`

Disparado por el Dataset `postgres://mlops/raw/diabetes_raw`.

| Tarea | Descripción |
|-------|-------------|
| `preprocess_and_store` | Ejecuta en una sola tarea la lógica de `preprocess_data.py` (lee `loaded`, limpieza, encoding, `split`) y `store_clean_data.py` (append a `clean.diabetes_clean`, marca raw como `processed`). Así el parquet intermedio en `/tmp` no se pierde entre reintentos. |
| `train_and_promote` | Lee todos los datos de clean, separa por `split`, entrena 3 modelos, registra cada uno en MLflow y promueve el mejor con alias `champion` usando **recall** como métrica de selección |

### Modelos entrenados

| Modelo | Características |
|--------|----------------|
| LogisticRegression | `max_iter=1000`, `class_weight='balanced'` |
| XGBoost | `n_estimators=100`, `max_depth=6`, `scale_pos_weight` ajustado |
| LightGBM | `n_estimators=100`, `max_depth=6`, `class_weight='balanced'` |

### Métrica principal: Recall

Se usa recall como métrica de selección porque en un contexto clínico el costo de un falso negativo (no detectar a un paciente que será readmitido en menos de 30 días y por tanto no intervenir preventivamente) es mayor que el de un falso positivo (una revisión adicional innecesaria). ROC-AUC mide la capacidad discriminativa general pero no penaliza directamente los falsos negativos; recall sí.

Se registran también en MLflow las métricas complementarias (ROC-AUC, F1, precision, accuracy) para análisis.

### Promoción del modelo

1. Se identifica el mejor candidato por `val_roc_auc`.
2. Se consulta el modelo productivo actual (alias `champion`).
3. Si el candidato supera al productivo, se le asigna el alias `champion`.
4. Si no hay productivo previo, el primer modelo se promueve automáticamente.

La API consulta dinámicamente el modelo con alias `champion`.

---

## Troubleshooting

| Problema | Causa | Solución |
|----------|-------|----------|
| `kubectl` → `localhost:8080: connection refused` | No hay clúster activo | Crear el clúster con `kind create cluster` o iniciar Docker Desktop / Minikube |
| `port-forward` → `address already in use` | Puerto local ocupado (ej. PostgreSQL del sistema) | Usar otro puerto local: `kubectl port-forward svc/postgres-service 5433:5432 -n mlops` |
| `Init:CrashLoopBackOff` en pods de Airflow | Migraciones no aplicadas | Verificar que `migrateDatabaseJob.enabled: true` en `values-local.yaml`. Si persiste, correr manualmente: `kubectl run airflow-migrate --rm -it --namespace mlops --image cccortesh/mlops-airflow:latest --restart=Never --env="AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres@airflow-postgresql:5432/postgres" -- airflow db migrate` |
| `helm list -n mlops` vacío tras instalar | Helm con `--wait` se bloqueó y no registró el release | No usar `--wait`. Borrar recursos huérfanos con `kubectl delete pods,deployments,statefulsets,services,configmaps,secrets,jobs -l release=airflow -n mlops` y reinstalar |
| `OOMKilled` en MLflow | Memoria insuficiente | Subir `resources.limits.memory` en `k8s/mlflow/deployment.yaml` |
| `Invalid Host header` en MLflow | Protección DNS rebinding | Agregar `--allowed-hosts all` en args del deployment |
| `password authentication failed` en Postgres | PVC conserva password anterior | Borrar StatefulSet y PVC, redesplegar |
| `ImagePullBackOff` | Imagen no existe o tag incorrecto | Verificar con `docker pull cccortesh/mlops-airflow:latest` |
| No aparece el DAG nuevo | Imagen vieja en los pods | Reconstruir, push y `helm upgrade` |
| API en `degraded` o `/predict` con **503** | No hay versión con alias `champion` | Correr `training_pipeline` hasta que `train_and_promote` promueva un modelo |
| `/predict` con **422** | JSON no coincide con columnas del modelo | Usar `GET /example-features` o las claves de `GET /model-info` |

---

## Limpieza

```bash
# Airflow (Helm)
helm uninstall airflow -n mlops

# Componentes desplegados con kubectl (orden inverso)
kubectl delete -f k8s/grafana/
kubectl delete -f k8s/prometheus/
kubectl delete -f k8s/locust/
kubectl delete -f k8s/streamlit/
kubectl delete -f k8s/api/
kubectl delete -f k8s/mlflow/
kubectl delete -f k8s/mlflow-postgres/
kubectl delete -f k8s/minio/
kubectl delete -f k8s/postgres/
kubectl delete -f k8s/namespace/

# Clúster
kind delete cluster
```

---

## 👥 Colaboradores

- 🧑‍💻 **Camilo Cortés** — [![GitHub](https://img.shields.io/badge/GitHub-@cccortesh-181717?logo=github)](https://github.com/cccortesh)

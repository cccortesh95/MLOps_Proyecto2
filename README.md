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
- [Despliegue de PostgreSQL](#despliegue-de-postgresql)
- [Despliegue de MinIO](#despliegue-de-minio)
- [Despliegue de MLflow](#despliegue-de-mlflow)
- [Despliegue de Airflow](#despliegue-de-airflow)
- [Capa de inferencia (API, Streamlit y Locust)](#capa-de-inferencia-api-streamlit-y-locust)
- [Actualización de DAGs](#actualización-de-dags)
- [Implementación de los DAGs](#implementación-de-los-dags)
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
| `postgres` | Base de datos del proyecto (raw, clean, inference logs) | `5432` |
| `mlflow-postgres` | Base de datos de metadata de MLflow | `5432` |
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
├── mlflow/
│   └── Dockerfile
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
├── prometheus/
│   └── prometheus.yml
└── README.md
```

---

## Imágenes en DockerHub

Todas las imágenes custom del proyecto se publican en DockerHub. Los manifiestos de Kubernetes las referencian directamente desde el registro, sin depender de imágenes locales.

| Imagen | Dockerfile | Descripción |
|--------|------------|-------------|
| `cccortesh/mlops-airflow:latest` | `airflow/Dockerfile` | Airflow con DAGs y dependencias del proyecto |
| `cccortesh/mlops-mlflow:latest` | `mlflow/Dockerfile` | MLflow server con psycopg2 y boto3 |
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

### API de inferencia, Streamlit y Locust

Estas variables las inyectan los manifiestos en `k8s/api/`, `k8s/streamlit/` y `k8s/locust/` (ConfigMap / Secret). Sirven para alinear la inferencia con MLflow y con la base `inference_db`.

**API (`api-config` + `api-secret`)**

| Variable | Descripción |
|----------|-------------|
| `MLFLOW_TRACKING_URI` | URI del servidor MLflow (p. ej. `http://mlflow-service:5000`) |
| `MLFLOW_S3_ENDPOINT_URL` | Endpoint S3-compatible de MinIO para descargar artefactos |
| `MODEL_NAME` | Nombre del modelo en el Model Registry (por defecto `diabetes-model`) |
| `DB_HOST` | Host de PostgreSQL donde está `inference_db` |
| `DB_PORT` | Puerto PostgreSQL (`5432`) |
| `DB_NAME` | Base de datos de logs (`inference_db`) |
| `DB_USER` / `DB_PASSWORD` | Credenciales (Secret `api-secret`) |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | Credenciales MinIO para que MLflow/Boto puedan leer artefactos |

**Streamlit (`streamlit-config`)**

| Variable | Descripción |
|----------|-------------|
| `API_URL` | URL base de la API dentro del clúster (p. ej. `http://api-service:8000`). La UI solo habla con FastAPI, no con MLflow. |

**Locust (`locust-config`)**

| Variable | Descripción |
|----------|-------------|
| `LOCUST_HOST` | URL base contra la que se genera carga (p. ej. `http://api-service:8000`). El `Dockerfile` de Locust usa esta variable en el comando de arranque. |

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
```

### Airflow

```bash
cd airflow
docker build --pull --build-arg AIRFLOW_BASE_TAG=3.2.0 \
  -t $DOCKERHUB_USER/mlops-airflow:latest .
docker push $DOCKERHUB_USER/mlops-airflow:latest
cd ..
```

### MLflow

```bash
cd mlflow
docker build -t $DOCKERHUB_USER/mlops-mlflow:latest .
docker push $DOCKERHUB_USER/mlops-mlflow:latest
cd ..
```

### API de inferencia

```bash
cd api
docker build -t $DOCKERHUB_USER/mlops-api:latest .
docker push $DOCKERHUB_USER/mlops-api:latest
cd ..
```

### Streamlit

```bash
cd streamlit
docker build -t $DOCKERHUB_USER/mlops-streamlit:latest .
docker push $DOCKERHUB_USER/mlops-streamlit:latest
cd ..
```

### Locust

```bash
cd locust
docker build -t $DOCKERHUB_USER/mlops-locust:latest .
docker push $DOCKERHUB_USER/mlops-locust:latest
cd ..
```

---

## Despliegue de la solución

> **Antes de desplegar**, todas las imágenes custom deben estar publicadas en DockerHub. Ver sección [Build y push de imágenes](#build-y-push-de-imágenes).

### 1. Infraestructura base

```bash
kubectl apply -f k8s/namespace/
kubectl apply -f k8s/postgres/
kubectl apply -f k8s/mlflow-postgres/
kubectl apply -f k8s/minio/
```

### 2. MLflow

```bash
kubectl apply -f k8s/mlflow/
```

### 3. Airflow

Ver sección [Despliegue de Airflow](#despliegue-de-airflow). La imagen `cccortesh/mlops-airflow` se jala directamente desde DockerHub.

### 4. Servicios de inferencia

Las imágenes `cccortesh/mlops-api` y `cccortesh/mlops-streamlit` se jalan desde DockerHub.

```bash
kubectl apply -f k8s/api/
kubectl apply -f k8s/streamlit/
```

Comportamiento funcional, endpoints y pruebas de carga: ver [Capa de inferencia (API, Streamlit y Locust)](#capa-de-inferencia-api-streamlit-y-locust).

### 5. Observabilidad y pruebas de carga

La imagen `cccortesh/mlops-locust` se jala desde DockerHub. Prometheus y Grafana usan imágenes oficiales.

```bash
kubectl apply -f k8s/prometheus/
kubectl apply -f k8s/grafana/
kubectl apply -f k8s/locust/
```

---

## Capa de inferencia (API, Streamlit y Locust)

Esta capa implementa el consumo del modelo ya promovido en MLflow: **FastAPI** sirve inferencia y métricas, **Streamlit** ofrece una UI que solo llama a la API, y **Locust** ejerce carga sobre los mismos endpoints. El código vive en `api/`, `streamlit/` y `locust/`.

### Requisitos previos (inferencia)

1. **PostgreSQL** desplegado con el script de init que crea `inference_db` y la tabla `inference.inference_logs` (ver [Despliegue de PostgreSQL](#despliegue-de-postgresql)).
2. **MLflow y MinIO** operativos, con al menos una versión del modelo registrada como `diabetes-model` y el alias **`champion`** asignado por el DAG `training_pipeline` (tarea `train_and_promote`). Sin `champion`, la API arranca en estado **degradado** (`/health`) hasta que exista un modelo promovido.
3. Imágenes publicadas y manifiestos aplicados (`k8s/api/`, `k8s/streamlit/`, `k8s/locust/`).

### API de inferencia (FastAPI)

| Módulo (`api/app/`) | Rol |
|---------------------|-----|
| `main.py` | Rutas HTTP, instrumentación Prometheus y orquestación de la petición |
| `schemas.py` | Modelos Pydantic de entrada/salida |
| `model_loader.py` | Resolución del URI `models:/<MODEL_NAME>@champion`, carga con `mlflow.sklearn.load_model` y orden de columnas desde `feature_names_in_` |
| `database.py` | Inserción de cada inferencia en `inference.inference_logs` (fallos de BD no cortan la respuesta al cliente) |
| `metrics.py` | Métricas propias: `mlops_predict_latency_seconds`, `mlops_predict_requests_total`, `mlops_predict_errors_total` |

**Endpoints principales**

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET` | `/` | Metadatos mínimos del servicio |
| `GET` | `/health` | Estado `ok` o `degradado`, si el modelo está cargado y versión actual |
| `GET` | `/model-info` | Nombre del modelo, versión, alias `champion`, lista de características esperadas |
| `GET` | `/example-features` | JSON con todas las características en `0.0` (plantilla para pruebas o UI) |
| `POST` | `/predict` | Inferencia; ver formato abajo |
| `GET` | `/metrics` | Métricas Prometheus (incluye histogramas/counters propios y las añadidas por `prometheus-fastapi-instrumentator`) |

**Contrato de `/predict`**

Cuerpo JSON con un objeto `features`: mapa **nombre de columna → número** (mismos nombres y orden semántico que en el entrenamiento: todas las columnas del modelo salvo `readmitted` y `split`). Faltan columnas o sobran claves no esperadas → **422** con `missing_features` / `extra_features` en el detalle.

Ejemplo mínimo (sustituir por la plantilla real de `/example-features`):

```json
{
  "features": {
    "race": 0.0,
    "gender": 1.0
  }
}
```

Respuesta exitosa incluye `prediction`, `probability` (si el estimador expone `predict_proba`), `model_name`, `model_version`, `response_time_ms` y `request_id` (también persistido en BD).

**Recarga del modelo**

Periódicamente (alrededor de cada 60 s bajo tráfico en `/predict`) se comprueba si el alias `champion` apunta a una **nueva versión** en el registry; en ese caso se vuelve a cargar el artefacto sin reiniciar el pod.

**Dependencias del contenedor**

La API incluye `xgboost` y `lightgbm` además de `scikit-learn`, para poder deserializar los mismos tipos de modelo que registra Airflow con `mlflow.sklearn.log_model`.

**Prueba local rápida** (con variables apuntando a MLflow/Postgres accesibles):

```bash
cd api
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Documentación interactiva: `http://localhost:8000/docs`.

### Streamlit

- **Código**: `streamlit/app/app.py`.
- **Comunicación**: solo **HTTP** hacia la API (`requests`). No se usa el tracking URI de MLflow desde la UI.
- **Configuración**: variable `API_URL` (en Kubernetes viene del ConfigMap `streamlit-config`, p. ej. `http://api-service:8000`).
- **Flujo sugerido**: comprobar `/health` → refrescar `/model-info` → obtener `/example-features` para rellenar el editor JSON → enviar `/predict` y revisar predicción y versión del modelo.

```bash
cd streamlit
streamlit run app/app.py --server.port 8501
```

### Locust (pruebas de carga)

- **Código**: `locust/locustfile.py`.
- **Host**: clase `HttpUser` con host por defecto desde `LOCUST_HOST` (ConfigMap `locust-config` en el clúster). El `Dockerfile` ejecuta Locust con `--host` tomando esa variable.
- **Comportamiento**: en `on_start` cada usuario virtual solicita `/example-features` y construye el cuerpo de `/predict`. Las tareas mezclan `POST /predict`, `GET /health` y `GET /model-info`. Si aún no hay modelo, los intentos a `/example-features` fallan y el usuario cae en un fallback mínimo (principalmente `/health`).

**UI de Locust (dentro del clúster)**

```bash
kubectl port-forward svc/locust-service 8089:8089 -n mlops
```

Abrir `http://localhost:8089`, fijar host si hace falta y arrancar la prueba.

**Modo headless (ejemplo)**

```bash
docker run --rm -e LOCUST_HOST=http://api-service:8000 cccortesh/mlops-locust:latest \
  locust -f locustfile.py --host http://api-service:8000 --headless -u 10 -r 2 -t 60s
```

(Ajustar `--host` a la URL alcanzable desde donde se ejecute el contenedor.)

### Exposición con port-forward

```bash
kubectl port-forward svc/api-service 8000:8000 -n mlops
kubectl port-forward svc/streamlit-service 8501:8501 -n mlops
kubectl port-forward svc/locust-service 8089:8089 -n mlops
```

---

## Despliegue de PostgreSQL

Se despliegan dos instancias de PostgreSQL en Kubernetes: una para los datos del proyecto (raw, clean, inference) y otra dedicada a la metadata de MLflow. Airflow cuenta con su propia instancia interna gestionada por Helm.

### Bases de datos

El proyecto usa 2 instancias de PostgreSQL en el namespace `mlops`, más la instancia interna de Airflow:

| Instancia | Base de datos | Esquema | Tabla | Propósito |
|---|---|---|---|---|
| `postgres` | `raw_data_db` | `raw` | `diabetes_raw` | Datos crudos sin transformar |
| `postgres` | `clean_data_db` | `clean` | `diabetes_clean` | Datos procesados para entrenamiento |
| `postgres` | `inference_db` | `inference` | `inference_logs` | Registros de inferencia de la API |
| `mlflow-postgres` | `mlflow_db` | `public` | — | Metadata de MLflow (backend store) |
| `airflow-postgresql` | `postgres` | `public` | — | Metadata de Airflow (viene con Helm, mismo namespace) |

La separación de MLflow en su propia instancia permite que las cargas de datos del DAG no afecten el rendimiento del tracking server, y que cada componente tenga su propio ciclo de vida de backups y mantenimiento.

### Manifiestos aplicados

**postgres (datos del proyecto)**

| Archivo | Recurso | Descripción |
|---------|---------|-------------|
| `secret.yaml` | Secret | Credenciales: usuario, contraseña, BD por defecto |
| `configmap.yaml` | ConfigMap | Host y puerto de conexión |
| `configmap-init.yaml` | ConfigMap | Script shell que crea las 3 BDs, esquemas y tablas |
| `pvc.yaml` | PersistentVolumeClaim | 5Gi de almacenamiento persistente |
| `statefulset.yaml` | StatefulSet | PostgreSQL 16 con resources, probes y volúmenes |
| `service.yaml` | Service (ClusterIP) | Exposición interna en puerto 5432 |

**mlflow-postgres (metadata de MLflow)**

| Archivo | Recurso | Descripción |
|---------|---------|-------------|
| `secret.yaml` | Secret | Credenciales de MLflow |
| `configmap.yaml` | ConfigMap | Host y puerto |
| `pvc.yaml` | PersistentVolumeClaim | 2Gi de almacenamiento |
| `statefulset.yaml` | StatefulSet | PostgreSQL 16 con resources y probes |
| `service.yaml` | Service (ClusterIP) | Exposición interna en puerto 5432 |

### Despliegue

```bash
kubectl apply -f k8s/namespace/
kubectl apply -f k8s/postgres/
kubectl apply -f k8s/mlflow-postgres/
```

Verificar que el pod esté corriendo:

```bash
kubectl get pods -n mlops
```

Verificar que las bases de datos se crearon:

```bash
kubectl exec -it postgres-0 -n mlops -- psql -U mlops_user -d mlops_db -c "\l"
```

Verificar tablas en cada BD:

```bash
kubectl exec -it postgres-0 -n mlops -- psql -U mlops_user -d raw_data_db -c "\dt raw.*"
kubectl exec -it postgres-0 -n mlops -- psql -U mlops_user -d clean_data_db -c "\dt clean.*"
kubectl exec -it postgres-0 -n mlops -- psql -U mlops_user -d inference_db -c "\dt inference.*"
```

### Conexión local

Exponer el servicio con port-forward:

```bash
# Datos del proyecto
kubectl port-forward svc/postgres-service 5432:5432 -n mlops

# Metadata de MLflow
kubectl port-forward svc/mlflow-postgres-service 5433:5432 -n mlops
```

Datos de conexión (postgres de datos):

| Campo | Valor |
|-------|-------|
| Host | `localhost` |
| Port | `5432` |
| User | `mlops_user` |
| Password | `mlops1234` |
| Database | `raw_data_db` / `clean_data_db` / `inference_db` |

Datos de conexión (postgres de MLflow):

| Campo | Valor |
|-------|-------|
| Host | `localhost` |
| Port | `5433` |
| User | `mlflow_user` |
| Password | `mlflow1234` |
| Database | `mlflow_db` |

---

## Despliegue de MinIO

MinIO se despliega como artifact store para MLflow. Almacena los modelos serializados, métricas y artefactos generados durante la experimentación.

### Manifiestos aplicados

| Archivo | Recurso | Descripción |
|---------|---------|-------------|
| `secret.yaml` | Secret | Credenciales root de MinIO |
| `configmap.yaml` | ConfigMap | Endpoint y puerto de consola |
| `pvc.yaml` | PersistentVolumeClaim | 10Gi de almacenamiento |
| `statefulset.yaml` | StatefulSet | MinIO con resources, probes y volumen |
| `service.yaml` | Service (ClusterIP) | API en puerto 9000, consola en 9001 |
| `job-create-bucket.yaml` | Job | Crea el bucket `mlflow-artifacts` automáticamente |

### Despliegue

```bash
kubectl apply -f k8s/minio/
```

Verificar:

```bash
kubectl get pods -n mlops
kubectl get jobs -n mlops
kubectl logs job/minio-create-bucket -n mlops
```

### Acceso a la consola web

```bash
kubectl port-forward svc/minio-service 9001:9001 -n mlops
```

Abrir: http://localhost:9001

| Campo | Valor |
|-------|-------|
| User | `minioadmin` |
| Password | `minioadmin123` |

---

## Despliegue de MLflow

MLflow se despliega como tracking server y model registry. Usa PostgreSQL dedicado como backend store y MinIO como artifact store.

Se usa una imagen custom (`cccortesh/mlops-mlflow`) porque la imagen oficial de MLflow no incluye los drivers de conexión a PostgreSQL (`psycopg2`) ni a MinIO/S3 (`boto3`). Sin estas dependencias, MLflow no puede guardar experimentos en la base de datos ni almacenar artefactos en MinIO.

### Manifiestos aplicados

| Archivo | Recurso | Descripción |
|---------|---------|-------------|
| `secret.yaml` | Secret | Credenciales S3 (MinIO) y URI de conexión a PostgreSQL |
| `configmap.yaml` | ConfigMap | Tracking URI, endpoint S3, artifact root |
| `deployment.yaml` | Deployment | MLflow server con resources y probes |
| `service.yaml` | Service (ClusterIP) | Exposición interna en puerto 5000 |

### Despliegue

Primero construir y publicar la imagen:

```bash
cd mlflow
docker build -t cccortesh/mlops-mlflow:latest .
docker push cccortesh/mlops-mlflow:latest
cd ..
```

Aplicar manifiestos (requiere que `mlflow-postgres` y `minio` estén corriendo):

```bash
kubectl apply -f k8s/mlflow/
```

Verificar:

```bash
kubectl get pods -n mlops
```

### Acceso a la UI

```bash
kubectl port-forward svc/mlflow-service 5000:5000 -n mlops
```

Abrir: http://localhost:5000

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
export NAMESPACE=mlops
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
  --set images.airflow.tag=latest \
  --set images.airflow.pullPolicy=Always \
  -f values/values-local.yaml \
  --wait --timeout 20m
```

### Paso 4: Ejecutar migración de base de datos

La primera vez que se instala, los pods quedan en `Init:CrashLoopBackOff` esperando las migraciones. Ejecutar manualmente:

```bash
kubectl run airflow-migrate --rm -it \
  --namespace mlops \
  --image apache/airflow:3.2.0 \
  --restart=Never \
  --env="AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres@airflow-postgresql:5432/postgres" \
  -- airflow db migrate
```

Esperar a que los pods pasen a `Running`.

### Paso 5: Crear usuario admin

```bash
kubectl exec -it deployment/airflow-api-server -n mlops -- \
  airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

### Paso 6: Verificar

```bash
kubectl get pods -n $NAMESPACE
helm list -n $NAMESPACE
```

### Paso 7: Exponer UI

```bash
kubectl port-forward svc/$RELEASE_NAME-api-server 8080:8080 -n $NAMESPACE
```

Abrir: http://localhost:8080

---

## Actualización de DAGs

1. Editar archivos en `airflow/dags/`
2. Actualizar `airflow/requirements.txt` si hay nuevas librerías
3. Reconstruir y publicar:
   ```bash
   cd airflow
   docker build --pull --build-arg AIRFLOW_BASE_TAG=3.2.0 \
     -t cccortesh/mlops-airflow:latest .
   docker push cccortesh/mlops-airflow:latest
   ```
4. Actualizar Helm:
   ```bash
   helm upgrade --install $RELEASE_NAME apache-airflow/airflow \
     --namespace $NAMESPACE \
     --set images.airflow.repository=cccortesh/mlops-airflow \
     --set images.airflow.tag=latest \
     --set images.airflow.pullPolicy=Always \
     -f values/values-local.yaml \
     --wait --timeout 20m
   ```

---

## Implementación de los DAGs

El proyecto usa dos DAGs independientes que simulan un escenario real de producción donde los datos llegan de forma continua y el modelo se reentrena periódicamente con datos acumulados.

### Arquitectura de los DAGs

```
ingestion_pipeline (cada 5 min)          training_pipeline (cada 5 min + 10s)
┌──────────────────────┐                 ┌──────────────────────────┐
│ validate_source      │                 │ preprocess_and_store     │
│        ↓             │                 │ (preprocess + store en   │
│ load_raw_batch       │  ──status──→    │  un solo operador)       │
│        ↓             │   column        │        ↓                 │
│ validate_quality     │                 │ train_and_promote        │
└──────────────────────┘                 └──────────────────────────┘
     raw_data_db                          clean_data_db    MLflow
```

Los DAGs no se comunican entre sí directamente. La coordinación se hace a través de la columna `status` en la tabla `raw.diabetes_raw`:
- `loaded` → dato nuevo, pendiente de procesar
- `processed` → dato ya procesado y almacenado en clean

### Estructura de archivos

Cada tarea está en su propio archivo Python dentro de `airflow/dags/tasks/`:

```
dags/
├── ingestion_pipeline.py       # DAG de ingesta (define el flujo)
├── training_pipeline.py        # DAG de entrenamiento (define el flujo)
└── tasks/
    ├── __init__.py
    ├── config.py               # Configuración compartida y engines de BD
    ├── validate_source.py      # Descarga y valida el CSV fuente
    ├── load_raw_batch.py       # Carga incremental a raw
    ├── validate_quality.py     # Validación del batch
    ├── preprocess_data.py      # Limpieza, encoding y asignación de split
    ├── store_clean_data.py     # Guarda en clean y marca raw como processed
    └── train_and_promote.py    # Entrena, registra en MLflow y promueve
```

### DAG 1: `ingestion_pipeline`

Simula la llegada incremental de datos. Se ejecuta cada 5 minutos y carga un lote de máximo 15,000 registros del CSV fuente a la tabla `raw.diabetes_raw`. Se detiene automáticamente cuando todo el dataset está cargado.

| Tarea | Descripción |
|-------|-------------|
| `validate_source` | Verifica que el CSV existe en `/tmp/`. Si no, lo descarga desde Google Drive |
| `load_raw_batch` | Lee el siguiente lote de 15,000 registros y los inserta en `raw.diabetes_raw` con metadatos: `batch_id`, `load_timestamp`, `source_file`, `row_hash`, `status` |
| `validate_quality` | Verifica conteo del batch, límite de registros y duplicados por `row_hash` |

La tabla `raw.diabetes_raw` se crea automáticamente en la primera ejecución con los nombres exactos de las columnas del CSV.

### DAG 2: `training_pipeline`

Procesa los datos nuevos y reentrena los modelos. Se ejecuta cada 5 minutos con 10 segundos de desfase respecto al ingestion, para que los datos ya estén disponibles.

| Tarea | Descripción |
|-------|-------------|
| `preprocess_and_store` | Una sola tarea que ejecuta en secuencia la lógica de `preprocess_data.py` (lee `loaded` en raw, limpieza, encoding, `split`) y `store_clean_data.py` (append a `clean.diabetes_clean`, marca raw como `processed`). Así el parquet intermedio en `/tmp` no se pierde entre reintentos o reinicios del scheduler entre dos operadores distintos. |
| `train_and_promote` | Lee **todos** los datos acumulados de clean, los separa por la columna `split`, entrena 3 modelos (LogisticRegression, XGBoost, LightGBM), registra cada uno en MLflow con métricas y parámetros, compara contra el modelo productivo actual y promueve el mejor con el alias `champion` usando recall como métrica de selección |

### Modelos entrenados

| Modelo | Características |
|--------|----------------|
| LogisticRegression | `max_iter=1000`, `class_weight='balanced'` |
| XGBoost | `n_estimators=100`, `max_depth=6`, `scale_pos_weight` ajustado al desbalance |
| LightGBM | `n_estimators=100`, `max_depth=6`, `class_weight='balanced'` |

### Métrica principal: Recall (sensibilidad)

Se usa recall como métrica de selección del mejor modelo porque en un contexto clínico el costo de un falso negativo es significativamente mayor que el de un falso positivo. No detectar a un paciente que será readmitido en menos de 30 días implica que no recibe intervención preventiva y termina hospitalizado de nuevo, con el costo económico y de salud que eso conlleva. Un falso positivo (alertar sobre un paciente que no será readmitido) solo genera una revisión adicional, que es un costo menor.

ROC-AUC mide la capacidad discriminativa general del modelo pero no penaliza directamente los falsos negativos. Recall sí: mide qué proporción de los casos positivos reales fueron correctamente identificados. En problemas de clasificación clínica con clases desbalanceadas, maximizar recall es la prioridad.

Se registran además en MLflow las métricas complementarias (ROC-AUC, F1, precision, accuracy) para análisis posterior.

### Promoción del modelo

La promoción no es automática de MLflow — se implementa en el código. Después de entrenar los 3 modelos:

1. Se identifica el mejor por `val_roc_auc`
2. Se consulta el modelo productivo actual (alias `champion`) en MLflow
3. Si el candidato supera al productivo, se le asigna el alias `champion`
4. Si no hay modelo productivo previo, el primer modelo entrenado se promueve automáticamente

La API de inferencia consulta dinámicamente el modelo con alias `champion` desde MLflow.

---

## Troubleshooting

| Problema | Causa | Solución |
|----------|-------|----------|
| `Init:CrashLoopBackOff` en pods de Airflow | Migraciones de BD no aplicadas | Ejecutar `airflow db migrate` manualmente (ver Paso 4 de Airflow) |
| `OOMKilled` en MLflow | Memoria insuficiente | Aumentar `resources.limits.memory` en el deployment |
| `Invalid Host header` en MLflow | MLflow rechaza requests por protección DNS rebinding | Agregar `--allowed-hosts all` en los args del deployment de MLflow |
| `password authentication failed` | PVC conserva password anterior | Borrar StatefulSet y PVC, redesplegar |
| CrashLoopBackOff en `airflow-api-server` | Incompatibilidad entre versión del chart y la imagen | Alinear `AIRFLOW_BASE_TAG` con `APP VERSION` del chart |
| ImagePullBackOff | Imagen no existe en DockerHub o tag incorrecto | Verificar con `docker pull cccortesh/mlops-airflow:latest` |
| No aparece el DAG nuevo | Imagen vieja en los pods | Reconstruir, push a DockerHub y `helm upgrade` |
| Port-forward no funciona | Servicio no encontrado | Verificar con `kubectl get svc -n mlops` |
| API en `degraded` o `/predict` con **503** | No existe versión con alias `champion` para `diabetes-model` | Ejecutar el DAG `training_pipeline` hasta que `train_and_promote` registre y promueva un modelo; revisar MLflow → Models |
| `/predict` con **422** (missing/extra features) | JSON no coincide con las columnas del modelo | Usar `GET /example-features` o copiar claves exactas desde `GET /model-info` → `feature_names` |

---

## Limpieza

```bash
# Desinstalar Airflow
helm uninstall airflow -n mlops

# Eliminar componentes
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

# Eliminar clúster
kind delete cluster
```

---

## 👥 Colaboradores

- 🧑‍💻 **Camilo Cortés** — [![GitHub](https://img.shields.io/badge/GitHub-@cccortesh-181717?logo=github)](https://github.com/cccortesh)

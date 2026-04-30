# Airflow en Kubernetes

Airflow se despliega usando el **Helm Chart oficial** de Apache Airflow,
siguiendo la guía del curso:
https://github.com/CristianDiazAlvarez/MLOPS_PUJ/tree/main/Niveles/3/Airflow

## Despliegue rápido

```bash
# Desde el directorio airflow/
cd airflow
bash deploy.sh
```

## Despliegue paso a paso

```bash
# 1. Agregar repo Helm
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# 2. Crear namespace
export NAMESPACE=airflow-local
export RELEASE_NAME=airflow
kubectl create namespace $NAMESPACE

# 3. Construir imagen custom con DAGs
cd airflow
export AIRFLOW_BASE_TAG=3.1.8
docker build --pull \
  --build-arg AIRFLOW_BASE_TAG=$AIRFLOW_BASE_TAG \
  --tag airflow-local-dags:0.0.1 .

# 4. Cargar imagen en clúster (ejemplo con kind)
kind load docker-image airflow-local-dags:0.0.1

# 5. Instalar con Helm
helm upgrade --install $RELEASE_NAME apache-airflow/airflow \
  --namespace $NAMESPACE \
  --set images.airflow.repository=airflow-local-dags \
  --set images.airflow.tag=0.0.1 \
  -f values/values-local.yaml \
  --wait --timeout 20m

# 6. Exponer UI
kubectl port-forward svc/$RELEASE_NAME-api-server 8080:8080 -n $NAMESPACE
```

## Actualizar DAGs

1. Editar archivos en `dags/`
2. Actualizar `requirements.txt` si hay nuevas dependencias
3. Reconstruir imagen con nuevo tag (0.0.2, 0.0.3, ...)
4. Cargar imagen en clúster
5. `helm upgrade ...` con el nuevo tag

## DAGs incluidos

| DAG | Descripción |
|---|---|
| `hello_level3` | DAG de prueba para verificar despliegue |
| `diabetes_pipeline` | Pipeline MLOps completo (10 tareas) |

# Airflow en Kubernetes

Airflow se despliega usando el Helm Chart oficial de Apache Airflow
con una imagen custom publicada en DockerHub.

Todos los componentes corren en el namespace `mlops`.

## Despliegue

```bash
export NAMESPACE=mlops
export RELEASE_NAME=airflow

helm repo add apache-airflow https://airflow.apache.org
helm repo update

cd airflow
helm upgrade --install $RELEASE_NAME apache-airflow/airflow \
  --namespace $NAMESPACE \
  --set images.airflow.repository=cccortesh/mlops-airflow \
  --set images.airflow.tag=latest \
  --set images.airflow.pullPolicy=Always \
  -f values/values-local.yaml \
  --wait --timeout 20m
```

## Exponer UI

```bash
kubectl port-forward svc/airflow-api-server 8080:8080 -n mlops
```

Abrir: http://localhost:8080

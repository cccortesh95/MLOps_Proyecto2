"""
DAG: training_pipeline
Procesa datos nuevos, los almacena en clean y reentrena modelos.

Flujo:
  1. preprocess_and_store → En un solo proceso: limpia raw (parquet) y persiste en clean
                             + marca raw como processed. Evita perder el parquet en /tmp
                             entre reintentos o reinicios del scheduler.
  2. train_and_promote    → Entrena con datos acumulados en clean, MLflow, promoción champion
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator

# Airflow 3 no siempre incluye /opt/airflow/dags en PYTHONPATH.
# Agregamos el directorio del DAG para resolver `from tasks import ...`.
DAGS_DIR = os.path.dirname(__file__)
if DAGS_DIR not in sys.path:
    sys.path.append(DAGS_DIR)

from tasks import preprocess_data, store_clean_data, train_and_promote

# Mismo Dataset definido en ingestion_pipeline.
# El DAG se dispara cuando ingestion produce datos nuevos en esta tabla.
DIABETES_RAW_DATASET = Dataset("postgres://mlops/raw/diabetes_raw")


def _preprocess_and_store(**kwargs):
    """Ejecuta preprocess y store en la misma tarea para que el parquet siga existiendo."""
    preprocess_data.run(**kwargs)
    store_clean_data.run(**kwargs)

default_args = {
    "owner": "mlops",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="training_pipeline",
    default_args=default_args,
    description="Procesamiento de datos nuevos y reentrenamiento de modelos",
    start_date=datetime(2025, 1, 1),
    schedule=[DIABETES_RAW_DATASET],
    catchup=False,
    tags=["mlops", "diabetes", "training"],
    max_active_runs=1,
) as dag:

    t_prep = PythonOperator(
        task_id="preprocess_and_store",
        python_callable=_preprocess_and_store,
    )
    t_train = PythonOperator(task_id="train_and_promote", python_callable=train_and_promote.run)

    t_prep >> t_train

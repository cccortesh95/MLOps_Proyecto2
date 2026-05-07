"""
DAG: training_pipeline
Procesa datos nuevos, los almacena en clean y reentrena modelos.

Flujo:
  1. preprocess_and_store → Limpia raw y persiste en clean + marca raw como processed
  2. train_and_promote    → Entrena con datos acumulados en clean, MLflow, promoción champion
"""

import os
import sys

from datetime import datetime, timedelta

from airflow.sdk import Asset, DAG
from airflow.providers.standard.operators.python import PythonOperator

# Airflow 3 no siempre incluye /opt/airflow/dags en PYTHONPATH.
DAGS_DIR = os.path.dirname(__file__)
if DAGS_DIR not in sys.path:
    sys.path.append(DAGS_DIR)

from tasks import preprocess_data, store_clean_data, train_and_promote

# Mismo Asset definido en ingestion_pipeline.
# El DAG se dispara cuando ingestion produce datos nuevos.
DIABETES_RAW_ASSET = Asset("diabetes_raw_data")


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
    schedule=[DIABETES_RAW_ASSET],
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

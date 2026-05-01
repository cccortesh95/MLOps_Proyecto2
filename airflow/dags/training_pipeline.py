"""
DAG: training_pipeline
Procesa datos nuevos, los almacena en clean y reentrena modelos.

Flujo:
  1. preprocess_data    → Lee registros 'loaded' de raw, limpia y asigna split
  2. store_clean_data   → Guarda en clean con columna split, marca raw como processed
  3. train_and_promote  → Entrena con todos los datos acumulados en clean,
                          registra en MLflow, compara y promueve si mejora
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from tasks import preprocess_data, store_clean_data, train_and_promote

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
    start_date=datetime(2025, 1, 1, 0, 0, 10),
    schedule=timedelta(minutes=5),
    catchup=False,
    tags=["mlops", "diabetes", "training"],
    max_active_runs=1,
) as dag:

    t1 = PythonOperator(task_id="preprocess_data", python_callable=preprocess_data.run)
    t2 = PythonOperator(task_id="store_clean_data", python_callable=store_clean_data.run)
    t3 = PythonOperator(task_id="train_and_promote", python_callable=train_and_promote.run)

    t1 >> t2 >> t3

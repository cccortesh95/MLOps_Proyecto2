"""
DAG: ingestion_pipeline
Carga incremental de datos crudos cada 5 minutos.

Flujo:
  1. validate_source   → Verifica que el CSV fuente existe
  2. load_raw_batch    → Carga siguiente lote de 15,000 registros a raw
  3. validate_quality  → Validación del batch cargado
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from tasks import validate_source, load_raw_batch, validate_quality

default_args = {
    "owner": "mlops",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ingestion_pipeline",
    default_args=default_args,
    description="Carga incremental de datos crudos por lotes",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["mlops", "diabetes", "ingestion"],
    max_active_runs=1,
) as dag:

    t1 = PythonOperator(task_id="validate_source", python_callable=validate_source.run)
    t2 = PythonOperator(task_id="load_raw_batch", python_callable=load_raw_batch.run)
    t3 = PythonOperator(task_id="validate_quality", python_callable=validate_quality.run)

    t1 >> t2 >> t3

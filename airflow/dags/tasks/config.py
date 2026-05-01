"""Configuración compartida para todas las tareas del DAG."""

import os

from sqlalchemy import create_engine

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
BATCH_SIZE = 15_000
DATA_SOURCE_URL = (
    "https://docs.google.com/uc?export=download"
    "&confirm=t&id=1k5-1caezQ3zWJbKaiMULTGq-3sz6uThC"
)
LOCAL_CSV_PATH = "/tmp/diabetes_raw.csv"
CLEAN_PARQUET_PATH = "/tmp/clean_batch.parquet"

EXPERIMENT_NAME = "diabetes_readmission"
MODEL_NAME = "diabetes-model"

TARGET_COL = "readmitted"
DROP_COLS = [
    "encounter_id",
    "patient_nbr",
    "weight",
    "payer_code",
    "medical_specialty",
    "examide",
    "citoglipton",
]


# ---------------------------------------------------------------------------
# Engines de base de datos
# ---------------------------------------------------------------------------
def _get_engine(db_name):
    host = os.getenv("DATA_DB_HOST", "postgres-service")
    port = os.getenv("DATA_DB_PORT", "5432")
    user = os.getenv("DATA_DB_USER", "mlops_user")
    pw = os.getenv("DATA_DB_PASSWORD", "mlops1234")
    return create_engine(
        f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db_name}"
    )


def get_raw_engine():
    return _get_engine(os.getenv("DATA_DB_NAME", "raw_data_db"))


def get_clean_engine():
    return _get_engine(os.getenv("CLEAN_DB_NAME", "clean_data_db"))


def get_inference_engine():
    return _get_engine(os.getenv("INFERENCE_DB_NAME", "inference_db"))

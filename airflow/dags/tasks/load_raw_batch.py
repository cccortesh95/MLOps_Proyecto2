"""Task 2: Carga incremental por lotes a tabla raw."""

import hashlib
import logging
from datetime import datetime

import pandas as pd
from airflow.exceptions import AirflowSkipException
from sqlalchemy import text

from tasks.config import BATCH_SIZE, LOCAL_CSV_PATH, get_raw_engine

logger = logging.getLogger(__name__)


def run(**kwargs):
    engine = get_raw_engine()
    df_full = pd.read_csv(LOCAL_CSV_PATH, na_values="?", low_memory=False)
    total_rows = len(df_full)
    logger.info(f"Total de registros en archivo fuente: {total_rows}")

    # Verificar si la tabla existe
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'raw' AND table_name = 'diabetes_raw')"
            )
        )
        table_exists = result.scalar()

    last_batch = 0
    loaded_count = 0

    if table_exists:
        with engine.connect() as conn:
            last_batch = conn.execute(
                text("SELECT COALESCE(MAX(batch_id), 0) FROM raw.diabetes_raw")
            ).scalar()
            loaded_count = conn.execute(
                text("SELECT COUNT(*) FROM raw.diabetes_raw")
            ).scalar()

    if loaded_count >= total_rows:
        logger.info("Todos los registros ya fueron cargados. Saltando ejecución.")
        raise AirflowSkipException("No hay datos nuevos para cargar.")

    start_idx = loaded_count
    end_idx = min(start_idx + BATCH_SIZE, total_rows)
    batch_df = df_full.iloc[start_idx:end_idx].copy()
    batch_id = last_batch + 1

    # Metadatos de carga
    batch_df["batch_id"] = batch_id
    batch_df["load_timestamp"] = datetime.utcnow()
    batch_df["source_file"] = "Diabetes.csv"
    batch_df["row_hash"] = batch_df.apply(
        lambda row: hashlib.md5(
            str(row.drop(["batch_id", "load_timestamp", "source_file"]).values).encode()
        ).hexdigest(),
        axis=1,
    )
    batch_df["status"] = "loaded"

    batch_df.to_sql("diabetes_raw", engine, schema="raw", if_exists="append", index=False)
    logger.info(
        f"Batch {batch_id}: cargados {len(batch_df)} registros "
        f"(filas {start_idx} a {end_idx - 1})"
    )

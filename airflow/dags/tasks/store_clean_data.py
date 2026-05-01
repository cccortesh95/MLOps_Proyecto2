"""Task: Almacenar datos procesados en tabla clean con columna split."""

import logging
import os
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from tasks.config import CLEAN_PARQUET_PATH, get_clean_engine, get_raw_engine

logger = logging.getLogger(__name__)


def run(**kwargs):
    if not os.path.isfile(CLEAN_PARQUET_PATH):
        logger.info("No hay archivo de datos limpios. Saltando.")
        return

    df = pd.read_parquet(CLEAN_PARQUET_PATH)
    if df.empty:
        logger.info("DataFrame vacío. Saltando.")
        return

    df["processed_timestamp"] = datetime.utcnow()

    engine = get_clean_engine()
    # Append: cada batch se agrega a la tabla acumulada
    df.to_sql("diabetes_clean", engine, schema="clean", if_exists="append", index=False)
    logger.info(f"Agregados {len(df)} registros a clean.diabetes_clean.")

    # Marcar registros raw como procesados
    raw_engine = get_raw_engine()
    with raw_engine.begin() as conn:
        conn.execute(
            text("UPDATE raw.diabetes_raw SET status = 'processed' WHERE status = 'loaded'")
        )
    logger.info("Registros raw marcados como 'processed'.")

"""Task: Preprocesamiento y transformación de datos nuevos.

Aplica limpieza general (drop columns, nulos, binarización del target) pero
NO codifica las variables categóricas. La codificación la hace el pipeline de
sklearn en la etapa de entrenamiento, para que el modelo registrado en MLflow
incluya las transformaciones y la API pueda recibir datos con categorías en texto.
"""

import logging

import numpy as np
import pandas as pd

from tasks.config import CLEAN_PARQUET_PATH, DROP_COLS, TARGET_COL, get_raw_engine

logger = logging.getLogger(__name__)


def run(**kwargs):
    engine = get_raw_engine()

    df = pd.read_sql(
        "SELECT * FROM raw.diabetes_raw WHERE status = 'loaded'", engine
    )
    logger.info(f"Registros crudos a procesar: {len(df)}")

    if df.empty:
        logger.info("No hay datos nuevos para procesar.")
        pd.DataFrame().to_parquet(CLEAN_PARQUET_PATH, index=False)
        return

    # Eliminar columnas de metadatos y de baja utilidad
    meta_cols = ["id", "batch_id", "load_timestamp", "source_file", "row_hash", "status"]
    cols_to_drop = [c for c in DROP_COLS + meta_cols if c in df.columns]
    df = df.drop(columns=cols_to_drop, errors="ignore")

    # Reemplazar '?' con NaN
    df = df.replace("?", pd.NA)

    # Eliminar filas con >50% nulos
    threshold = len(df.columns) * 0.5
    df = df.dropna(thresh=int(threshold))

    # Numéricas: rellenar con mediana
    for col in df.select_dtypes(include=["number"]).columns:
        df[col] = df[col].fillna(df[col].median())

    # Categóricas: rellenar con moda (NO codificar, eso lo hace el pipeline)
    for col in df.select_dtypes(include=["object"]).columns:
        if col != TARGET_COL:
            mode_val = df[col].mode()
            if not mode_val.empty:
                df[col] = df[col].fillna(mode_val.iloc[0])

    # Binarizar target: readmitted '<30' -> 1, resto -> 0
    if TARGET_COL in df.columns:
        df[TARGET_COL] = (df[TARGET_COL] == "<30").astype(int)

    # Asignar split: 70% train, 15% validation, 15% test
    rng = np.random.default_rng(seed=42)
    splits = rng.choice(["train", "validation", "test"], size=len(df), p=[0.70, 0.15, 0.15])
    df["split"] = splits

    logger.info(
        f"Datos procesados: {len(df)} filas. "
        f"Train={sum(splits == 'train')}, "
        f"Val={sum(splits == 'validation')}, "
        f"Test={sum(splits == 'test')}"
    )
    df.to_parquet(CLEAN_PARQUET_PATH, index=False)

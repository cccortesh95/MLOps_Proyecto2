"""Task 1: Validar disponibilidad del archivo fuente."""

import logging
import os

import pandas as pd
import requests

from tasks.config import DATA_SOURCE_URL, LOCAL_CSV_PATH

logger = logging.getLogger(__name__)


def run(**kwargs):
    if os.path.isfile(LOCAL_CSV_PATH):
        df = pd.read_csv(LOCAL_CSV_PATH, nrows=5)
        logger.info(f"Archivo fuente ya existe. Columnas: {list(df.columns)}")
        return

    logger.info("Descargando dataset desde Google Drive...")
    r = requests.get(DATA_SOURCE_URL, allow_redirects=True, stream=True, timeout=120)
    r.raise_for_status()
    with open(LOCAL_CSV_PATH, "wb") as f:
        f.write(r.content)

    df = pd.read_csv(LOCAL_CSV_PATH, nrows=5)
    logger.info(f"Dataset descargado. Columnas: {list(df.columns)}")

"""Task 3: Validación básica de calidad de datos."""

import logging

from sqlalchemy import text

from tasks.config import BATCH_SIZE, get_raw_engine

logger = logging.getLogger(__name__)


def run(**kwargs):
    engine = get_raw_engine()

    # Obtener el último batch cargado
    with engine.connect() as conn:
        batch_id = conn.execute(
            text("SELECT COALESCE(MAX(batch_id), 0) FROM raw.diabetes_raw")
        ).scalar()

    if batch_id == 0:
        logger.info("No hay datos en raw. Saltando validación.")
        return

    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM raw.diabetes_raw WHERE batch_id = :bid"),
            {"bid": batch_id},
        ).scalar()

    logger.info(f"Batch {batch_id}: {count} registros.")

    if count == 0:
        raise ValueError(f"Batch {batch_id} no tiene registros.")
    if count > BATCH_SIZE:
        raise ValueError(f"Batch {batch_id} excede el límite de {BATCH_SIZE}.")

    # Verificar duplicados por hash dentro del batch
    with engine.connect() as conn:
        duplicates = conn.execute(
            text(
                "SELECT COUNT(*) - COUNT(DISTINCT row_hash) "
                "FROM raw.diabetes_raw WHERE batch_id = :bid"
            ),
            {"bid": batch_id},
        ).scalar()

    if duplicates > 0:
        logger.warning(f"Batch {batch_id}: {duplicates} posibles duplicados.")

    logger.info(f"Validación OK para batch {batch_id}.")

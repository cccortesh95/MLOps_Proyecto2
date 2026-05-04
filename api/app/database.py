# =============================================================================
# Database - Registro de inferencias
# Cada solicitud de inferencia se registra en la tabla inference_logs.
# Campos: timestamp, input_data, prediction, score, model_name,
#          model_version, response_time, request_id
# =============================================================================

from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from typing import Any, Dict, Iterator, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_engine: Optional[Engine] = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        user = os.getenv("DB_USER", "mlops_user")
        password = os.getenv("DB_PASSWORD", "mlops1234")
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        db = os.getenv("DB_NAME", "inference_db")
        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
        _engine = create_engine(url, pool_pre_ping=True, pool_size=3, max_overflow=5)
    return _engine


@contextmanager
def db_connection() -> Iterator[Any]:
    eng = get_engine()
    conn = eng.connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def log_inference(
    *,
    request_id: str,
    input_data: Dict[str, Any],
    prediction: Optional[int],
    probability: Optional[float],
    model_name: str,
    model_version: str,
    response_time_ms: float,
    status_code: int,
) -> None:
    """Inserta un registro en inference.inference_logs. Fallos silenciosos (no bloquean la API)."""
    try:
        with db_connection() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO inference.inference_logs
                    (request_id, input_data, prediction, probability, model_name,
                     model_version, response_time_ms, status_code)
                    VALUES
                    (:request_id, CAST(:input_data AS jsonb), :prediction, :probability,
                     :model_name, :model_version, :response_time_ms, :status_code)
                    """
                ),
                {
                    "request_id": request_id,
                    "input_data": json.dumps(input_data),
                    "prediction": prediction,
                    "probability": probability,
                    "model_name": model_name,
                    "model_version": model_version,
                    "response_time_ms": response_time_ms,
                    "status_code": status_code,
                },
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("No se pudo registrar inferencia en BD: %s", exc)

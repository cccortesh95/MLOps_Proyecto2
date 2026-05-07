# =============================================================================
# API de Inferencia - FastAPI
# Endpoints:
#   /health      - Verificación de salud de la API
#   /predict     - Inferencia usando el modelo productivo de MLflow
#   /model-info  - Información del modelo actual (nombre, versión, estado)
#   /metrics     - Métricas compatibles con Prometheus
# =============================================================================

from __future__ import annotations

import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Set

from fastapi import FastAPI, HTTPException, status
from prometheus_fastapi_instrumentator import Instrumentator

from app.database import log_inference
from app.metrics import PREDICT_ERRORS_TOTAL, PREDICT_LATENCY_SECONDS, PREDICT_REQUESTS_TOTAL
from app.model_loader import ModelLoadError, model_service
from app.schemas import ErrorDetail, HealthResponse, ModelInfoResponse, PredictionRequest, PredictionResponse

logger = logging.getLogger(__name__)

_last_reload_monotonic: float = 0.0
RELOAD_CHECK_INTERVAL_SEC = 60.0


def _maybe_reload_model() -> None:
    global _last_reload_monotonic
    now = time.monotonic()
    if now - _last_reload_monotonic >= RELOAD_CHECK_INTERVAL_SEC:
        _last_reload_monotonic = now
        try:
            model_service.reload_if_needed()
        except Exception as exc:  # noqa: BLE001
            logger.debug("Comprobación de recarga de modelo: %s", exc)


def _validate_features(
    payload: Dict[str, Any], expected: List[str]
) -> Dict[str, Any]:
    exp: Set[str] = set(expected)
    got: Set[str] = set(payload.keys())
    missing = sorted(exp - got)
    extra = sorted(got - exp)
    if missing or extra:
        detail = ErrorDetail(
            error="Características no alineadas con el modelo",
            missing_features=missing or None,
            extra_features=extra or None,
        )
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=detail.model_dump())
    return {k: payload[k] for k in expected}


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=logging.INFO)
    try:
        model_service.load()
    except ModelLoadError as exc:
        logger.error("No se pudo cargar el modelo al iniciar: %s", exc)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Error inesperado al cargar el modelo: %s", exc)
    yield


app = FastAPI(
    title="API de Inferencia MLOps",
    description="Inferencia sobre el modelo de readmisión (alias MLflow `champion`).",
    version="1.0.0",
    lifespan=lifespan,
)

Instrumentator(
    should_group_status_codes=True,
    should_ignore_untemplated=True,
    should_instrument_requests_inprogress=True,
    excluded_handlers=["/metrics"],
).instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)


@app.get("/health", response_model=HealthResponse, tags=["operación"])
def health() -> HealthResponse:
    if model_service.is_ready:
        return HealthResponse(
            status="ok",
            model_loaded=True,
            model_name=model_service.model_name,
            model_version=model_service.version,
        )
    return HealthResponse(
        status="degraded",
        model_loaded=False,
        model_name=model_service.model_name,
        model_version=model_service.version,
        detail=model_service.last_error,
    )


@app.get("/model-info", response_model=ModelInfoResponse, tags=["modelo"])
def model_info() -> ModelInfoResponse:
    feats = model_service.feature_names
    return ModelInfoResponse(
        model_name=model_service.model_name,
        model_version=model_service.version or "none",
        alias="champion",
        loaded=model_service.is_ready,
        feature_names=feats,
        n_features=len(feats),
    )


@app.get("/", tags=["operación"])
def root() -> Dict[str, str]:
    return {"service": "inference-api", "docs": "/docs"}


@app.get("/example-features", tags=["modelo"])
def example_features() -> Dict[str, Any]:
    """
    Plantilla JSON con ceros para todas las características esperadas por el modelo.
    Útil para Streamlit y pruebas manuales.
    """
    if not model_service.is_ready:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Modelo no cargado; no se puede generar la plantilla.",
        )
    return {"features": {name: 0.0 for name in model_service.feature_names}}


@app.post(
    "/predict",
    response_model=PredictionResponse,
    responses={
        422: {"model": ErrorDetail},
        503: {"model": ErrorDetail},
    },
    tags=["inferencia"],
)
def predict(body: PredictionRequest) -> PredictionResponse:
    _maybe_reload_model()
    if not model_service.is_ready:
        PREDICT_ERRORS_TOTAL.labels(error_type="model_unavailable").inc()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Modelo no disponible. Entrene y promueva un modelo con alias 'champion' en MLflow.",
        )

    ordered = _validate_features(body.features, model_service.feature_names)

    request_id = str(uuid.uuid4())
    t0 = time.perf_counter()
    try:
        pred, proba = model_service.predict_row(ordered)
    except ModelLoadError as exc:
        PREDICT_ERRORS_TOTAL.labels(error_type="model_load").inc()
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        PREDICT_ERRORS_TOTAL.labels(error_type="inference").inc()
        elapsed_ms = (time.perf_counter() - t0) * 1000
        log_inference(
            request_id=request_id,
            input_data=body.features,
            prediction=None,
            probability=None,
            model_name=model_service.model_name,
            model_version=model_service.version or "unknown",
            response_time_ms=elapsed_ms,
            status_code=500,
        )
        logger.exception("Fallo en inferencia: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error interno al ejecutar el modelo.",
        ) from exc

    elapsed_ms = (time.perf_counter() - t0) * 1000
    PREDICT_LATENCY_SECONDS.observe(elapsed_ms / 1000.0)
    PREDICT_REQUESTS_TOTAL.labels(outcome="success").inc()

    log_inference(
        request_id=request_id,
        input_data=body.features,
        prediction=pred,
        probability=proba,
        model_name=model_service.model_name,
        model_version=model_service.version or "unknown",
        response_time_ms=elapsed_ms,
        status_code=200,
    )

    return PredictionResponse(
        prediction=pred,
        prediction_label="Sí" if pred == 1 else "No",
        probability=proba,
        model_name=model_service.model_name,
        model_version=model_service.version or "unknown",
        response_time_ms=round(elapsed_ms, 3),
        request_id=request_id,
    )

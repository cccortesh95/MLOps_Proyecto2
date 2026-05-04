# =============================================================================
# Schemas Pydantic
# Define los modelos de entrada/salida para la API.
# - PredictionRequest: datos de entrada del paciente
# - PredictionResponse: predicción, score, modelo, versión, tiempo
# =============================================================================

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

_MODEL_NS = ConfigDict(protected_namespaces=())


class PredictionRequest(BaseModel):
    """Una fila de características alineada con el entrenamiento (sin readmitted ni split)."""

    features: Dict[str, Any] = Field(
        ...,
        description="Diccionario nombre_columna -> valor numérico (como en clean.diabetes_clean)",
    )

    @field_validator("features", mode="before")
    @classmethod
    def coerce_numeric_values(cls, v: Any) -> Dict[str, Any]:
        if not isinstance(v, dict):
            raise TypeError("features debe ser un objeto JSON (diccionario).")
        out: Dict[str, Any] = {}
        for key, val in v.items():
            if isinstance(val, bool):
                raise ValueError(f"La característica '{key}' no puede ser booleana.")
            if isinstance(val, (int, float)):
                out[str(key)] = float(val)
            elif val is None:
                raise ValueError(f"La característica '{key}' no puede ser null.")
            else:
                raise ValueError(
                    f"La característica '{key}' debe ser numérica; se recibió {type(val).__name__}."
                )
        return out


class PredictionResponse(BaseModel):
    model_config = _MODEL_NS

    prediction: int = Field(..., description="Clase predicha (0 o 1).")
    probability: Optional[float] = Field(
        None, description="Probabilidad estimada de clase positiva (1), si el modelo la expone."
    )
    model_name: str
    model_version: str
    response_time_ms: float
    request_id: str


class ModelInfoResponse(BaseModel):
    model_config = _MODEL_NS

    model_name: str
    model_version: str
    alias: str
    loaded: bool
    feature_names: List[str]
    n_features: int


class HealthResponse(BaseModel):
    model_config = _MODEL_NS

    status: str
    model_loaded: bool
    model_name: Optional[str] = None
    model_version: Optional[str] = None
    detail: Optional[str] = None


class ErrorDetail(BaseModel):
    error: str
    detail: Optional[str] = None
    missing_features: Optional[List[str]] = None
    extra_features: Optional[List[str]] = None

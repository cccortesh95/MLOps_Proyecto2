# =============================================================================
# Model Loader
# Carga dinámica del modelo productivo desde MLflow.
# Estrategia: carga al iniciar + recarga periódica o bajo demanda.
# Usa el alias "champion" o stage "Production" según versión de MLflow.
# =============================================================================

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, List, Optional, Tuple

import mlflow
import pandas as pd
from mlflow.tracking import MlflowClient

from app.metrics import MODEL_LOADED

logger = logging.getLogger(__name__)

MODEL_ALIAS = "champion"


class ModelLoadError(Exception):
    """No se pudo resolver o cargar el modelo desde MLflow."""


class ModelService:
    """Carga sklearn/xgboost/lightgbm registrados vía MLflow (flavor sklearn)."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._sklearn_model: Any = None
        self._model_name: str = os.getenv("MODEL_NAME", "diabetes-model")
        self._version: Optional[str] = None
        self._run_id: Optional[str] = None
        self._feature_names: List[str] = []
        self._loaded_at: float = 0.0
        self._last_error: Optional[str] = None
        MODEL_LOADED.set(0)

    def configure_tracking(self) -> None:
        uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
        mlflow.set_tracking_uri(uri)
        logger.info("MLflow tracking URI: %s", uri)

    def load(self) -> None:
        """Descarga y cachea el modelo apuntado por el alias champion."""
        with self._lock:
            self._load_unlocked()

    def _load_unlocked(self) -> None:
        self.configure_tracking()
        client = MlflowClient()
        try:
            mv = client.get_model_version_by_alias(self._model_name, MODEL_ALIAS)
        except Exception as exc:  # noqa: BLE001
            self._last_error = str(exc)
            logger.warning("No hay modelo con alias '%s': %s", MODEL_ALIAS, exc)
            self._sklearn_model = None
            self._version = None
            self._run_id = None
            self._feature_names = []
            MODEL_LOADED.set(0)
            return

        model_uri = f"models:/{self._model_name}@{MODEL_ALIAS}"
        try:
            sklearn_model = mlflow.sklearn.load_model(model_uri)
        except Exception as exc:  # noqa: BLE001
            self._last_error = str(exc)
            logger.exception("Fallo al cargar artefacto desde %s", model_uri)
            self._sklearn_model = None
            self._feature_names = []
            MODEL_LOADED.set(0)
            raise ModelLoadError(str(exc)) from exc

        self._sklearn_model = sklearn_model
        self._version = str(mv.version)
        self._run_id = mv.run_id
        self._feature_names = self._infer_feature_names(sklearn_model)
        self._loaded_at = time.time()
        self._last_error = None
        MODEL_LOADED.set(1 if self.is_ready else 0)
        logger.info(
            "Modelo cargado: %s v%s (%d características)",
            self._model_name,
            self._version,
            len(self._feature_names),
        )

    @staticmethod
    def _infer_feature_names(model: Any) -> List[str]:
        names = getattr(model, "feature_names_in_", None)
        if names is not None:
            return [str(x) for x in list(names)]
        n = getattr(model, "n_features_in_", None)
        if n is not None:
            return [f"feature_{i}" for i in range(int(n))]
        return []

    @property
    def is_ready(self) -> bool:
        return self._sklearn_model is not None and bool(self._feature_names)

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def version(self) -> Optional[str]:
        return self._version

    @property
    def feature_names(self) -> List[str]:
        return list(self._feature_names)

    @property
    def last_error(self) -> Optional[str]:
        return self._last_error

    def reload_if_needed(self) -> None:
        """Recarga el modelo si el alias champion apunta a otra versión."""
        with self._lock:
            client = MlflowClient()
            try:
                mv = client.get_model_version_by_alias(self._model_name, MODEL_ALIAS)
            except Exception:  # noqa: BLE001
                MODEL_LOADED.set(0)
                return
            MODEL_LOADED.set(1 if self.is_ready else 0)
            remote_v = str(mv.version)
            if self._version != remote_v:
                logger.info(
                    "Detectada nueva versión del modelo (%s -> %s). Recargando.",
                    self._version,
                    remote_v,
                )
                self._load_unlocked()

    def predict_row(self, features: dict[str, Any]) -> Tuple[int, Optional[float]]:
        """
        Ejecuta inferencia para un único registro.
        Acepta valores numéricos y strings (el pipeline se encarga de transformar).
        Devuelve (clase, probabilidad_clase_1 o None).
        """
        with self._lock:
            if not self.is_ready:
                raise ModelLoadError(
                    self._last_error
                    or "Modelo no disponible. Verifique el alias 'champion' en MLflow."
                )
            model = self._sklearn_model
            cols = self._feature_names
            row = pd.DataFrame([features], columns=cols)
            pred = int(model.predict(row)[0])
            proba: Optional[float] = None
            if hasattr(model, "predict_proba"):
                probas = model.predict_proba(row)[0]
                if len(probas) > 1:
                    proba = float(probas[1])
                else:
                    proba = float(probas[0])
            return pred, proba


model_service = ModelService()

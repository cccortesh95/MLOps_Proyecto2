# =============================================================================
# Locust - Pruebas de carga
# Simula usuarios concurrentes consumiendo el endpoint /predict.
# Reporta: usuarios simulados, spawn rate, solicitudes totales,
#           exitosas/fallidas, latencia promedio, percentiles,
#           punto de degradación de la API.
# =============================================================================

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

from locust import HttpUser, between, events, task

logger = logging.getLogger(__name__)

# Host por defecto: variable de entorno (Kubernetes) o local.
DEFAULT_HOST = os.environ.get("LOCUST_HOST", "http://127.0.0.1:8000").rstrip("/")


@events.init.add_listener
def on_locust_init(environment: Any, **kwargs: Any) -> None:
    """Permite ver en logs el host efectivo al arrancar en modo headless."""
    logger.info("Locust host por defecto (clase HttpUser): %s", DEFAULT_HOST)


class InferenceUser(HttpUser):
    wait_time = between(0.3, 1.2)
    host = DEFAULT_HOST

    def on_start(self) -> None:
        self._payload: Optional[Dict[str, Any]] = None
        with self.client.get("/example-features", catch_response=True, name="/example-features") as resp:
            if resp.status_code == 200:
                data = resp.json()
                feats = data.get("features", data)
                self._payload = {"features": feats}
            else:
                resp.failure(f"No se pudo obtener plantilla: {resp.status_code}")
        if self._payload is None:
            # Fallback mínimo para no romper el escenario si aún no hay modelo
            self._payload = {"features": {}}

    @task(3)
    def predict(self) -> None:
        if not self._payload or not self._payload.get("features"):
            with self.client.get("/health", name="/health") as resp:
                if resp.status_code != 200:
                    resp.failure("health falló")
            return
        self.client.post(
            "/predict",
            data=json.dumps(self._payload),
            headers={"Content-Type": "application/json"},
            name="/predict",
        )

    @task(1)
    def health(self) -> None:
        self.client.get("/health", name="/health")

    @task(1)
    def model_info(self) -> None:
        self.client.get("/model-info", name="/model-info")

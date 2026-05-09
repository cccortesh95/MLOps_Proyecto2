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
from typing import Any, Dict
 
from locust import HttpUser, between, events, task
 
logger = logging.getLogger(__name__)
 
DEFAULT_HOST = os.environ.get("LOCUST_HOST", "http://127.0.0.1:8000").rstrip("/")
 
# Payload con valores reales válidos para el modelo diabetes-model
VALID_PAYLOAD: Dict[str, Any] = {
    "features": {
        "race": "Caucasian",
        "gender": "Male",
        "age": "[50-60)",
        "admission_type_id": 1,
        "discharge_disposition_id": 1,
        "admission_source_id": 7,
        "time_in_hospital": 4,
        "num_lab_procedures": 41,
        "num_procedures": 1,
        "num_medications": 12,
        "number_outpatient": 0,
        "number_emergency": 0,
        "number_inpatient": 0,
        "diag_1": "250.01",
        "diag_2": "272",
        "diag_3": "250.01",
        "number_diagnoses": 9,
        "max_glu_serum": "None",
        "A1Cresult": "None",
        "metformin": "No",
        "repaglinide": "No",
        "nateglinide": "No",
        "chlorpropamide": "No",
        "glimepiride": "No",
        "acetohexamide": "No",
        "glipizide": "No",
        "glyburide": "No",
        "tolbutamide": "No",
        "pioglitazone": "No",
        "rosiglitazone": "No",
        "acarbose": "No",
        "miglitol": "No",
        "troglitazone": "No",
        "tolazamide": "No",
        "insulin": "No",
        "glyburide-metformin": "No",
        "glipizide-metformin": "No",
        "glimepiride-pioglitazone": "No",
        "metformin-rosiglitazone": "No",
        "metformin-pioglitazone": "No",
        "change": "No",
        "diabetesMed": "Yes",
    }
}
 
 
@events.init.add_listener
def on_locust_init(environment: Any, **kwargs: Any) -> None:
    logger.info("Locust host: %s", DEFAULT_HOST)
 
 
class InferenceUser(HttpUser):
    wait_time = between(0.3, 1.2)
    host = DEFAULT_HOST
 
    @task(3)
    def predict(self) -> None:
        self.client.post(
            "/predict",
            data=json.dumps(VALID_PAYLOAD),
            headers={"Content-Type": "application/json"},
            name="/predict",
        )
 
    @task(1)
    def health(self) -> None:
        self.client.get("/health", name="/health")
 
    @task(1)
    def model_info(self) -> None:
        self.client.get("/model-info", name="/model-info")

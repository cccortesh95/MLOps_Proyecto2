# =============================================================================
# Interfaz Gráfica - Streamlit
# Funcionalidades:
#   - Ingresar valores para realizar inferencia
#   - Cargar valores de ejemplo
#   - Enviar solicitud a la API
#   - Visualizar predicción y versión del modelo
#   - Mostrar errores de validación
# La inferencia se hace exclusivamente a través de la API (no MLflow directo).
# =============================================================================

from __future__ import annotations

import json
import os
from typing import Any, Dict

import requests
import streamlit as st

API_URL = os.environ.get("API_URL", "http://127.0.0.1:8000").rstrip("/")


def _render_detail(detail: Any) -> None:
    if isinstance(detail, dict):
        st.json(detail)
    else:
        st.write(detail)


def main() -> None:
    st.set_page_config(page_title="Inferencia — MLOps Diabetes", layout="wide")
    st.title("Inferencia de readmisión")
    st.caption("Los datos se envían solo a la API de inferencia (FastAPI), no a MLflow.")

    with st.sidebar:
        st.subheader("Conexión")
        api_url = st.text_input(
            "URL base de la API",
            value=API_URL,
            help="En Kubernetes suele ser http://api-service:8000",
        )
        st.caption("Variable de entorno: `API_URL`")
        if st.button("Comprobar salud (/health)"):
            try:
                r = requests.get(f"{api_url.rstrip('/')}/health", timeout=15)
                st.write(r.status_code, r.json())
            except requests.RequestException as exc:
                st.error(str(exc))

    base = api_url.rstrip("/")

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Estado del modelo")
        if st.button("Refrescar /model-info", key="mi"):
            try:
                r = requests.get(f"{base}/model-info", timeout=15)
                if r.ok:
                    st.session_state["model_info"] = r.json()
                else:
                    st.error(r.text)
            except requests.RequestException as exc:
                st.error(str(exc))
        if "model_info" in st.session_state:
            st.json(st.session_state["model_info"])

    with col_b:
        st.subheader("Plantilla de características")
        if st.button("Obtener /example-features"):
            try:
                r = requests.get(f"{base}/example-features", timeout=15)
                if r.ok:
                    payload = r.json()
                    inner = payload.get("features", payload)
                    st.session_state["features_json"] = json.dumps(
                        {"features": inner}, indent=2, sort_keys=True
                    )
                else:
                    data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
                    st.warning(data.get("detail", r.text))
            except requests.RequestException as exc:
                st.error(str(exc))
        if st.button("Limpiar editor"):
            st.session_state["features_json"] = '{"features": {}}'

    st.subheader("Cuerpo de la solicitud (`features`)")
    editor_default = st.session_state.get("features_json", '{"features": {}}')
    features_raw = st.text_area(
        "JSON: objeto con clave `features` (mapa nombre → número)",
        value=editor_default,
        height=320,
        help='Formato: {"features": {"col1": 1.0, "col2": 0.0, ...}}',
    )

    if st.button("Enviar inferencia (/predict)", type="primary"):
        try:
            body = json.loads(features_raw)
        except json.JSONDecodeError as exc:
            st.error(f"JSON inválido: {exc}")
            return
        if not isinstance(body, dict) or "features" not in body:
            st.error('El JSON debe ser un objeto que contenga la clave "features".')
            return
        try:
            r = requests.post(f"{base}/predict", json=body, timeout=60)
        except requests.RequestException as exc:
            st.error(f"Error de red: {exc}")
            return

        if r.status_code == 200:
            st.success("Inferencia correcta")
            st.json(r.json())
        elif r.status_code == 422:
            st.error("Validación rechazada (422)")
            _render_detail(r.json().get("detail"))
        else:
            st.error(f"Error {r.status_code}")
            try:
                _render_detail(r.json().get("detail", r.json()))
            except json.JSONDecodeError:
                st.code(r.text)

    st.divider()
    st.markdown(
        """
**Notas**
- Primero debe existir un modelo registrado en MLflow con alias `champion` (pipeline Airflow).
- Use **Obtener /example-features** para rellenar todas las columnas esperadas con ceros y editar valores.
"""
    )


if __name__ == "__main__":
    main()

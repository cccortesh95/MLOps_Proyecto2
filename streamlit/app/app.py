# =============================================================================
# Interfaz Gráfica - Streamlit
# Inferencia de readmisión hospitalaria de pacientes con diabetes.
# Comunicación exclusiva con la API de inferencia (FastAPI).
# =============================================================================

from __future__ import annotations

import json
import os
from typing import Any, Dict

import requests
import streamlit as st

API_URL = os.environ.get("API_URL", "http://127.0.0.1:8000").rstrip("/")

# =============================================================================
# Definición de features con sus tipos, rangos y opciones
# =============================================================================
FEATURE_DEFINITIONS = {
    "race": {
        "type": "categorical",
        "label": "Raza",
        "options": ["Caucasian", "AfricanAmerican", "Hispanic", "Asian", "Other"],
        "default": "Caucasian",
    },
    "gender": {
        "type": "categorical",
        "label": "Género",
        "options": ["Male", "Female"],
        "default": "Female",
    },
    "age": {
        "type": "categorical",
        "label": "Rango de edad",
        "options": [
            "[0-10)", "[10-20)", "[20-30)", "[30-40)", "[40-50)",
            "[50-60)", "[60-70)", "[70-80)", "[80-90)", "[90-100)",
        ],
        "default": "[50-60)",
    },
    "admission_type_id": {
        "type": "numeric",
        "label": "Tipo de admisión",
        "min": 1, "max": 8, "default": 1, "step": 1,
        "help": "1=Emergencia, 2=Urgente, 3=Electiva, 4=Recién nacido, 5=No disponible, 6-8=Otros",
    },
    "discharge_disposition_id": {
        "type": "numeric",
        "label": "Disposición al alta",
        "min": 1, "max": 29, "default": 1, "step": 1,
        "help": "1=Alta a casa, 2=Otra institución, 3=SNF, 6=Fallecido, etc.",
    },
    "admission_source_id": {
        "type": "numeric",
        "label": "Fuente de admisión",
        "min": 1, "max": 26, "default": 1, "step": 1,
        "help": "1=Referido, 2=Clínica, 7=Emergencia, etc.",
    },
    "time_in_hospital": {
        "type": "numeric",
        "label": "Días en hospital",
        "min": 1, "max": 14, "default": 4, "step": 1,
        "help": "Rango: 1-14 días",
    },
    "num_lab_procedures": {
        "type": "numeric",
        "label": "Procedimientos de laboratorio",
        "min": 1, "max": 132, "default": 40, "step": 1,
        "help": "Rango: 1-132",
    },
    "num_procedures": {
        "type": "numeric",
        "label": "Procedimientos realizados",
        "min": 0, "max": 6, "default": 1, "step": 1,
        "help": "Rango: 0-6",
    },
    "num_medications": {
        "type": "numeric",
        "label": "Número de medicamentos",
        "min": 1, "max": 81, "default": 15, "step": 1,
        "help": "Rango: 1-81",
    },
    "number_outpatient": {
        "type": "numeric",
        "label": "Visitas ambulatorias (último año)",
        "min": 0, "max": 42, "default": 0, "step": 1,
        "help": "Rango: 0-42",
    },
    "number_emergency": {
        "type": "numeric",
        "label": "Visitas de emergencia (último año)",
        "min": 0, "max": 76, "default": 0, "step": 1,
        "help": "Rango: 0-76",
    },
    "number_inpatient": {
        "type": "numeric",
        "label": "Hospitalizaciones (último año)",
        "min": 0, "max": 21, "default": 0, "step": 1,
        "help": "Rango: 0-21",
    },
    "number_diagnoses": {
        "type": "numeric",
        "label": "Número de diagnósticos",
        "min": 1, "max": 16, "default": 7, "step": 1,
        "help": "Rango: 1-16",
    },
    "max_glu_serum": {
        "type": "categorical",
        "label": "Resultado glucosa sérica",
        "options": ["None", "Norm", ">200", ">300"],
        "default": "None",
    },
    "A1Cresult": {
        "type": "categorical",
        "label": "Resultado A1C",
        "options": ["None", "Norm", ">7", ">8"],
        "default": "None",
    },
    "metformin": {
        "type": "categorical",
        "label": "Metformina",
        "options": ["No", "Steady", "Up", "Down"],
        "default": "No",
    },
    "repaglinide": {
        "type": "categorical",
        "label": "Repaglinida",
        "options": ["No", "Steady", "Up", "Down"],
        "default": "No",
    },
    "nateglinide": {
        "type": "categorical",
        "label": "Nateglinida",
        "options": ["No", "Steady", "Up", "Down"],
        "default": "No",
    },
    "chlorpropamide": {
        "type": "categorical",
        "label": "Clorpropamida",
        "options": ["No", "Steady", "Up", "Down"],
        "default": "No",
    },
    "glimepiride": {
        "type": "categorical",
        "label": "Glimepirida",
        "options": ["No", "Steady", "Up", "Down"],
        "default": "No",
    },
    "acetohexamide": {
        "type": "categorical",
        "label": "Acetohexamida",
        "options": ["No", "Steady", "Up", "Down"],
        "default": "No",
    },
    "glipizide": {
        "type": "categorical",
        "label": "Glipizida",
        "options": ["No", "Steady", "Up", "Down"],
        "default": "No",
    },
    "glyburide": {
        "type": "categorical",
        "label": "Gliburida",
        "options": ["No", "Steady", "Up", "Down"],
        "default": "No",
    },
    "tolbutamide": {
        "type": "categorical",
        "label": "Tolbutamida",
        "options": ["No", "Steady", "Up", "Down"],
        "default": "No",
    },
    "pioglitazone": {
        "type": "categorical",
        "label": "Pioglitazona",
        "options": ["No", "Steady", "Up", "Down"],
        "default": "No",
    },
    "rosiglitazone": {
        "type": "categorical",
        "label": "Rosiglitazona",
        "options": ["No", "Steady", "Up", "Down"],
        "default": "No",
    },
    "acarbose": {
        "type": "categorical",
        "label": "Acarbosa",
        "options": ["No", "Steady", "Up", "Down"],
        "default": "No",
    },
    "miglitol": {
        "type": "categorical",
        "label": "Miglitol",
        "options": ["No", "Steady", "Up", "Down"],
        "default": "No",
    },
    "troglitazone": {
        "type": "categorical",
        "label": "Troglitazona",
        "options": ["No", "Steady", "Up", "Down"],
        "default": "No",
    },
    "tolazamide": {
        "type": "categorical",
        "label": "Tolazamida",
        "options": ["No", "Steady", "Up", "Down"],
        "default": "No",
    },
    "insulin": {
        "type": "categorical",
        "label": "Insulina",
        "options": ["No", "Steady", "Up", "Down"],
        "default": "No",
    },
    "glyburide-metformin": {
        "type": "categorical",
        "label": "Gliburida-Metformina",
        "options": ["No", "Steady", "Up", "Down"],
        "default": "No",
    },
    "glipizide-metformin": {
        "type": "categorical",
        "label": "Glipizida-Metformina",
        "options": ["No", "Steady", "Up", "Down"],
        "default": "No",
    },
    "glimepiride-pioglitazone": {
        "type": "categorical",
        "label": "Glimepirida-Pioglitazona",
        "options": ["No", "Steady", "Up", "Down"],
        "default": "No",
    },
    "metformin-rosiglitazone": {
        "type": "categorical",
        "label": "Metformina-Rosiglitazona",
        "options": ["No", "Steady", "Up", "Down"],
        "default": "No",
    },
    "metformin-pioglitazone": {
        "type": "categorical",
        "label": "Metformina-Pioglitazona",
        "options": ["No", "Steady", "Up", "Down"],
        "default": "No",
    },
    "change": {
        "type": "categorical",
        "label": "Cambio de medicación",
        "options": ["No", "Ch"],
        "default": "No",
    },
    "diabetesMed": {
        "type": "categorical",
        "label": "Medicamento para diabetes",
        "options": ["Yes", "No"],
        "default": "Yes",
    },
    "diag_1": {
        "type": "text",
        "label": "Diagnóstico principal (código ICD-9)",
        "default": "250",
        "help": "Código ICD-9. Ej: 250=Diabetes, 390-459=Circulatorio, 460-519=Respiratorio",
    },
    "diag_2": {
        "type": "text",
        "label": "Diagnóstico secundario (código ICD-9)",
        "default": "250",
        "help": "Código ICD-9. Ej: 250=Diabetes, 390-459=Circulatorio, 460-519=Respiratorio",
    },
    "diag_3": {
        "type": "text",
        "label": "Diagnóstico terciario (código ICD-9)",
        "default": "250",
        "help": "Código ICD-9. Ej: 250=Diabetes, 390-459=Circulatorio, 460-519=Respiratorio",
    },
}


# =============================================================================
# Ejemplos predefinidos
# =============================================================================
EXAMPLE_POSITIVE = {
    "race": "Caucasian", "gender": "Male", "age": "[70-80)",
    "admission_type_id": 1, "discharge_disposition_id": 3, "admission_source_id": 7,
    "time_in_hospital": 12, "num_lab_procedures": 72, "num_procedures": 4,
    "num_medications": 21, "number_outpatient": 0, "number_emergency": 2,
    "number_inpatient": 3, "number_diagnoses": 9,
    "max_glu_serum": "None", "A1Cresult": "None",
    "metformin": "No", "repaglinide": "No", "nateglinide": "No",
    "chlorpropamide": "No", "glimepiride": "No", "acetohexamide": "No",
    "glipizide": "Steady", "glyburide": "No", "tolbutamide": "No",
    "pioglitazone": "No", "rosiglitazone": "No", "acarbose": "No",
    "miglitol": "No", "troglitazone": "No", "tolazamide": "No",
    "insulin": "Up", "glyburide-metformin": "No", "glipizide-metformin": "No",
    "glimepiride-pioglitazone": "No", "metformin-rosiglitazone": "No",
    "metformin-pioglitazone": "No",
    "change": "Ch", "diabetesMed": "Yes",
    "diag_1": "390", "diag_2": "250", "diag_3": "255",
}

EXAMPLE_NEGATIVE = {
    "race": "Caucasian", "gender": "Female", "age": "[30-40)",
    "admission_type_id": 1, "discharge_disposition_id": 1, "admission_source_id": 1,
    "time_in_hospital": 2, "num_lab_procedures": 30, "num_procedures": 1,
    "num_medications": 8, "number_outpatient": 0, "number_emergency": 0,
    "number_inpatient": 0, "number_diagnoses": 3,
    "max_glu_serum": "None", "A1Cresult": "None",
    "metformin": "Steady", "repaglinide": "No", "nateglinide": "No",
    "chlorpropamide": "No", "glimepiride": "No", "acetohexamide": "No",
    "glipizide": "No", "glyburide": "No", "tolbutamide": "No",
    "pioglitazone": "No", "rosiglitazone": "No", "acarbose": "No",
    "miglitol": "No", "troglitazone": "No", "tolazamide": "No",
    "insulin": "No", "glyburide-metformin": "No", "glipizide-metformin": "No",
    "glimepiride-pioglitazone": "No", "metformin-rosiglitazone": "No",
    "metformin-pioglitazone": "No",
    "change": "No", "diabetesMed": "Yes",
    "diag_1": "250", "diag_2": "250", "diag_3": "250",
}


def _get_prefill_value(key: str) -> Any:
    """Retorna el valor prefilled según el estado actual (positivo, negativo o default)."""
    prefill = st.session_state.get("prefill", "default")
    if prefill == "positive":
        return EXAMPLE_POSITIVE.get(key)
    elif prefill == "negative":
        return EXAMPLE_NEGATIVE.get(key)
    return None


def _get(url: str) -> requests.Response | None:
    """GET con manejo de errores."""
    try:
        return requests.get(url, timeout=15)
    except requests.RequestException as exc:
        st.error(f"Error de conexión: {exc}")
        return None


def _fetch_sidebar_data(base: str) -> None:
    """Obtiene datos de salud y modelo y los guarda en session_state."""
    r = _get(f"{base}/health")
    if r and r.ok:
        st.session_state["sidebar_health"] = r.json()
    else:
        st.session_state["sidebar_health"] = None

    r = _get(f"{base}/model-info")
    if r and r.ok:
        st.session_state["sidebar_model"] = r.json()
    else:
        st.session_state["sidebar_model"] = None


def _render_sidebar(base: str) -> None:
    """Panel lateral con salud del servicio y estado del modelo."""
    st.sidebar.title("🏥 Estado del sistema")
    st.sidebar.divider()

    if st.sidebar.button("🔄 Refrescar", use_container_width=True):
        _fetch_sidebar_data(base)

    if "sidebar_health" not in st.session_state:
        _fetch_sidebar_data(base)

    # --- Health ---
    st.sidebar.subheader("Salud de la API")
    health_data = st.session_state.get("sidebar_health")
    if health_data:
        status = health_data.get("status", "unknown")
        if status == "ok":
            st.sidebar.success("✅ API operativa")
        else:
            st.sidebar.warning("⚠️ API en modo degradado")

        col1, col2 = st.sidebar.columns(2)
        col1.metric("Estado", status.upper())
        col2.metric("Modelo cargado", "Sí" if health_data.get("model_loaded") else "No")

        if health_data.get("detail"):
            st.sidebar.caption(f"Detalle: {health_data['detail']}")
    else:
        st.sidebar.error("No se pudo conectar a la API")

    st.sidebar.divider()

    # --- Model Info ---
    st.sidebar.subheader("Modelo en producción")
    model_data = st.session_state.get("sidebar_model")
    if model_data:
        st.sidebar.markdown(f"**Nombre:** `{model_data.get('model_name', '—')}`")
        st.sidebar.markdown(f"**Versión:** `{model_data.get('model_version', '—')}`")
        st.sidebar.markdown(f"**Alias:** `{model_data.get('alias', '—')}`")
        st.sidebar.markdown(f"**Features:** {model_data.get('n_features', '—')}")
        st.sidebar.markdown(f"**Cargado:** {'✅' if model_data.get('loaded') else '❌'}")
    else:
        st.sidebar.warning("No se pudo obtener info del modelo")

    st.sidebar.divider()
    st.sidebar.caption(f"API: `{base}`")


def _render_form() -> Dict[str, Any]:
    """Renderiza el formulario con inputs para cada feature y retorna los valores."""
    features = {}

    def _cat_value(key: str, defn: dict) -> int:
        """Retorna el índice de la opción para selectbox."""
        pv = _get_prefill_value(key)
        if pv is not None and pv in defn["options"]:
            return defn["options"].index(pv)
        return defn["options"].index(defn["default"])

    def _num_value(key: str, defn: dict) -> int:
        """Retorna el valor numérico prefilled o default."""
        pv = _get_prefill_value(key)
        if pv is not None:
            return int(pv)
        return defn["default"]

    def _text_value(key: str, defn: dict) -> str:
        """Retorna el valor de texto prefilled o default."""
        pv = _get_prefill_value(key)
        if pv is not None:
            return str(pv)
        return defn["default"]

    # Sección: Datos demográficos
    st.subheader("👤 Datos demográficos")
    cols = st.columns(3)
    demo_keys = ["race", "gender", "age"]
    for i, key in enumerate(demo_keys):
        defn = FEATURE_DEFINITIONS[key]
        with cols[i]:
            features[key] = st.selectbox(
                defn["label"], options=defn["options"],
                index=_cat_value(key, defn),
                key=f"input_{key}",
            )

    # Sección: Información de la visita
    st.subheader("🏨 Información de la visita")
    cols = st.columns(3)
    visit_keys = ["admission_type_id", "discharge_disposition_id", "admission_source_id"]
    for i, key in enumerate(visit_keys):
        defn = FEATURE_DEFINITIONS[key]
        with cols[i]:
            features[key] = st.number_input(
                defn["label"],
                min_value=defn["min"], max_value=defn["max"],
                value=_num_value(key, defn), step=defn["step"],
                help=defn["help"], key=f"input_{key}",
            )

    cols = st.columns(4)
    stay_keys = ["time_in_hospital", "num_lab_procedures", "num_procedures", "num_medications"]
    for i, key in enumerate(stay_keys):
        defn = FEATURE_DEFINITIONS[key]
        with cols[i]:
            features[key] = st.number_input(
                defn["label"],
                min_value=defn["min"], max_value=defn["max"],
                value=_num_value(key, defn), step=defn["step"],
                help=defn["help"], key=f"input_{key}",
            )

    # Sección: Historial previo
    st.subheader("📋 Historial previo (último año)")
    cols = st.columns(4)
    hist_keys = ["number_outpatient", "number_emergency", "number_inpatient", "number_diagnoses"]
    for i, key in enumerate(hist_keys):
        defn = FEATURE_DEFINITIONS[key]
        with cols[i]:
            features[key] = st.number_input(
                defn["label"],
                min_value=defn["min"], max_value=defn["max"],
                value=_num_value(key, defn), step=defn["step"],
                help=defn["help"], key=f"input_{key}",
            )

    # Sección: Resultados de laboratorio
    st.subheader("🔬 Resultados de laboratorio")
    cols = st.columns(2)
    lab_keys = ["max_glu_serum", "A1Cresult"]
    for i, key in enumerate(lab_keys):
        defn = FEATURE_DEFINITIONS[key]
        with cols[i]:
            features[key] = st.selectbox(
                defn["label"], options=defn["options"],
                index=_cat_value(key, defn),
                key=f"input_{key}",
            )

    # Sección: Medicamentos
    st.subheader("💊 Medicamentos")
    med_keys = [
        "metformin", "repaglinide", "nateglinide", "chlorpropamide",
        "glimepiride", "acetohexamide", "glipizide", "glyburide",
        "tolbutamide", "pioglitazone", "rosiglitazone", "acarbose",
        "miglitol", "troglitazone", "tolazamide", "insulin",
        "glyburide-metformin", "glipizide-metformin",
        "glimepiride-pioglitazone", "metformin-rosiglitazone",
        "metformin-pioglitazone",
    ]
    for row_start in range(0, len(med_keys), 4):
        cols = st.columns(4)
        for i, key in enumerate(med_keys[row_start:row_start + 4]):
            defn = FEATURE_DEFINITIONS[key]
            with cols[i]:
                features[key] = st.selectbox(
                    defn["label"], options=defn["options"],
                    index=_cat_value(key, defn),
                    key=f"input_{key}",
                )

    # Sección: Cambio de medicación y diabetes
    st.subheader("📝 Tratamiento general")
    cols = st.columns(2)
    treat_keys = ["change", "diabetesMed"]
    for i, key in enumerate(treat_keys):
        defn = FEATURE_DEFINITIONS[key]
        with cols[i]:
            features[key] = st.selectbox(
                defn["label"], options=defn["options"],
                index=_cat_value(key, defn),
                key=f"input_{key}",
            )

    # Sección: Diagnósticos
    st.subheader("🩺 Diagnósticos (códigos ICD-9)")
    cols = st.columns(3)
    diag_keys = ["diag_1", "diag_2", "diag_3"]
    for i, key in enumerate(diag_keys):
        defn = FEATURE_DEFINITIONS[key]
        with cols[i]:
            features[key] = st.text_input(
                defn["label"], value=_text_value(key, defn),
                help=defn["help"], key=f"input_{key}",
            )

    return features


def _render_prediction_result(data: Dict[str, Any]) -> None:
    """Muestra el resultado de la predicción de forma visual."""
    prediction = data.get("prediction", 0)
    label = data.get("prediction_label", "Sí" if prediction == 1 else "No")
    probability = data.get("probability")

    if prediction == 1:
        st.error(f"### 🔴 Predicción: **{label}** — Readmisión probable (<30 días)")
    else:
        st.success(f"### 🟢 Predicción: **{label}** — Readmisión no probable")

    cols = st.columns(4)
    cols[0].metric("Predicción", label)
    if probability is not None:
        cols[1].metric("Probabilidad", f"{probability:.1%}")
    cols[2].metric("Modelo", f"v{data.get('model_version', '?')}")
    cols[3].metric("Tiempo", f"{data.get('response_time_ms', 0):.1f} ms")

    with st.expander("Ver respuesta completa"):
        st.json(data)


def _render_error_result(status_code: int, response: requests.Response) -> None:
    """Muestra errores de la API de forma legible."""
    if status_code == 422:
        st.error("❌ Validación rechazada")
        try:
            detail = response.json().get("detail", {})
            if isinstance(detail, dict):
                if detail.get("missing_features"):
                    st.warning(f"**Features faltantes:** {', '.join(detail['missing_features'])}")
                if detail.get("extra_features"):
                    st.warning(f"**Features sobrantes:** {', '.join(detail['extra_features'])}")
                if detail.get("error"):
                    st.info(detail["error"])
            else:
                st.json(detail)
        except Exception:
            st.code(response.text)
    elif status_code == 503:
        st.error("⚠️ Modelo no disponible. Ejecute el pipeline de entrenamiento primero.")
    else:
        st.error(f"Error {status_code}")
        try:
            st.json(response.json())
        except Exception:
            st.code(response.text)


def main() -> None:
    st.set_page_config(
        page_title="Inferencia — MLOps Diabetes",
        page_icon="🏥",
        layout="wide",
    )

    base = API_URL

    # Sidebar
    _render_sidebar(base)

    # --- Contenido principal ---
    st.title("🏥 Predicción de Readmisión Hospitalaria")
    st.markdown(
        "Predice si un paciente con diabetes será **readmitido en menos de 30 días** "
        "basándose en sus datos clínicos. Complete el formulario y presione **Realizar predicción**."
    )
    st.divider()

    # Botones de ejemplo
    col_pos, col_neg, col_reset = st.columns(3)
    with col_pos:
        if st.button("🔴 Ejemplo positivo (readmisión)", use_container_width=True):
            st.session_state["prefill"] = "positive"
            st.rerun()
    with col_neg:
        if st.button("🟢 Ejemplo negativo (no readmisión)", use_container_width=True):
            st.session_state["prefill"] = "negative"
            st.rerun()
    with col_reset:
        if st.button("🗑️ Restablecer valores", use_container_width=True):
            st.session_state["prefill"] = "default"
            st.rerun()

    st.divider()

    # Formulario
    features = _render_form()

    st.divider()

    # Botón de predicción
    if st.button("🔮 Realizar predicción", type="primary", use_container_width=True):
        body = {"features": features}

        with st.spinner("Consultando modelo..."):
            try:
                r = requests.post(f"{base}/predict", json=body, timeout=60)
            except requests.RequestException as exc:
                st.error(f"Error de red: {exc}")
                return

        st.divider()
        if r.status_code == 200:
            _render_prediction_result(r.json())
        else:
            _render_error_result(r.status_code, r)

    # Footer
    st.divider()
    st.caption(
        "💡 Los valores por defecto representan un paciente promedio. "
        "Modifique los campos según el caso clínico a evaluar."
    )


if __name__ == "__main__":
    main()

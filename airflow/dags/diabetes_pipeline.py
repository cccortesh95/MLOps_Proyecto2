# =============================================================================
# DAG: diabetes_pipeline
# Pipeline MLOps completo para el dataset Diabetes 130-US Hospitals.
#
# Flujo:
#   1. validate_source     → Verifica disponibilidad del archivo fuente
#   2. load_raw_batch      → Carga incremental por lotes (max 15,000 registros)
#   3. validate_quality    → Validación básica de calidad de datos
#   4. preprocess_data     → Limpieza, transformación e ingeniería de features
#   5. store_clean_data    → Almacena datos procesados en tabla independiente
#   6. split_data          → Separación en train / validation / test
#   7. train_models        → Entrenamiento de modelos (LogisticRegression, XGBoost, LightGBM)
#   8. register_mlflow     → Registro de parámetros, métricas, artefactos en MLflow
#   9. compare_models      → Comparación contra modelos anteriores
#  10. promote_best_model  → Promoción automática del mejor modelo (alias champion)
#
# Métrica principal: ROC-AUC (justificación: problema de clasificación clínica
# con clases desbalanceadas donde importa la capacidad discriminativa global)
# =============================================================================

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timedelta

import mlflow
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
BATCH_SIZE = 15_000
DATA_SOURCE_URL = (
    "https://docs.google.com/uc?export=download"
    "&confirm=t&id=1k5-1caezQ3zWJbKaiMULTGq-3sz6uThC"
)
LOCAL_CSV_PATH = "/tmp/diabetes_raw.csv"

EXPERIMENT_NAME = "diabetes_readmission"
MODEL_NAME = "diabetes-model"

# Columnas objetivo y a eliminar
TARGET_COL = "readmitted"
DROP_COLS = [
    "encounter_id",
    "patient_nbr",
    "weight",
    "payer_code",
    "medical_specialty",
    "examide",
    "citoglipton",
]


def _get_engine(db_name):
    """Crea engine de SQLAlchemy hacia una BD específica del proyecto."""
    host = os.getenv("DATA_DB_HOST", "postgres-service")
    port = os.getenv("DATA_DB_PORT", "5432")
    user = os.getenv("DATA_DB_USER", "mlops_user")
    pw = os.getenv("DATA_DB_PASSWORD", "mlops1234")
    return create_engine(f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db_name}")


def _get_raw_engine():
    """Engine hacia raw_data_db."""
    return _get_engine(os.getenv("DATA_DB_NAME", "raw_data_db"))


def _get_clean_engine():
    """Engine hacia clean_data_db."""
    return _get_engine(os.getenv("CLEAN_DB_NAME", "clean_data_db"))


def _get_inference_engine():
    """Engine hacia inference_db."""
    return _get_engine(os.getenv("INFERENCE_DB_NAME", "inference_db"))


# ---------------------------------------------------------------------------
# Task 1: Validar disponibilidad del archivo fuente
# ---------------------------------------------------------------------------
def validate_source(**kwargs):
    """Descarga el CSV fuente si no existe localmente."""
    import requests

    if os.path.isfile(LOCAL_CSV_PATH):
        df = pd.read_csv(LOCAL_CSV_PATH, nrows=5)
        logger.info(f"Archivo fuente ya existe con columnas: {list(df.columns)}")
        return True

    logger.info("Descargando dataset desde Google Drive...")
    r = requests.get(DATA_SOURCE_URL, allow_redirects=True, stream=True, timeout=120)
    r.raise_for_status()
    with open(LOCAL_CSV_PATH, "wb") as f:
        f.write(r.content)

    df = pd.read_csv(LOCAL_CSV_PATH, nrows=5)
    logger.info(f"Dataset descargado. Columnas: {list(df.columns)}, muestra OK.")
    return True


# ---------------------------------------------------------------------------
# Task 2: Carga incremental por lotes a tabla raw
# ---------------------------------------------------------------------------
def load_raw_batch(**kwargs):
    """
    Carga incremental del CSV en lotes de max 15,000 registros.
    Usa batch_id y row_hash para control de duplicados.
    """
    engine = _get_raw_engine()
    df_full = pd.read_csv(LOCAL_CSV_PATH, na_values="?")
    total_rows = len(df_full)
    logger.info(f"Total de registros en archivo fuente: {total_rows}")

    # Determinar siguiente batch_id
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COALESCE(MAX(batch_id), 0) FROM raw.diabetes_raw")
        )
        last_batch = result.scalar()

    # Determinar registros ya cargados
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM raw.diabetes_raw"))
        loaded_count = result.scalar()

    if loaded_count >= total_rows:
        logger.info("Todos los registros ya fueron cargados. Nada que hacer.")
        kwargs["ti"].xcom_push(key="new_rows_loaded", value=0)
        return

    # Cargar siguiente lote
    start_idx = loaded_count
    end_idx = min(start_idx + BATCH_SIZE, total_rows)
    batch_df = df_full.iloc[start_idx:end_idx].copy()
    batch_id = last_batch + 1

    # Agregar metadatos de carga
    batch_df["batch_id"] = batch_id
    batch_df["load_timestamp"] = datetime.utcnow()
    batch_df["source_file"] = "Diabetes.csv"
    batch_df["row_hash"] = batch_df.apply(
        lambda row: hashlib.md5(
            str(row.drop(["batch_id", "load_timestamp", "source_file"]).values).encode()
        ).hexdigest(),
        axis=1,
    )
    batch_df["status"] = "loaded"

    batch_df.to_sql("diabetes_raw", engine, schema="raw", if_exists="append", index=False)
    rows_loaded = len(batch_df)
    logger.info(
        f"Batch {batch_id}: cargados {rows_loaded} registros "
        f"(filas {start_idx} a {end_idx - 1})"
    )
    kwargs["ti"].xcom_push(key="new_rows_loaded", value=rows_loaded)
    kwargs["ti"].xcom_push(key="batch_id", value=batch_id)


# ---------------------------------------------------------------------------
# Task 3: Validación básica de calidad de datos
# ---------------------------------------------------------------------------
def validate_quality(**kwargs):
    """Ejecuta validaciones mínimas sobre los datos crudos recién cargados."""
    engine = _get_raw_engine()
    ti = kwargs["ti"]
    batch_id = ti.xcom_pull(task_ids="load_raw_batch", key="batch_id")

    if batch_id is None:
        logger.info("No hay batch nuevo. Saltando validación.")
        return

    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM raw.diabetes_raw WHERE batch_id = :bid"),
            {"bid": batch_id},
        )
        count = result.scalar()

    logger.info(f"Batch {batch_id}: {count} registros en raw.diabetes_raw.")

    if count == 0:
        raise ValueError(f"Batch {batch_id} no tiene registros. Fallo de carga.")
    if count > BATCH_SIZE:
        raise ValueError(
            f"Batch {batch_id} tiene {count} registros, excede el límite de {BATCH_SIZE}."
        )

    # Verificar que no haya duplicados por row_hash dentro del batch
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT COUNT(*) - COUNT(DISTINCT row_hash) "
                "FROM raw.diabetes_raw WHERE batch_id = :bid"
            ),
            {"bid": batch_id},
        )
        duplicates = result.scalar()

    if duplicates > 0:
        logger.warning(f"Batch {batch_id}: {duplicates} posibles duplicados por hash.")

    logger.info(f"Validación de calidad OK para batch {batch_id}.")


# ---------------------------------------------------------------------------
# Task 4: Preprocesamiento y transformación
# ---------------------------------------------------------------------------
def preprocess_data(**kwargs):
    """
    Limpieza, transformación e ingeniería de características.
    Lee de raw_data, procesa y devuelve DataFrame limpio vía XCom (serializado).
    """
    from sklearn.preprocessing import LabelEncoder

    engine = _get_raw_engine()

    # Leer todos los datos crudos con status 'loaded'
    df = pd.read_sql(
        "SELECT * FROM raw.diabetes_raw WHERE status = 'loaded'", engine
    )
    logger.info(f"Registros crudos a procesar: {len(df)}")

    if df.empty:
        logger.info("No hay datos nuevos para procesar.")
        kwargs["ti"].xcom_push(key="clean_count", value=0)
        return

    # --- Limpieza ---
    # Eliminar columnas con alta cardinalidad o poco valor predictivo
    cols_to_drop = [c for c in DROP_COLS if c in df.columns]
    df = df.drop(columns=cols_to_drop, errors="ignore")

    # Eliminar columnas de metadatos de carga
    meta_cols = ["batch_id", "load_timestamp", "source_file", "row_hash", "status"]
    df = df.drop(columns=[c for c in meta_cols if c in df.columns], errors="ignore")

    # Reemplazar '?' con NaN (ya hecho en carga, pero por seguridad)
    df = df.replace("?", pd.NA)

    # Eliminar filas con demasiados nulos (>50% de columnas)
    threshold = len(df.columns) * 0.5
    df = df.dropna(thresh=int(threshold))

    # --- Tratamiento de valores nulos ---
    # Numéricas: rellenar con mediana
    num_cols = df.select_dtypes(include=["number"]).columns
    for col in num_cols:
        df[col] = df[col].fillna(df[col].median())

    # Categóricas: rellenar con moda
    cat_cols = df.select_dtypes(include=["object"]).columns
    for col in cat_cols:
        if col != TARGET_COL:
            mode_val = df[col].mode()
            if not mode_val.empty:
                df[col] = df[col].fillna(mode_val.iloc[0])

    # --- Ingeniería de características ---
    # Binarizar target: readmitted -> 1 si '<30', 0 en otro caso
    if TARGET_COL in df.columns:
        df[TARGET_COL] = (df[TARGET_COL] == "<30").astype(int)

    # Codificación de variables categóricas con LabelEncoder
    label_encoders = {}
    for col in df.select_dtypes(include=["object"]).columns:
        le = LabelEncoder()
        df[col] = df[col].astype(str)
        df[col] = le.fit_transform(df[col])
        label_encoders[col] = le

    logger.info(f"Datos procesados: {len(df)} filas, {len(df.columns)} columnas.")
    kwargs["ti"].xcom_push(key="clean_count", value=len(df))

    # Guardar temporalmente para las siguientes tareas
    df.to_parquet("/tmp/clean_data.parquet", index=False)


# ---------------------------------------------------------------------------
# Task 5: Almacenar datos procesados en tabla clean
# ---------------------------------------------------------------------------
def store_clean_data(**kwargs):
    """Almacena los datos procesados en la tabla clean_data."""
    ti = kwargs["ti"]
    clean_count = ti.xcom_pull(task_ids="preprocess_data", key="clean_count")

    if clean_count == 0:
        logger.info("No hay datos limpios nuevos. Saltando.")
        return

    engine = _get_clean_engine()
    df = pd.read_parquet("/tmp/clean_data.parquet")

    # Agregar metadatos de procesamiento
    df["processed_timestamp"] = datetime.utcnow()

    df.to_sql("diabetes_clean", engine, schema="clean", if_exists="replace", index=False)
    logger.info(f"Almacenados {len(df)} registros en clean.diabetes_clean.")

    # Marcar registros raw como procesados
    raw_engine = _get_raw_engine()
    with raw_engine.begin() as conn:
        conn.execute(
            text("UPDATE raw.diabetes_raw SET status = 'processed' WHERE status = 'loaded'")
        )
    logger.info("Registros raw marcados como 'processed'.")


# ---------------------------------------------------------------------------
# Task 6: Separación en train / validation / test
# ---------------------------------------------------------------------------
def split_data(**kwargs):
    """Divide los datos limpios en conjuntos de entrenamiento, validación y prueba."""
    from sklearn.model_selection import train_test_split

    df = pd.read_parquet("/tmp/clean_data.parquet")

    if df.empty:
        logger.info("No hay datos para dividir.")
        return

    X = df.drop(columns=[TARGET_COL, "processed_timestamp"], errors="ignore")
    y = df[TARGET_COL]

    # 70% train, 15% validation, 15% test
    X_train, X_temp, y_train, y_temp = train_test_split(
        X, y, test_size=0.30, random_state=42, stratify=y
    )
    X_val, X_test, y_val, y_test = train_test_split(
        X_temp, y_temp, test_size=0.50, random_state=42, stratify=y_temp
    )

    # Guardar splits
    X_train.to_parquet("/tmp/X_train.parquet", index=False)
    X_val.to_parquet("/tmp/X_val.parquet", index=False)
    X_test.to_parquet("/tmp/X_test.parquet", index=False)
    y_train.to_frame().to_parquet("/tmp/y_train.parquet", index=False)
    y_val.to_frame().to_parquet("/tmp/y_val.parquet", index=False)
    y_test.to_frame().to_parquet("/tmp/y_test.parquet", index=False)

    logger.info(
        f"Split: train={len(X_train)}, val={len(X_val)}, test={len(X_test)}"
    )


# ---------------------------------------------------------------------------
# Task 7 + 8: Entrenamiento y registro en MLflow
# ---------------------------------------------------------------------------
def train_models(**kwargs):
    """
    Entrena múltiples modelos, registra cada uno en MLflow.
    Modelos: LogisticRegression, XGBoost, LightGBM.
    Métrica principal: ROC-AUC.
    """
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import (
        accuracy_score,
        f1_score,
        precision_score,
        recall_score,
        roc_auc_score,
    )

    import xgboost as xgb
    import lightgbm as lgb

    # Cargar splits
    X_train = pd.read_parquet("/tmp/X_train.parquet")
    X_val = pd.read_parquet("/tmp/X_val.parquet")
    X_test = pd.read_parquet("/tmp/X_test.parquet")
    y_train = pd.read_parquet("/tmp/y_train.parquet")[TARGET_COL]
    y_val = pd.read_parquet("/tmp/y_val.parquet")[TARGET_COL]
    y_test = pd.read_parquet("/tmp/y_test.parquet")[TARGET_COL]

    if X_train.empty:
        logger.info("No hay datos de entrenamiento.")
        return

    # Configurar MLflow
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-service:5000"))
    mlflow.set_experiment(EXPERIMENT_NAME)

    models = {
        "LogisticRegression": LogisticRegression(
            max_iter=1000, random_state=42, class_weight="balanced"
        ),
        "XGBoost": xgb.XGBClassifier(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            random_state=42,
            use_label_encoder=False,
            eval_metric="logloss",
            scale_pos_weight=(y_train == 0).sum() / max((y_train == 1).sum(), 1),
        ),
        "LightGBM": lgb.LGBMClassifier(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            random_state=42,
            class_weight="balanced",
            verbose=-1,
        ),
    }

    best_auc = -1.0
    best_run_id = None
    best_model_name = None

    for name, model in models.items():
        with mlflow.start_run(run_name=f"{name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"):
            logger.info(f"Entrenando {name}...")
            start = time.time()
            model.fit(X_train, y_train)
            train_time = time.time() - start

            # Predicciones
            y_val_pred = model.predict(X_val)
            y_val_proba = (
                model.predict_proba(X_val)[:, 1]
                if hasattr(model, "predict_proba")
                else y_val_pred
            )
            y_test_pred = model.predict(X_test)
            y_test_proba = (
                model.predict_proba(X_test)[:, 1]
                if hasattr(model, "predict_proba")
                else y_test_pred
            )

            # Métricas de validación
            val_auc = roc_auc_score(y_val, y_val_proba)
            val_acc = accuracy_score(y_val, y_val_pred)
            val_f1 = f1_score(y_val, y_val_pred, zero_division=0)
            val_precision = precision_score(y_val, y_val_pred, zero_division=0)
            val_recall = recall_score(y_val, y_val_pred, zero_division=0)

            # Métricas de test
            test_auc = roc_auc_score(y_test, y_test_proba)
            test_acc = accuracy_score(y_test, y_test_pred)
            test_f1 = f1_score(y_test, y_test_pred, zero_division=0)

            # Registrar parámetros
            mlflow.log_param("model_type", name)
            mlflow.log_param("train_samples", len(X_train))
            mlflow.log_param("val_samples", len(X_val))
            mlflow.log_param("test_samples", len(X_test))
            mlflow.log_param("n_features", X_train.shape[1])
            mlflow.log_params(
                {k: str(v) for k, v in model.get_params().items()}
                if hasattr(model, "get_params")
                else {}
            )

            # Registrar métricas
            mlflow.log_metric("val_roc_auc", val_auc)
            mlflow.log_metric("val_accuracy", val_acc)
            mlflow.log_metric("val_f1", val_f1)
            mlflow.log_metric("val_precision", val_precision)
            mlflow.log_metric("val_recall", val_recall)
            mlflow.log_metric("test_roc_auc", test_auc)
            mlflow.log_metric("test_accuracy", test_acc)
            mlflow.log_metric("test_f1", test_f1)
            mlflow.log_metric("training_time_seconds", train_time)

            # Registrar modelo
            mlflow.sklearn.log_model(
                model,
                artifact_path="model",
                registered_model_name=MODEL_NAME,
            )

            run_id = mlflow.active_run().info.run_id
            logger.info(
                f"{name} -> val_AUC={val_auc:.4f}, val_F1={val_f1:.4f}, "
                f"test_AUC={test_auc:.4f}, run_id={run_id}"
            )

            if val_auc > best_auc:
                best_auc = val_auc
                best_run_id = run_id
                best_model_name = name

    logger.info(f"Mejor modelo: {best_model_name} con val_AUC={best_auc:.4f}")
    kwargs["ti"].xcom_push(key="best_run_id", value=best_run_id)
    kwargs["ti"].xcom_push(key="best_model_name", value=best_model_name)
    kwargs["ti"].xcom_push(key="best_val_auc", value=best_auc)


# ---------------------------------------------------------------------------
# Task 9: Comparación contra modelos anteriores
# ---------------------------------------------------------------------------
def compare_models(**kwargs):
    """
    Compara el mejor modelo del run actual contra el modelo productivo
    actual en MLflow (si existe).
    """
    ti = kwargs["ti"]
    best_run_id = ti.xcom_pull(task_ids="train_models", key="best_run_id")
    best_val_auc = ti.xcom_pull(task_ids="train_models", key="best_val_auc")
    best_model_name = ti.xcom_pull(task_ids="train_models", key="best_model_name")

    if best_run_id is None:
        logger.info("No hay modelo nuevo para comparar.")
        kwargs["ti"].xcom_push(key="should_promote", value=False)
        return

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-service:5000"))
    client = mlflow.tracking.MlflowClient()

    # Buscar modelo productivo actual
    should_promote = True
    try:
        # Intentar obtener modelo con alias "champion"
        model_version = client.get_model_version_by_alias(MODEL_NAME, "champion")
        prod_run_id = model_version.run_id

        # Obtener métrica del modelo productivo
        prod_run = client.get_run(prod_run_id)
        prod_auc = prod_run.data.metrics.get("val_roc_auc", 0.0)

        logger.info(
            f"Modelo productivo actual: version={model_version.version}, "
            f"val_AUC={prod_auc:.4f}"
        )
        logger.info(f"Modelo candidato: {best_model_name}, val_AUC={best_val_auc:.4f}")

        if best_val_auc > prod_auc:
            logger.info("El modelo candidato SUPERA al productivo. Se promoverá.")
            should_promote = True
        else:
            logger.info("El modelo productivo actual es mejor o igual. No se promueve.")
            should_promote = False

    except Exception as e:
        logger.info(f"No hay modelo productivo previo ({e}). Se promoverá el nuevo.")
        should_promote = True

    kwargs["ti"].xcom_push(key="should_promote", value=should_promote)


# ---------------------------------------------------------------------------
# Task 10: Promoción automática del mejor modelo
# ---------------------------------------------------------------------------
def promote_best_model(**kwargs):
    """
    Promueve el mejor modelo como productivo usando el alias 'champion'
    en el registro de modelos de MLflow.
    """
    ti = kwargs["ti"]
    should_promote = ti.xcom_pull(task_ids="compare_models", key="should_promote")
    best_run_id = ti.xcom_pull(task_ids="train_models", key="best_run_id")
    best_model_name = ti.xcom_pull(task_ids="train_models", key="best_model_name")
    best_val_auc = ti.xcom_pull(task_ids="train_models", key="best_val_auc")

    if not should_promote:
        logger.info("No se requiere promoción. El modelo productivo actual se mantiene.")
        return

    if best_run_id is None:
        logger.info("No hay modelo para promover.")
        return

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-service:5000"))
    client = mlflow.tracking.MlflowClient()

    # Buscar la versión del modelo registrada en este run
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    target_version = None
    for v in versions:
        if v.run_id == best_run_id:
            target_version = v.version
            break

    if target_version is None:
        raise ValueError(
            f"No se encontró versión del modelo para run_id={best_run_id}"
        )

    # Asignar alias "champion" al mejor modelo
    client.set_registered_model_alias(MODEL_NAME, "champion", target_version)
    logger.info(
        f"Modelo promovido: {best_model_name} v{target_version} "
        f"(val_AUC={best_val_auc:.4f}) -> alias 'champion'"
    )


# ---------------------------------------------------------------------------
# Definición del DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "mlops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="diabetes_pipeline",
    default_args=default_args,
    description="Pipeline MLOps: ingesta incremental, procesamiento, entrenamiento y registro",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Ejecución manual o programar con cron
    catchup=False,
    tags=["nivel3", "mlops", "diabetes", "pipeline"],
    max_active_runs=1,
) as dag:

    t1_validate = PythonOperator(
        task_id="validate_source",
        python_callable=validate_source,
    )

    t2_load_raw = PythonOperator(
        task_id="load_raw_batch",
        python_callable=load_raw_batch,
    )

    t3_quality = PythonOperator(
        task_id="validate_quality",
        python_callable=validate_quality,
    )

    t4_preprocess = PythonOperator(
        task_id="preprocess_data",
        python_callable=preprocess_data,
    )

    t5_store_clean = PythonOperator(
        task_id="store_clean_data",
        python_callable=store_clean_data,
    )

    t6_split = PythonOperator(
        task_id="split_data",
        python_callable=split_data,
    )

    t7_train = PythonOperator(
        task_id="train_models",
        python_callable=train_models,
    )

    t8_compare = PythonOperator(
        task_id="compare_models",
        python_callable=compare_models,
    )

    t9_promote = PythonOperator(
        task_id="promote_best_model",
        python_callable=promote_best_model,
    )

    # Dependencias del flujo
    (
        t1_validate
        >> t2_load_raw
        >> t3_quality
        >> t4_preprocess
        >> t5_store_clean
        >> t6_split
        >> t7_train
        >> t8_compare
        >> t9_promote
    )

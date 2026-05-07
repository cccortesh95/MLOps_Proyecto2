"""Task: Entrenamiento con datos acumulados, registro en MLflow y promoción."""

import logging
import os
import time
from datetime import datetime

import lightgbm as lgb
import mlflow
import pandas as pd
import xgboost as xgb
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

from sqlalchemy import text

from tasks.config import EXPERIMENT_NAME, MODEL_NAME, TARGET_COL, get_clean_engine

logger = logging.getLogger(__name__)


def run(**kwargs):
    engine = get_clean_engine()

    # Verificar si la tabla existe
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'clean' AND table_name = 'diabetes_clean')"
            )
        )
        if not result.scalar():
            logger.info("Tabla clean.diabetes_clean no existe aún. Saltando entrenamiento.")
            return

    # Leer todos los datos acumulados de clean, separados por split
    df = pd.read_sql("SELECT * FROM clean.diabetes_clean", engine)
    if df.empty:
        logger.info("No hay datos en clean. Saltando entrenamiento.")
        return

    logger.info(f"Total registros en clean: {len(df)}")

    drop_cols = ["processed_timestamp"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore")

    train_df = df[df["split"] == "train"].drop(columns=["split"])
    val_df = df[df["split"] == "validation"].drop(columns=["split"])
    test_df = df[df["split"] == "test"].drop(columns=["split"])

    X_train = train_df.drop(columns=[TARGET_COL])
    y_train = train_df[TARGET_COL]
    X_val = val_df.drop(columns=[TARGET_COL])
    y_val = val_df[TARGET_COL]
    X_test = test_df.drop(columns=[TARGET_COL])
    y_test = test_df[TARGET_COL]

    logger.info(f"Split: train={len(X_train)}, val={len(X_val)}, test={len(X_test)}")

    if X_train.empty or X_val.empty:
        logger.info("Datos insuficientes para entrenar.")
        return

    # Configurar MLflow
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-service:5000")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(EXPERIMENT_NAME)

    models = {
        "LogisticRegression": LogisticRegression(
            max_iter=300, random_state=42, class_weight="balanced",
            solver="saga", tol=1e-3, n_jobs=-1,
        ),
        "XGBoost": xgb.XGBClassifier(
            n_estimators=100, max_depth=6, learning_rate=0.1,
            random_state=42, use_label_encoder=False, eval_metric="logloss",
            scale_pos_weight=(y_train == 0).sum() / max((y_train == 1).sum(), 1),
            n_jobs=-1,
        ),
        "LightGBM": lgb.LGBMClassifier(
            n_estimators=100, max_depth=6, learning_rate=0.1,
            random_state=42, class_weight="balanced", verbose=-1,
            n_jobs=-1,
        ),
    }

    best_recall = -1.0
    best_run_id = None
    best_model_name = None

    for name, model in models.items():
        with mlflow.start_run(run_name=f"{name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"):
            logger.info(f"Entrenando {name}...")
            start = time.time()
            model.fit(X_train, y_train)
            train_time = time.time() - start

            y_val_pred = model.predict(X_val)
            y_val_proba = model.predict_proba(X_val)[:, 1] if hasattr(model, "predict_proba") else y_val_pred
            y_test_pred = model.predict(X_test)
            y_test_proba = model.predict_proba(X_test)[:, 1] if hasattr(model, "predict_proba") else y_test_pred

            val_recall = recall_score(y_val, y_val_pred, zero_division=0)
            val_auc = roc_auc_score(y_val, y_val_proba)
            test_recall = recall_score(y_test, y_test_pred, zero_division=0)
            test_auc = roc_auc_score(y_test, y_test_proba)

            mlflow.log_param("model_type", name)
            mlflow.log_param("train_samples", len(X_train))
            mlflow.log_param("val_samples", len(X_val))
            mlflow.log_param("test_samples", len(X_test))
            mlflow.log_param("total_clean_records", len(df))
            mlflow.log_param("n_features", X_train.shape[1])
            mlflow.log_param("selection_metric", "val_recall")
            mlflow.log_metric("val_recall", val_recall)
            mlflow.log_metric("val_roc_auc", val_auc)
            mlflow.log_metric("val_accuracy", accuracy_score(y_val, y_val_pred))
            mlflow.log_metric("val_f1", f1_score(y_val, y_val_pred, zero_division=0))
            mlflow.log_metric("val_precision", precision_score(y_val, y_val_pred, zero_division=0))
            mlflow.log_metric("test_roc_auc", test_auc)
            mlflow.log_metric("test_recall", test_recall)
            mlflow.log_metric("test_accuracy", accuracy_score(y_test, y_test_pred))
            mlflow.log_metric("test_f1", f1_score(y_test, y_test_pred, zero_division=0))
            mlflow.log_metric("training_time_seconds", train_time)

            mlflow.sklearn.log_model(model, artifact_path="model", registered_model_name=MODEL_NAME)
            run_id = mlflow.active_run().info.run_id
            logger.info(f"{name} -> val_recall={val_recall:.4f}, val_AUC={val_auc:.4f}")

            if val_recall > best_recall:
                best_recall = val_recall
                best_run_id = run_id
                best_model_name = name

    if best_run_id is None:
        return

    # --- Comparar y promover ---
    client = mlflow.tracking.MlflowClient()
    should_promote = True

    try:
        current = client.get_model_version_by_alias(MODEL_NAME, "champion")
        prod_recall = client.get_run(current.run_id).data.metrics.get("val_recall", 0.0)
        logger.info(f"Productivo actual: v{current.version}, val_recall={prod_recall:.4f}")
        logger.info(f"Candidato: {best_model_name}, val_recall={best_recall:.4f}")
        should_promote = best_recall > prod_recall
    except Exception:
        logger.info("No hay modelo productivo previo.")

    if should_promote:
        versions = client.search_model_versions(f"name='{MODEL_NAME}'")
        target_version = next((v.version for v in versions if v.run_id == best_run_id), None)
        if target_version:
            client.set_registered_model_alias(MODEL_NAME, "champion", target_version)
            logger.info(f"Promovido: {best_model_name} v{target_version} (val_recall={best_recall:.4f}) -> champion")
    else:
        logger.info("Modelo productivo actual es mejor. No se promueve.")

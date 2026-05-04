# =============================================================================
# Prometheus Metrics
# Métricas expuestas en /metrics:
#   - Total de solicitudes
#   - Solicitudes por segundo
#   - Latencia promedio y por percentiles (p50, p95, p99)
#   - Número y tasa de errores
# =============================================================================

from __future__ import annotations

from prometheus_client import Counter, Histogram

# Histograma de latencia del endpoint de inferencia (segundos; Prometheus expone _bucket/_sum/_count)
PREDICT_LATENCY_SECONDS = Histogram(
    "mlops_predict_latency_seconds",
    "Latencia del endpoint /predict en segundos",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

PREDICT_REQUESTS_TOTAL = Counter(
    "mlops_predict_requests_total",
    "Total de solicitudes a /predict",
    labelnames=("outcome",),
)

PREDICT_ERRORS_TOTAL = Counter(
    "mlops_predict_errors_total",
    "Errores en /predict por tipo",
    labelnames=("error_type",),
)

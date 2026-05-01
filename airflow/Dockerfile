ARG AIRFLOW_BASE_TAG=3.2.0
FROM apache/airflow:${AIRFLOW_BASE_TAG}

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY dags/ /opt/airflow/dags/

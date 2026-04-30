ARG AIRFLOW_BASE_TAG=3.2.0
FROM apache/airflow:${AIRFLOW_BASE_TAG}

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY dags/ /opt/airflow/dags/

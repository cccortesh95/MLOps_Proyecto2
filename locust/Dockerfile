FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY locustfile.py .

EXPOSE 8089

# LOCUST_HOST lo define el ConfigMap de Kubernetes (p. ej. http://api-service:8000)
CMD ["sh", "-c", "locust -f locustfile.py --host ${LOCUST_HOST:-http://api-service:8000}"]

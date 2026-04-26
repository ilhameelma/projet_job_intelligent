#!/bin/bash
set -e
echo "=== Verification environnement ==="
python3 --version
ls /opt/airflow/scrapers/
ls /opt/airflow/scripts/
curl -sf http://minio:9000/minio/health/live && echo "MinIO OK" || echo "MinIO KO"
echo "=== OK ==="
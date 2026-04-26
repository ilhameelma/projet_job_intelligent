#!/bin/bash
set -e
export MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
export MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minio_admin}"
export MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minio_password}"
echo "ETL France Travail..."
python3 /opt/airflow/scripts/etl_france_travail.py
echo "ETL France Travail OK"
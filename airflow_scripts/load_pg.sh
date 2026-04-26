#!/bin/bash
set -e
export MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
export MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minio_admin}"
export MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minio_password}"
export POSTGRES_HOST="${POSTGRES_HOST:-postgres-dwh}"
export POSTGRES_PORT="${POSTGRES_PORT:-5432}"
export POSTGRES_DB="${POSTGRES_DB:-job_intelligent_dwh}"
export POSTGRES_USER="${POSTGRES_USER:-dwh_user}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-dwh_password}"
echo "Chargement PostgreSQL..."
python3 /opt/airflow/scripts/load_to_postgresql.py
echo "PostgreSQL OK"
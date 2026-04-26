#!/bin/bash
set -e
export MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
export MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minio_admin}"
export MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minio_password}"
export FRANCE_TRAVAIL_CLIENT_ID="${FRANCE_TRAVAIL_CLIENT_ID}"
export FRANCE_TRAVAIL_CLIENT_SECRET="${FRANCE_TRAVAIL_CLIENT_SECRET}"
echo "Scraping France Travail..."
python3 /opt/airflow/scrapers/france_travail_api.py
echo "France Travail OK"
#!/bin/bash
set -e
export MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
export MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minio_admin}"
export MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minio_password}"
export ADZUNA_APP_ID="${ADZUNA_APP_ID}"
export ADZUNA_APP_KEY="${ADZUNA_APP_KEY}"
echo "Scraping Adzuna..."
python3 /opt/airflow/scrapers/indeed_scraper.py
echo "Adzuna OK"
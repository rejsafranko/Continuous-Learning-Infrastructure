#!/bin/bash

echo "[INIT] Initializing Airflow DB..."
airflow db upgrade

echo "[INIT] Creating admin user..."
airflow users create \
  --username "${AIRFLOW_USERNAME:-admin}" \
  --firstname "${AIRFLOW_FIRSTNAME:-Admin}" \
  --lastname "${AIRFLOW_LASTNAME:-User}" \
  --role Admin \
  --email "${AIRFLOW_EMAIL:-admin@example.com}" \
  --password "${AIRFLOW_PASSWORD:-admin123}"

echo "[INIT] Setting up AWS connection..."
airflow connections add 'aws_default' \
  --conn-type 'aws' \
  --conn-extra "{\"region_name\": \"eu-central-1\"}"

echo "[INIT] Starting Airflow webserver..."
exec airflow webserver

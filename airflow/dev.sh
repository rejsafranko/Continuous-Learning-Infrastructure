#!/bin/bash
set -euo pipefail

export $(grep -v '^#' .env | xargs)

AWS_REGION="eu-central-1"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text)
ECR_REPO_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/ml-infra"

docker rm -f airflow-container 2>/dev/null || true

docker run -d -p 8080:8080 \
    --env-file .env \
    -v $(pwd)/dags:/opt/airflow/dags \
    -v $(pwd)/scripts:/opt/airflow/scripts \
    --name airflow-container \
    ${ECR_REPO_URI}:latest

echo "Airflow is running at http://localhost:8080"

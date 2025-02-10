#!/bin/bash

export $(grep -v '^#' .env | xargs)

AWS_REGION="eu-central-1"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text)
ECR_REPO_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/ml-infra"

docker run -d -p 8080:8080 \
    --env-file .env \
    -v $(pwd)/dags:/opt/airflow/dags \
    --name airflow-container \
    ${ECR_REPO_URI}:latest

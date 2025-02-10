#!/bin/bash

export $(grep -v '^#' .env | xargs)

AWS_REGION="eu-central-1"
ECR_REPO_NAME="ml-infra"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text)
ECR_REPO_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}"

aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_REPO_URI

docker build \
    --build-arg AIRFLOW_HOME \
    --build-arg AIRFLOW_FIRSTNAME \
    --build-arg AIRFLOW_LASTNAME \
    --build-arg AIRFLOW_EMAIL \
    --build-arg AIRFLOW_PASSWORD \
    --build-arg AWS_ACCESS_KEY \
    --build-arg AWS_SECRET_KEY \
    --build-arg AZURE_CONN_STRING \
    --build-arg SNS_TARGET_ARN \
    --build-arg AWS_CONN_ID \
    --build-arg SNS_REGION \
    -t ${ECR_REPO_URI}:latest .

aws ecr describe-repositories --repository-names $ECR_REPO_NAME --region $AWS_REGION >/dev/null 2>&1 || \
aws ecr create-repository --repository-name $ECR_REPO_NAME --region $AWS_REGION

docker push ${ECR_REPO_URI}:latest

echo "Docker image successfully pushed to AWS ECR: ${ECR_REPO_URI}:latest"

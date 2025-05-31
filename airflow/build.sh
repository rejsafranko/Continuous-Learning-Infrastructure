#!/bin/bash

set -euo pipefail

export $(grep -v '^#' .env | xargs)

AWS_REGION="eu-central-1"
ECR_REPO_NAME="ml-infra"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text)
ECR_REPO_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}"

aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_REPO_URI

aws ecr describe-repositories --repository-names $ECR_REPO_NAME --region $AWS_REGION >/dev/null 2>&1 || \
aws ecr create-repository --repository-name $ECR_REPO_NAME --region $AWS_REGION

docker build -t ${ECR_REPO_URI}:latest .

docker push ${ECR_REPO_URI}:latest

echo "Docker image pushed: ${ECR_REPO_URI}:latest"

#!/bin/bash

set -euo pipefail

export $(grep -v '^#' .env | xargs)

AWS_REGION="eu-central-1"
ECS_CLUSTER_NAME="ml-cluster"
ECS_SERVICE_NAME="infrastructure"
ECS_TASK_DEFINITION="mlinfra-workflow"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text)
ECR_REPO_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/ml-infra"

aws ecs describe-task-definition --task-definition $ECS_TASK_DEFINITION > current-task.json

jq ".taskDefinition |
    del(.taskDefinitionArn, .revision, .status, .requiresAttributes, .compatibilities, .registeredAt, .registeredBy) |
    .containerDefinitions[0].image = \"${ECR_REPO_URI}:latest\"" \
    current-task.json > new-task-def.json

NEW_REVISION=$(aws ecs register-task-definition \
  --cli-input-json file://new-task-def.json \
  --query 'taskDefinition.revision' --output text)

aws ecs update-service \
  --cluster $ECS_CLUSTER_NAME \
  --service $ECS_SERVICE_NAME \
  --task-definition "$ECS_TASK_DEFINITION:$NEW_REVISION"

echo "ECS service '$ECS_SERVICE_NAME' updated with image: ${ECR_REPO_URI}:latest"

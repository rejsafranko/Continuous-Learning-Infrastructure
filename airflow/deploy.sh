#!/bin/bash

export $(grep -v '^#' .env | xargs)

AWS_REGION="eu-central-1"  
ECS_CLUSTER_NAME="ml-cluster"
ECS_SERVICE_NAME="infrastructure"
ECS_TASK_DEFINITION="mlinfra-workflow"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text)
ECR_REPO_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/ml-infra"

aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_REPO_URI

NEW_TASK_DEF=$(aws ecs describe-task-definition --task-definition $ECS_TASK_DEFINITION)

echo "$NEW_TASK_DEF" | jq '.taskDefinition.containerDefinitions[0].image = "'"${ECR_REPO_URI}:latest"'"' > new-task-def.json

NEW_REVISION=$(aws ecs register-task-definition --cli-input-json file://new-task-def.json --query 'taskDefinition.revision' --output text)

aws ecs update-service --cluster $ECS_CLUSTER_NAME --service $ECS_SERVICE_NAME --task-definition "$ECS_TASK_DEFINITION:$NEW_REVISION"

echo "Successfully updated ECS service '$ECS_SERVICE_NAME' with new image: ${ECR_REPO_URI}:latest"
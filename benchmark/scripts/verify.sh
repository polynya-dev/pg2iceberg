#!/usr/bin/env bash
# Run a one-shot pg2iceberg `verify` as a Fargate task (row-by-row diff
# of Postgres vs the Iceberg tables in Glue/S3), wait for it to stop,
# and print its CloudWatch logs + exit code. This is the correctness
# gate. Reads infra coordinates from `tofu output`.
#
# Usage: verify.sh   (run from benchmark/, after `make bench`)
set -euo pipefail

TF_DIR="${TF_DIR:-terraform}"
out() { tofu -chdir="$TF_DIR" output -raw "$1"; }

REGION="$(out region)"
CLUSTER="$(out cluster)"
TASKDEF="$(out pg2iceberg_task_definition)"
SUBNETS="$(tofu -chdir="$TF_DIR" output -json private_subnets | jq -r 'join(",")')"
SG="$(out pg2iceberg_security_group)"
LOG_GROUP="/ecs/$(out cluster)/pg2iceberg"

echo "Launching verify task..."
TASK_ARN="$(aws ecs run-task \
  --region "$REGION" \
  --cluster "$CLUSTER" \
  --launch-type FARGATE \
  --task-definition "$TASKDEF" \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNETS],securityGroups=[$SG],assignPublicIp=DISABLED}" \
  --overrides '{"containerOverrides":[{"name":"pg2iceberg","command":["verify","--config","/tmp/config.yaml"]}]}' \
  --query 'tasks[0].taskArn' --output text)"

echo "Task: $TASK_ARN"
echo "Waiting for verify to finish (this can take a few minutes)..."
aws ecs wait tasks-stopped --region "$REGION" --cluster "$CLUSTER" --tasks "$TASK_ARN"

TASK_ID="${TASK_ARN##*/}"
EXIT_CODE="$(aws ecs describe-tasks --region "$REGION" --cluster "$CLUSTER" --tasks "$TASK_ARN" \
  --query 'tasks[0].containers[0].exitCode' --output text)"

echo "──────── verify logs ────────"
aws logs get-log-events --region "$REGION" \
  --log-group-name "$LOG_GROUP" \
  --log-stream-name "pg2iceberg/pg2iceberg/$TASK_ID" \
  --query 'events[].message' --output text 2>/dev/null || echo "(log stream not found yet)"
echo "─────────────────────────────"
echo "verify exit code: $EXIT_CODE"
exit "${EXIT_CODE:-1}"

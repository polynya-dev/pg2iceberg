#!/usr/bin/env bash
set -euo pipefail

# pg2iceberg AWS benchmark runner
#
# Uses AWS Glue as Iceberg REST catalog (no self-hosted catalog needed).
#
# Prerequisites:
#   - Terraform applied (terraform apply)
#   - AWS CLI configured
#   - Docker for building images
#
# Usage:
#   ./run-aws.sh build                    # Build and push images to ECR
#   ./run-aws.sh init-schema              # Create tables in RDS
#   ./run-aws.sh seed [SIZE]              # Seed data (default: 10GB)
#   ./run-aws.sh start-pg2iceberg         # Start pg2iceberg as ECS task
#   ./run-aws.sh stop-pg2iceberg          # Stop pg2iceberg
#   ./run-aws.sh drain                    # Wait for pg2iceberg to drain
#   ./run-aws.sh verify                   # Run verification
#   ./run-aws.sh scenario [NAME]          # Run full scenario
#   ./run-aws.sh logs [SERVICE]           # Tail CloudWatch logs
#   ./run-aws.sh cleanup                  # Stop all tasks, clean S3

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Load Terraform outputs.
cd "$SCRIPT_DIR"
eval "$(terraform output -json | jq -r 'to_entries[] | "TF_\(.key | ascii_upcase)=\(.value.value)"')"

REGION="$TF_REGION"
CLUSTER="$TF_ECS_CLUSTER"
SUBNET="$TF_SUBNET_ID"
SG="$TF_SECURITY_GROUP_ID"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Benchmark config (override via env).
SEED_SIZE="${SEED_SIZE:-10GB}"
SEED_CONCURRENCY="${SEED_CONCURRENCY:-4}"
SEED_BATCH_SIZE="${SEED_BATCH_SIZE:-5000}"
STREAM_RATE="${STREAM_RATE:-1000}"
STREAM_DURATION="${STREAM_DURATION:-15m}"
INSERT_PCT="${INSERT_PCT:-70}"
UPDATE_PCT="${UPDATE_PCT:-20}"

log() {
    echo "[$(date '+%H:%M:%S')] $*"
}

# ============================================================
# Build & push images
# ============================================================

cmd_build() {
    log "Logging into ECR..."
    aws ecr get-login-password --region "$REGION" | \
        docker login --username AWS --password-stdin "$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com"

    log "Building and pushing images..."

    # pg2iceberg
    docker build -t "$TF_ECR_PG2ICEBERG:latest" -f "$REPO_ROOT/Dockerfile" "$REPO_ROOT"
    docker push "$TF_ECR_PG2ICEBERG:latest"

    # bench-writer
    docker build -t "$TF_ECR_BENCH_WRITER:latest" \
        --target bench-writer \
        -f "$REPO_ROOT/benchmark/Dockerfile.bench" "$REPO_ROOT"
    docker push "$TF_ECR_BENCH_WRITER:latest"

    # bench-verify
    docker build -t "$TF_ECR_BENCH_VERIFY:latest" \
        --target bench-verify \
        -f "$REPO_ROOT/benchmark/Dockerfile.bench" "$REPO_ROOT"
    docker push "$TF_ECR_BENCH_VERIFY:latest"

    log "All images pushed."
}

# ============================================================
# Schema init
# ============================================================

cmd_init_schema() {
    log "Initializing schema on RDS..."
    local task_arn
    task_arn=$(aws ecs run-task \
        --cluster "$CLUSTER" \
        --task-definition "$TF_BENCH_WRITER_TASK_DEF" \
        --launch-type FARGATE \
        --network-configuration "awsvpcConfiguration={subnets=[$SUBNET],securityGroups=[$SG],assignPublicIp=ENABLED}" \
        --overrides "{\"containerOverrides\":[{\"name\":\"bench-writer\",\"command\":[\"--mode\",\"init\"]}]}" \
        --query 'tasks[0].taskArn' --output text)

    log "Schema init task: $task_arn"
    aws ecs wait tasks-stopped --cluster "$CLUSTER" --tasks "$task_arn"

    local exit_code
    exit_code=$(aws ecs describe-tasks --cluster "$CLUSTER" --tasks "$task_arn" \
        --query 'tasks[0].containers[0].exitCode' --output text)

    if [ "$exit_code" = "0" ]; then
        log "Schema initialized."
    else
        log "ERROR: Schema init failed (exit code $exit_code). Check CloudWatch logs."
        return 1
    fi
}

# ============================================================
# Run ECS tasks
# ============================================================

run_task() {
    local task_def="$1"
    shift
    local container_name="$1"
    shift
    local cmd="$*"

    local overrides
    overrides=$(jq -n --arg name "$container_name" --arg cmd "$cmd" \
        '{containerOverrides: [{name: $name, command: ($cmd | split(" "))}]}')

    local task_arn
    task_arn=$(aws ecs run-task \
        --cluster "$CLUSTER" \
        --task-definition "$task_def" \
        --launch-type FARGATE \
        --network-configuration "awsvpcConfiguration={subnets=[$SUBNET],securityGroups=[$SG],assignPublicIp=ENABLED}" \
        --overrides "$overrides" \
        --query 'tasks[0].taskArn' --output text)

    echo "$task_arn"
}

wait_task() {
    local task_arn="$1"
    log "Waiting for task $task_arn..."
    aws ecs wait tasks-stopped --cluster "$CLUSTER" --tasks "$task_arn"

    local exit_code
    exit_code=$(aws ecs describe-tasks --cluster "$CLUSTER" --tasks "$task_arn" \
        --query 'tasks[0].containers[0].exitCode' --output text)

    if [ "$exit_code" != "0" ]; then
        log "ERROR: Task exited with code $exit_code"
        return 1
    fi
    log "Task completed successfully."
}

cmd_seed() {
    local size="${1:-$SEED_SIZE}"
    log "Seeding $size of data..."
    local task_arn
    task_arn=$(run_task "$TF_BENCH_WRITER_TASK_DEF" "bench-writer" \
        "--mode seed --seed-size $size --batch-size $SEED_BATCH_SIZE --concurrency $SEED_CONCURRENCY")
    log "Seed task: $task_arn"
    wait_task "$task_arn"
    log "Seed complete."
}

cmd_stream() {
    local rate="${1:-$STREAM_RATE}"
    local duration="${2:-$STREAM_DURATION}"
    log "Streaming at $rate rows/s for $duration..."
    local task_arn
    task_arn=$(run_task "$TF_BENCH_WRITER_TASK_DEF" "bench-writer" \
        "--mode stream --rate $rate --duration $duration --insert-pct $INSERT_PCT --update-pct $UPDATE_PCT --batch-size 100")
    log "Stream task: $task_arn"
    wait_task "$task_arn"
    log "Stream complete."
}

# ============================================================
# pg2iceberg lifecycle
# ============================================================

PG2ICEBERG_TASK_ARN=""

get_pg2iceberg_ip() {
    local eni
    eni=$(aws ecs describe-tasks --cluster "$CLUSTER" --tasks "$PG2ICEBERG_TASK_ARN" \
        --query 'tasks[0].attachments[0].details[?name==`networkInterfaceId`].value' --output text)
    aws ec2 describe-network-interfaces --network-interface-ids "$eni" \
        --query 'NetworkInterfaces[0].PrivateIpAddresses[0].PrivateIpAddress' --output text
}

cmd_start_pg2iceberg() {
    log "Starting pg2iceberg..."
    PG2ICEBERG_TASK_ARN=$(run_task "$TF_PG2ICEBERG_TASK_DEF" "pg2iceberg" \
        "--config /dev/null")
    log "pg2iceberg task: $PG2ICEBERG_TASK_ARN"

    # Wait for task to be running.
    aws ecs wait tasks-running --cluster "$CLUSTER" --tasks "$PG2ICEBERG_TASK_ARN"

    PG2ICEBERG_IP=$(get_pg2iceberg_ip)
    METRICS_URL="http://$PG2ICEBERG_IP:9090/metrics"
    log "pg2iceberg running at $PG2ICEBERG_IP"

    # Wait for metrics endpoint.
    local retries=0
    while ! curl -sf "$METRICS_URL" > /dev/null 2>&1; do
        retries=$((retries + 1))
        if [ "$retries" -ge 30 ]; then
            log "WARNING: metrics not available after 30s"
            break
        fi
        sleep 1
    done
    log "pg2iceberg ready."
}

cmd_stop_pg2iceberg() {
    if [ -n "$PG2ICEBERG_TASK_ARN" ]; then
        log "Stopping pg2iceberg..."
        aws ecs stop-task --cluster "$CLUSTER" --task "$PG2ICEBERG_TASK_ARN" \
            --reason "benchmark complete" > /dev/null
        log "pg2iceberg stopped."
    fi
}

cmd_drain() {
    if [ -z "${METRICS_URL:-}" ]; then
        log "ERROR: pg2iceberg not running (no METRICS_URL)"
        return 1
    fi

    local linger="${1:-true}"
    local start_time=$(date +%s)
    log "Waiting for pg2iceberg to drain..."

    while true; do
        local elapsed=$(( $(date +%s) - start_time ))
        local metrics
        metrics=$(curl -sf "$METRICS_URL" 2>/dev/null || echo "")
        if [ -z "$metrics" ]; then
            sleep 2
            continue
        fi

        local buffered
        buffered=$(echo "$metrics" | grep -o '"buffered_rows":[0-9]*' | grep -o '[0-9]*' || echo "-1")
        local status
        status=$(echo "$metrics" | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4 || echo "unknown")
        local rows_processed
        rows_processed=$(echo "$metrics" | grep -o '"rows_processed":[0-9]*' | grep -o '[0-9]*' || echo "0")
        local bytes_processed
        bytes_processed=$(echo "$metrics" | grep -o '"bytes_processed":[0-9]*' | grep -o '[0-9]*' || echo "0")

        local size_human=""
        local throughput=""
        if [ "$bytes_processed" -gt 0 ] 2>/dev/null; then
            if [ "$bytes_processed" -ge 1073741824 ]; then
                size_human="$(awk "BEGIN{printf \"%.2fGB\", $bytes_processed/1073741824}")"
            else
                size_human="$(awk "BEGIN{printf \"%.0fMB\", $bytes_processed/1048576}")"
            fi
            if [ "$elapsed" -gt 0 ]; then
                throughput=" $(awk "BEGIN{printf \"%.1fMB/s\", $bytes_processed/$elapsed/1048576}")"
            fi
        fi

        log "  status=$status buffered_rows=$buffered rows_processed=$rows_processed bytes=${size_human:-0}${throughput} elapsed=${elapsed}s"

        if [ "$buffered" = "0" ] && [ "$status" = "running" ]; then
            if [ "$linger" = "true" ]; then
                log "Buffer empty, waiting one flush interval (10s)..."
                sleep 10
                metrics=$(curl -sf "$METRICS_URL" 2>/dev/null || echo "")
                buffered=$(echo "$metrics" | grep -o '"buffered_rows":[0-9]*' | grep -o '[0-9]*' || echo "-1")
                if [ "$buffered" = "0" ]; then
                    log "Drain complete (${elapsed}s)"
                    return 0
                fi
            else
                log "Drain complete (${elapsed}s)"
                return 0
            fi
        fi

        sleep 2
    done
}

cmd_verify() {
    log "Running verification..."
    local task_arn
    task_arn=$(run_task "$TF_BENCH_VERIFY_TASK_DEF" "bench-verify" "")
    log "Verify task: $task_arn"
    wait_task "$task_arn"
    log "VERIFICATION PASSED"
}

# ============================================================
# Scenarios
# ============================================================

cmd_scenario() {
    local scenario="${1:-snapshot}"

    case "$scenario" in
        snapshot)
            log "=== Scenario: snapshot (${SEED_SIZE}) ==="
            cmd_seed "$SEED_SIZE"
            cmd_start_pg2iceberg
            cmd_drain false
            cmd_stop_pg2iceberg
            cmd_verify
            ;;
        steady-state)
            log "=== Scenario: steady-state ==="
            cmd_start_pg2iceberg
            cmd_stream "$STREAM_RATE" "$STREAM_DURATION"
            cmd_drain true
            cmd_stop_pg2iceberg
            cmd_verify
            ;;
        catchup)
            log "=== Scenario: catchup ==="
            cmd_start_pg2iceberg
            cmd_seed "$SEED_SIZE"
            cmd_drain false
            cmd_stop_pg2iceberg
            cmd_stream 5000 100s
            cmd_start_pg2iceberg
            cmd_drain true
            cmd_stop_pg2iceberg
            cmd_verify
            ;;
        *)
            echo "Unknown scenario: $scenario"
            echo "Available: snapshot, steady-state, catchup"
            exit 1
            ;;
    esac
}

# ============================================================
# Logs
# ============================================================

cmd_logs() {
    local service="${1:-pg2iceberg}"
    aws logs tail "/ecs/$CLUSTER" \
        --filter-pattern "$service" \
        --follow \
        --since 1h
}

# ============================================================
# Cleanup
# ============================================================

cmd_cleanup() {
    log "Stopping all running tasks..."
    local tasks
    tasks=$(aws ecs list-tasks --cluster "$CLUSTER" --query 'taskArns[]' --output text)
    for task in $tasks; do
        aws ecs stop-task --cluster "$CLUSTER" --task "$task" --reason "cleanup" > /dev/null 2>&1 || true
    done

    log "Clearing S3 bucket..."
    aws s3 rm "s3://$TF_S3_BUCKET" --recursive > /dev/null 2>&1 || true

    log "Cleanup complete."
}

# ============================================================
# Main
# ============================================================

CMD="${1:-help}"
shift || true

case "$CMD" in
    build)              cmd_build ;;
    init-schema)        cmd_init_schema ;;
    seed)               cmd_seed "$@" ;;
    stream)             cmd_stream "$@" ;;
    start-pg2iceberg)   cmd_start_pg2iceberg ;;
    stop-pg2iceberg)    cmd_stop_pg2iceberg ;;
    drain)              cmd_drain "$@" ;;
    verify)             cmd_verify ;;
    scenario)           cmd_scenario "$@" ;;
    logs)               cmd_logs "$@" ;;
    cleanup)            cmd_cleanup ;;
    *)
        echo "Usage: $0 <command>"
        echo ""
        echo "Commands:"
        echo "  build               Build and push images to ECR"
        echo "  init-schema         Create tables in RDS"
        echo "  seed [SIZE]         Seed data (default: $SEED_SIZE)"
        echo "  stream [RATE] [DUR] Stream changes"
        echo "  start-pg2iceberg    Start pg2iceberg ECS task"
        echo "  stop-pg2iceberg     Stop pg2iceberg"
        echo "  drain [LINGER]      Wait for drain (true/false)"
        echo "  verify              Run correctness verification"
        echo "  scenario [NAME]     Run full scenario (snapshot, steady-state, catchup)"
        echo "  logs [SERVICE]      Tail CloudWatch logs"
        echo "  cleanup             Stop tasks, clear S3"
        exit 1
        ;;
esac

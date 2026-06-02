#!/bin/sh
# Render the pg2iceberg config from env (the binary reads YAML only, no
# env overrides), then exec the binary with whatever subcommand the task
# passed (run / verify / snapshot / ...). Writing to /tmp keeps it
# writable for the unprivileged `pg2iceberg` user.
set -eu

# Fargate task-role credentials → AWS_* env vars. pg2iceberg's blob store
# (object_store) reads the ECS container-credentials endpoint directly,
# but the iceberg-rust catalog's S3 IO (opendal/reqsign) only checks env
# vars / IMDS — neither of which is the Fargate container endpoint. So we
# fetch the creds once and export them. NOTE: these are a point-in-time
# snapshot (~6h TTL) — fine for a benchmark run; a long-lived deployment
# wants an auto-refreshing opendal credential loader instead.
if [ -n "${AWS_CONTAINER_CREDENTIALS_RELATIVE_URI:-}" ]; then
  _creds="$(curl -fsS "http://169.254.170.2${AWS_CONTAINER_CREDENTIALS_RELATIVE_URI}")" || _creds=""
  if [ -n "$_creds" ]; then
    AWS_ACCESS_KEY_ID="$(printf '%s' "$_creds" | jq -r .AccessKeyId)"
    AWS_SECRET_ACCESS_KEY="$(printf '%s' "$_creds" | jq -r .SecretAccessKey)"
    AWS_SESSION_TOKEN="$(printf '%s' "$_creds" | jq -r .Token)"
    export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
    echo "exported task-role credentials to AWS_* env (key ...${AWS_ACCESS_KEY_ID#"${AWS_ACCESS_KEY_ID%????}"})"
  fi
fi

envsubst '$PG_HOST $PG_PORT $PG_DATABASE $PG_USER $PG_PASSWORD $REGION $WAREHOUSE $GLUE_CATALOG_ID $NAMESPACE $FLUSH_INTERVAL $FLUSH_ROWS $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY $AWS_SESSION_TOKEN' \
  < /etc/pg2iceberg/config.yaml.tmpl > /tmp/config.yaml

exec /usr/local/bin/pg2iceberg "$@"

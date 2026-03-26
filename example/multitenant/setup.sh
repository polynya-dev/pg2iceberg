#!/bin/sh
set -e

API="http://pg2iceberg:8080/api/v1/pipelines"

echo "Creating rideshare pipeline..."
curl -sf -X POST "$API" -H "Content-Type: application/json" -d '{
  "id": "rideshare",
  "config": {
    "source": {
      "mode": "logical",
      "postgres": {
        "host": "postgres-rideshare",
        "port": 5432,
        "database": "rideshare",
        "user": "postgres",
        "password": "postgres"
      },
      "logical": {
        "publication_name": "pg2iceberg_pub",
        "slot_name": "pg2iceberg_slot",
        "tables": [
          "public.riders",
          "public.drivers",
          "public.rides",
          "public.payments",
          "public.ratings"
        ]
      }
    },
    "sink": {
      "catalog_uri": "http://iceberg-rest:8181",
      "warehouse": "s3://warehouse/",
      "namespace": "rideshare",
      "s3_endpoint": "http://minio:9000",
      "s3_access_key": "admin",
      "s3_secret_key": "password",
      "s3_region": "us-east-1",
      "flush_interval": "10s",
      "flush_rows": 1000
    }
  }
}'
echo ""

echo "Creating cashcat pipeline..."
curl -sf -X POST "$API" -H "Content-Type: application/json" -d '{
  "id": "cashcat",
  "config": {
    "source": {
      "mode": "logical",
      "postgres": {
        "host": "postgres-cashcat",
        "port": 5432,
        "database": "cashcat",
        "user": "postgres",
        "password": "postgres"
      },
      "logical": {
        "publication_name": "pg2iceberg_pub",
        "slot_name": "pg2iceberg_slot",
        "tables": [
          "public.customers",
          "public.accounts",
          "public.transactions",
          "public.cards",
          "public.support_tickets"
        ]
      }
    },
    "sink": {
      "catalog_uri": "http://iceberg-rest:8181",
      "warehouse": "s3://warehouse/",
      "namespace": "cashcat",
      "s3_endpoint": "http://minio:9000",
      "s3_access_key": "admin",
      "s3_secret_key": "password",
      "s3_region": "us-east-1",
      "flush_interval": "10s",
      "flush_rows": 1000
    }
  }
}'
echo ""

echo "Creating todo-app pipeline..."
curl -sf -X POST "$API" -H "Content-Type: application/json" -d '{
  "id": "todo-app",
  "config": {
    "source": {
      "mode": "logical",
      "postgres": {
        "host": "postgres-todoapp",
        "port": 5432,
        "database": "todo_app",
        "user": "postgres",
        "password": "postgres"
      },
      "logical": {
        "publication_name": "pg2iceberg_pub",
        "slot_name": "pg2iceberg_slot",
        "tables": [
          "public.users",
          "public.todos",
          "public.labels",
          "public.todo_labels"
        ]
      }
    },
    "sink": {
      "catalog_uri": "http://iceberg-rest:8181",
      "warehouse": "s3://warehouse/",
      "namespace": "todo_app",
      "s3_endpoint": "http://minio:9000",
      "s3_access_key": "admin",
      "s3_secret_key": "password",
      "s3_region": "us-east-1",
      "flush_interval": "10s",
      "flush_rows": 1000
    }
  }
}'
echo ""

echo "All pipelines created. Verifying..."
curl -sf "$API" | cat
echo ""
echo "Done!"

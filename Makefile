.PHONY: build run server docker-up docker-down simulate query clean test

build:
	go build -o bin/pg2iceberg ./cmd/pg2iceberg

run: build
	./bin/pg2iceberg --config config.example.yaml

server: build
	@docker compose exec -T postgres psql -U postgres -c "CREATE DATABASE pg2iceberg" 2>/dev/null || true
	./bin/pg2iceberg --server --listen :8080 \
		--store-dsn "host=localhost port=5434 dbname=pg2iceberg user=postgres password=postgres sslmode=disable" \
		--clickhouse-addr http://localhost:8123 \
		--clickhouse-catalog-uri http://iceberg-rest:8181/v1 \
		--clickhouse-s3-endpoint http://minio:9000 \
		--clickhouse-warehouse "s3://warehouse/"

docker-up:
	docker compose up -d
	@echo "Waiting for services to be ready..."
	@sleep 5
	@echo "Services ready: postgres=5432 minio=9000 iceberg-rest=8181 clickhouse=8123"

docker-down:
	docker compose down -v

simulate:
	pip install -q -r scripts/requirements.txt
	python scripts/simulate.py

query:
	@docker compose exec clickhouse clickhouse-client --query "SELECT * FROM iceberg.\`default.orders\` ORDER BY id LIMIT 20"

query-count:
	@docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM iceberg.\`default.orders\`"

query-sql:
	@docker compose exec clickhouse clickhouse-client

test:
	./tests/run.sh

clean:
	rm -rf bin/ pg2iceberg-state.json

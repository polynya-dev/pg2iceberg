.PHONY: build run docker-up docker-down simulate query clean test

build:
	go build -o bin/pg2iceberg ./cmd/pg2iceberg

run: build
	./bin/pg2iceberg --config config.example.yaml

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

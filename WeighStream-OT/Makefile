.PHONY: up down clean init seed seed-bg dbt logs

COMPOSE = docker compose
FLINK_JM = weighstream-flink-jm

# ── Lifecycle ─────────────────────────────────────────────────────────────────

up:
	@echo "▶ Starting all services..."
	$(COMPOSE) up -d --build
	@echo "✓ Stack is up. Waiting for Trino..."
	@until curl -sf http://localhost:8080/v1/info > /dev/null; do sleep 3; done
	@echo "✓ Trino ready."

down:
	@echo "▶ Stopping services (volumes preserved)..."
	$(COMPOSE) down

clean:
	@echo "▶ Stopping services and wiping volumes..."
	$(COMPOSE) down -v --remove-orphans

# ── Initialise pipeline ───────────────────────────────────────────────────────

init: _create-topics _create-namespaces _submit-flink-job
	@echo "✓ Pipeline initialised."

_create-topics:
	@echo "▶ Creating Kafka topics..."
	docker exec weighstream-kafka \
	  kafka-topics --bootstrap-server localhost:9092 \
	  --create --if-not-exists --topic weigh.readings.raw \
	  --partitions 3 --replication-factor 1
	docker exec weighstream-kafka \
	  kafka-topics --bootstrap-server localhost:9092 \
	  --create --if-not-exists --topic weigh.readings.dlq \
	  --partitions 1 --replication-factor 1
	@echo "✓ Topics created."

_create-namespaces:
	@echo "▶ Creating Iceberg namespaces via Trino..."
	docker exec weighstream-trino \
	  trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.bronze WITH (location='s3://warehouse/bronze/')"
	docker exec weighstream-trino \
	  trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.silver WITH (location='s3://warehouse/silver/')"
	docker exec weighstream-trino \
	  trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.gold   WITH (location='s3://warehouse/gold/')"
	@echo "✓ Iceberg namespaces ready."

_submit-flink-job:
	@echo "▶ Submitting Flink SQL job..."
	docker exec $(FLINK_JM) /bin/bash -c \
	  "cat /opt/flink/sql/01_kafka_source.sql \
	        /opt/flink/sql/02_iceberg_sink_bronze.sql \
	        /opt/flink/sql/03_silver_dedup_enrich.sql \
	   | /opt/flink/bin/sql-client.sh embedded"
	@echo "✓ Flink job submitted."

# ── Producer ──────────────────────────────────────────────────────────────────

seed:
	@echo "▶ Starting device simulator (Ctrl-C to stop)..."
	$(COMPOSE) --profile seed up producer

seed-bg:
	@echo "▶ Starting device simulator in background..."
	$(COMPOSE) --profile seed up -d producer

# ── dbt ───────────────────────────────────────────────────────────────────────

dbt:
	@echo "▶ Running dbt (run + test)..."
	docker run --rm --network weighstream \
	  -v $(PWD)/dbt/weighstream:/usr/app/dbt \
	  -e TRINO_HOST=trino \
	  -e TRINO_PORT=8080 \
	  ghcr.io/dbt-labs/dbt-trino:1.8.0 \
	  sh -c "cd /usr/app/dbt && dbt deps && dbt run && dbt test"

dbt-snapshot:
	@echo "▶ Running dbt snapshots..."
	docker run --rm --network weighstream \
	  -v $(PWD)/dbt/weighstream:/usr/app/dbt \
	  -e TRINO_HOST=trino \
	  -e TRINO_PORT=8080 \
	  ghcr.io/dbt-labs/dbt-trino:1.8.0 \
	  sh -c "cd /usr/app/dbt && dbt snapshot"

# ── Logs ──────────────────────────────────────────────────────────────────────

logs:
	$(COMPOSE) logs -f --tail=100

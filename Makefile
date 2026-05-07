COMPOSE    = docker compose
DOCKER_DIR = infrastructure/docker
ENV_FILE   = .env

# ── Guard: require .env to exist before any target that starts containers ────
_check_env:
	@test -f $(ENV_FILE) || { \
	  echo "ERROR: $(ENV_FILE) not found."; \
	  echo "Copy .env.example to .env and fill in credentials first:"; \
	  echo "  cp .env.example .env"; \
	  exit 1; \
	}

.PHONY: base
base: _check_env
	$(COMPOSE) -f $(DOCKER_DIR)/docker-compose.base.yml --env-file $(ENV_FILE) up -d

.PHONY: security
security: base
	$(COMPOSE) -f $(DOCKER_DIR)/docker-compose.security.yml --env-file $(ENV_FILE) up -d

.PHONY: storage
storage: base
	$(COMPOSE) -f $(DOCKER_DIR)/docker-compose.storage.yml --env-file $(ENV_FILE) up -d

.PHONY: ingestion
ingestion: base
	$(COMPOSE) -f $(DOCKER_DIR)/docker-compose.ingestion.yml --env-file $(ENV_FILE) up -d

.PHONY: transform
transform: storage
	$(COMPOSE) -f $(DOCKER_DIR)/docker-compose.transform.yml --env-file $(ENV_FILE) up -d

.PHONY: serving
serving: base
	$(COMPOSE) -f $(DOCKER_DIR)/docker-compose.serving.yml --env-file $(ENV_FILE) up -d

.PHONY: governance
governance: serving transform
	$(COMPOSE) -f $(DOCKER_DIR)/docker-compose.governance.yml --env-file $(ENV_FILE) up -d

.PHONY: observability
observability: base
	$(COMPOSE) -f $(DOCKER_DIR)/docker-compose.observability.yml --env-file $(ENV_FILE) up -d

.PHONY: visualization
visualization: serving
	$(COMPOSE) -f $(DOCKER_DIR)/docker-compose.visualization.yml --env-file $(ENV_FILE) up -d

.PHONY: sources
sources: base
	$(COMPOSE) -f $(DOCKER_DIR)/docker-compose.sources.yml --env-file $(ENV_FILE) up -d --build
	@echo 'MySQL + SAP API started. Run: make seed-mysql to load synthetic data.'

.PHONY: seed-mysql
seed-mysql:
	@echo 'Seeding ShopFlow MySQL with 50K customers / 5K products / 500K orders ...'
	pip install faker mysql-connector-python -q
	python scripts/generate_mysql_data.py

.PHONY: seed-mysql-small
seed-mysql-small:
	@echo 'Seeding ShopFlow MySQL with small dataset (500/200/2000) ...'
	pip install faker mysql-connector-python -q
	python scripts/generate_mysql_data.py --small

.PHONY: seed-saas
seed-saas:
	@echo 'Seeding SaaS PostgreSQL with 10K users / 200K events / 10K subscriptions ...'
	pip install faker psycopg2-binary -q
	python scripts/generate_saas_data.py

.PHONY: seed-saas-small
seed-saas-small:
	@echo 'Seeding SaaS PostgreSQL with small dataset (500/5000/500) ...'
	pip install faker psycopg2-binary -q
	python scripts/generate_saas_data.py --small

.PHONY: seed-all
seed-all: seed-mysql seed-saas
	@echo 'All synthetic data loaded at production scale.'

.PHONY: ai-agent
ai-agent: serving transform
	$(COMPOSE) -f $(DOCKER_DIR)/docker-compose.ai-agent.yml --env-file $(ENV_FILE) up -d --build
	@echo 'AI Agent (FastAPI) started at http://localhost:8502'

.PHONY: deploy
deploy:
	@echo '==> Building and pushing AI Agent Docker image to GHCR...'
	docker buildx build \
	  --platform linux/amd64 \
	  --push \
	  -t ghcr.io/$(shell git config user.name | tr '[:upper:]' '[:lower:]' | tr ' ' '-')/datalake-ai-agent:latest \
	  -t ghcr.io/$(shell git config user.name | tr '[:upper:]' '[:lower:]' | tr ' ' '-')/datalake-ai-agent:$(shell git rev-parse --short HEAD) \
	  services/ai-agent/
	@echo '==> Building React frontend production bundle...'
	cd services/ai-agent-v2/frontend && npm ci && npm run build
	@echo '==> Deploy complete.'

.PHONY: ui
ui:
	@bash scripts/start_ui.sh

.PHONY: streaming
streaming: base
	$(COMPOSE) -f $(DOCKER_DIR)/docker-compose.streaming.yml --env-file $(ENV_FILE) up -d
	@echo 'Kafka (KRaft) + Kafka Connect (Debezium) started.'
	@echo 'Register the MySQL connector: make setup-debezium'

.PHONY: setup-debezium
setup-debezium:
	@echo 'Registering Debezium MySQL connector (idempotent)...'
	@MYSQL_PASS=$$(grep "^MYSQL_ROOT_PASSWORD=" $(ENV_FILE) | cut -d= -f2) && \
	  jq --arg pw "$$MYSQL_PASS" '.config["database.password"] = $$pw | .config' \
	    infrastructure/kafka/debezium-connector-mysql.json | \
	  docker exec -i kafka-connect curl -s -w "\nHTTP %{http_code}" -X PUT \
	    -H "Content-Type: application/json" --data @- \
	    http://localhost:8083/connectors/shopflow-mysql-connector/config
	@echo 'Done. Topics will appear at kafka:9092 once the initial snapshot completes.'

.PHONY: all
all: base security storage ingestion transform serving governance observability visualization sources streaming
	@echo 'Full stack started. Watch RAM: make ram'

# ── Keycloak: provision datalake realm + Grafana OIDC client ─────────────────
.PHONY: setup-keycloak
setup-keycloak:
	@echo 'Provisioning Keycloak datalake realm and Grafana OIDC client...'
	@set -a && . $(ENV_FILE) && set +a && python scripts/setup_keycloak.py
	@echo ''
	@echo 'Recreating Grafana container to activate SSO env vars...'
	$(COMPOSE) -f $(DOCKER_DIR)/docker-compose.observability.yml --env-file $(ENV_FILE) up -d grafana
	@echo 'Done. Visit http://localhost:3000 → Sign in with Keycloak'

# ── Bronze pipeline without Airbyte (MySQL + HTTP → MinIO → ClickHouse views) ─
.PHONY: bronze-init
bronze-init:
	@echo 'Step 1/3 — Ingesting MySQL ShopFlow → MinIO Parquet ...'
	pip install mysql-connector-python pyarrow minio -q
	python scripts/ingest_mysql_to_minio.py
	@echo 'Step 2/3 — Ingesting SAP API + JSONPlaceholder → MinIO Parquet ...'
	pip install pyarrow minio requests -q
	python scripts/ingest_http_to_minio.py
	@echo 'Step 3/3 — Creating ClickHouse bronze S3 views ...'
	pip install clickhouse-connect -q
	python scripts/setup_clickhouse_bronze.py
	@echo 'Bronze layer ready. Run: make transform && dbt run'

.PHONY: bronze-init-small
bronze-init-small:
	@echo 'Step 1/3 — Ingesting MySQL (small) → MinIO ...'
	pip install mysql-connector-python pyarrow minio -q
	python scripts/ingest_mysql_to_minio.py --small
	@echo 'Step 2/3 — Ingesting HTTP sources → MinIO ...'
	python scripts/ingest_http_to_minio.py
	@echo 'Step 3/3 — Creating ClickHouse bronze S3 views ...'
	pip install clickhouse-connect -q
	python scripts/setup_clickhouse_bronze.py
	@echo 'Bronze layer ready (small dataset).'

# ── Superset: provision datasource + dashboards, then export to git ───────────
.PHONY: setup-superset
setup-superset:
	@echo 'Setting up Superset dashboards (ClickHouse datasource + charts + dashboard)...'
	@set -a && . $(ENV_FILE) && set +a && python scripts/setup_superset.py
	@echo 'Done. Dashboard exported to infrastructure/superset/dashboards/'

.PHONY: export-superset
export-superset:
	@echo 'Exporting Superset dashboards to git ...'
	@set -a && . $(ENV_FILE) && set +a && python scripts/setup_superset.py
	@echo 'Commit infrastructure/superset/dashboards/ to persist dashboard state.'

.PHONY: import-superset
import-superset:
	@echo 'Importing Superset dashboards from git ...'
	@set -a && . $(ENV_FILE) && set +a && python scripts/import_superset_dashboards.py

# ── Vault secrets bootstrap (run after make security) ─────────────────────────
.PHONY: setup-vault
setup-vault:
	@echo 'Writing pipeline credentials to HashiCorp Vault...'
	python scripts/setup_vault_secrets.py
	@echo 'Done. Verify: curl -s -H "X-Vault-Token: root" http://localhost:8200/v1/secret/data/datalake/clickhouse | python3 -m json.tool'

# ── Spark: cross-domain unified customer profile (ShopFlow × SaaS) ───────────
.PHONY: spark-unified
spark-unified:
	@echo 'Submitting unified customer profile job to Spark...'
	spark-submit \
	  --master spark://localhost:7077 \
	  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.clickhouse:clickhouse-jdbc:0.4.6,org.apache.httpcomponents.client5:httpclient5:5.2.1 \
	  --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \
	  --conf spark.hadoop.fs.s3a.access.key=admin \
	  --conf "spark.hadoop.fs.s3a.secret.key=$$(grep MINIO_ROOT_PASSWORD $(ENV_FILE) | cut -d= -f2)" \
	  --conf spark.hadoop.fs.s3a.path.style.access=true \
	  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
	  --conf spark.sql.adaptive.enabled=true \
	  services/spark/jobs/unified_customer_profile.py
	@echo 'Done. Query results: SELECT customer_type, count() FROM gold.unified_customers GROUP BY 1'

# ── Spark: run PySpark data profiler against MinIO bronze layer ───────────────
.PHONY: spark-profile
spark-profile:
	@echo 'Submitting MinIO data profiler to Spark...'
	spark-submit \
	  --master spark://localhost:7077 \
	  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
	  --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \
	  --conf spark.hadoop.fs.s3a.access.key=admin \
	  --conf "spark.hadoop.fs.s3a.secret.key=$$(grep MINIO_ROOT_PASSWORD $(ENV_FILE) | cut -d= -f2)" \
	  --conf spark.hadoop.fs.s3a.path.style.access=true \
	  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
	  services/spark/jobs/minio_profiler.py
	@echo 'Profiling report written to MinIO: s3://logs/profiling/'

# ── Tests ─────────────────────────────────────────────────────────────────────
.PHONY: test
test:
	@echo 'Running unit tests...'
	pip install pytest pytest-mock faker mysql-connector-python psycopg2-binary -q
	pytest tests/test_data_generators.py tests/test_saas_pipeline_logic.py -v --tb=short
	@echo 'Done.'

.PHONY: test-integration
test-integration:
	@echo 'Starting test services...'
	$(COMPOSE) -f $(DOCKER_DIR)/docker-compose.test.yml up -d
	@echo 'Waiting for services to be healthy...'
	@until curl -sf http://localhost:18123/ping > /dev/null 2>&1; do sleep 3; done; echo '  ClickHouse ready.'
	@until docker exec postgres-test pg_isready -U postgres -d saas_test > /dev/null 2>&1; do sleep 3; done; echo '  PostgreSQL ready.'
	@echo 'Running integration tests...'
	pip install pytest psycopg2-binary requests -q
	TEST_CLICKHOUSE_HOST=localhost TEST_CLICKHOUSE_PORT=18123 \
	TEST_CLICKHOUSE_USER=default TEST_CLICKHOUSE_PASSWORD=test_password \
	TEST_POSTGRES_HOST=localhost TEST_POSTGRES_PORT=15432 \
	TEST_POSTGRES_USER=postgres TEST_POSTGRES_PASSWORD=test_password \
	TEST_POSTGRES_DB=saas_test \
	  pytest tests/integration/ -v --tb=short 2>&1 \
	  || ($(COMPOSE) -f $(DOCKER_DIR)/docker-compose.test.yml down && exit 1)
	$(COMPOSE) -f $(DOCKER_DIR)/docker-compose.test.yml down
	@echo 'Integration tests done.'

# ── Operations ────────────────────────────────────────────────────────────────

.PHONY: down
down:
	$(COMPOSE) \
	  -f $(DOCKER_DIR)/docker-compose.base.yml \
	  -f $(DOCKER_DIR)/docker-compose.security.yml \
	  -f $(DOCKER_DIR)/docker-compose.storage.yml \
	  -f $(DOCKER_DIR)/docker-compose.ingestion.yml \
	  -f $(DOCKER_DIR)/docker-compose.transform.yml \
	  -f $(DOCKER_DIR)/docker-compose.serving.yml \
	  -f $(DOCKER_DIR)/docker-compose.governance.yml \
	  -f $(DOCKER_DIR)/docker-compose.observability.yml \
	  -f $(DOCKER_DIR)/docker-compose.visualization.yml \
	  -f $(DOCKER_DIR)/docker-compose.sources.yml \
	  -f $(DOCKER_DIR)/docker-compose.ai-agent.yml \
	  -f $(DOCKER_DIR)/docker-compose.streaming.yml \
	  --env-file $(ENV_FILE) down

.PHONY: logs
logs:
	docker compose logs -f --tail=50

.PHONY: ps
ps:
	docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'

.PHONY: ram
ram:
	docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}'

.PHONY: clean
clean: down
	docker system prune -f
	@echo 'Containers and networks removed. Volumes preserved.'

# ── nuke: DESTRUCTIVE — prompts for confirmation before wiping volumes ────────
.PHONY: nuke
nuke:
	@echo ""
	@echo "WARNING: This will permanently delete ALL containers, networks, AND volumes."
	@echo "All data in MinIO, ClickHouse, PostgreSQL, etc. will be LOST."
	@echo ""
	@printf "Type 'yes' to confirm: "; \
	  read CONFIRM; \
	  if [ "$$CONFIRM" = "yes" ]; then \
	    $(MAKE) down; \
	    docker system prune -af --volumes; \
	    echo "All containers and volumes removed."; \
	  else \
	    echo "Aborted."; \
	  fi

# ── TTL: apply ClickHouse TTL policies to bronze/staging tables ───────────────
.PHONY: apply-ttl
apply-ttl:
	@echo 'Applying ClickHouse TTL policies...'
	@cat scripts/clickhouse_ttl.sql | \
	  docker exec -i clickhouse clickhouse-client \
	    --user default \
	    --password "$$(grep CLICKHOUSE_DEFAULT_PASSWORD $(ENV_FILE) | cut -d= -f2)" \
	    --multiquery \
	  && echo 'TTL policies applied.' \
	  || echo 'ERROR: Could not apply TTL. Is ClickHouse running? Try: make transform'

# ── Health: quick stack health check ─────────────────────────────────────────
.PHONY: health
health:
	@echo '── Container status ─────────────────────────────────────────'
	@docker ps --format 'table {{.Names}}\t{{.Status}}' | grep -v NAMES | sort
	@echo ''
	@echo '── Service endpoints ────────────────────────────────────────'
	@curl -sf http://localhost:8123/ping >/dev/null && echo 'ClickHouse  :8123  OK' || echo 'ClickHouse  :8123  DOWN'
	@curl -sf http://localhost:9000/minio/health/live >/dev/null && echo 'MinIO       :9000  OK' || echo 'MinIO       :9000  DOWN'
	@curl -sf http://localhost:8080/health >/dev/null && echo 'Airflow     :8080  OK' || echo 'Airflow     :8080  DOWN'
	@curl -sf http://localhost:9090/-/healthy >/dev/null && echo 'Prometheus  :9090  OK' || echo 'Prometheus  :9090  DOWN'
	@curl -sf http://localhost:3000/api/health >/dev/null && echo 'Grafana     :3000  OK' || echo 'Grafana     :3000  DOWN'
	@curl -sf http://localhost:5432 >/dev/null 2>&1 || echo 'PostgreSQL  :5432  OK (no HTTP)'

# ── Setup: one-command bootstrap of the full platform ─────────────────────────
.PHONY: setup
setup: all
	@echo ''
	@echo '── Running setup scripts ──────────────────────────────────'
	@echo 'Step 1/6: Vault secrets...'
	@-$(MAKE) setup-vault 2>/dev/null || echo '  [SKIPPED] Vault not configured'
	@echo 'Step 2/6: Keycloak SSO...'
	@-$(MAKE) setup-keycloak 2>/dev/null || echo '  [SKIPPED] Keycloak not configured'
	@echo 'Step 3/6: Seeding MySQL (small dataset)...'
	@-$(MAKE) seed-mysql-small 2>/dev/null || echo '  [SKIPPED] MySQL seed failed'
	@echo 'Step 4/6: Seeding SaaS PostgreSQL (small)...'
	@-$(MAKE) seed-saas-small 2>/dev/null || echo '  [SKIPPED] SaaS seed failed'
	@echo 'Step 5/6: Bronze layer init...'
	@-$(MAKE) bronze-init-small 2>/dev/null || echo '  [SKIPPED] Bronze init failed'
	@echo 'Step 6/6: Superset dashboards...'
	@-$(MAKE) setup-superset 2>/dev/null || echo '  [SKIPPED] Superset setup failed'
	@echo ''
	@echo '── Platform ready ────────────────────────────────────────'
	@$(MAKE) health

# ── Reset: clean teardown and re-bootstrap ─────────────────────────────────────
.PHONY: reset
reset:
	@echo 'WARNING: This will delete ALL data and restart the full stack.'
	@printf "Type 'yes' to confirm: "; \
	  read CONFIRM; \
	  if [ "$$CONFIRM" = "yes" ]; then \
	    echo 'Stopping all containers...'; \
	    $(MAKE) down; \
	    docker volume prune -f; \
	    echo 'Rebuilding from scratch...'; \
	    $(MAKE) setup; \
	  else \
	    echo 'Aborted.'; \
	  fi

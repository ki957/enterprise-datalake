COMPOSE = docker compose
DOCKER_DIR = infrastructure/docker
ENV_FILE = .env

.PHONY: base
base:
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
transform: base
	$(COMPOSE) -f $(DOCKER_DIR)/docker-compose.transform.yml --env-file $(ENV_FILE) up -d

.PHONY: serving
serving: base
	$(COMPOSE) -f $(DOCKER_DIR)/docker-compose.serving.yml --env-file $(ENV_FILE) up -d

.PHONY: governance
governance: base
	$(COMPOSE) -f $(DOCKER_DIR)/docker-compose.governance.yml --env-file $(ENV_FILE) up -d

.PHONY: observability
observability: base
	$(COMPOSE) -f $(DOCKER_DIR)/docker-compose.observability.yml --env-file $(ENV_FILE) up -d

.PHONY: visualization
visualization: base
	$(COMPOSE) -f $(DOCKER_DIR)/docker-compose.visualization.yml --env-file $(ENV_FILE) up -d

.PHONY: all
all: base security storage ingestion transform serving governance observability visualization
	@echo '✅ Full stack started. Watch RAM: htop'

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
	@echo '⚠ Containers and networks removed. Volumes preserved.'

.PHONY: nuke
nuke: down
	docker system prune -af --volumes
	@echo '💀 Everything removed including volumes. Data is gone.'

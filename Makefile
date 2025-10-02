compose_file ?= local.compose.yaml

DC_ARGS ?=

.PHONY: help
help: ## Show this help
	@echo "Usage: make <target> [DC_ARGS=\"<docker-compose args>\"]"
	@echo ""
	@echo "Hint:"
	@echo "  To get started run: make clean up logs"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help

.PHONY: logs
logs: ## Check logs
	@docker compose -f $(compose_file) logs -f $(DC_ARGS)

.PHONY: up
up: ## Spin up a docker compose
	@docker compose -f $(compose_file) up -d $(DC_ARGS)
	@echo "[TIP] You can run make logs to see logs"

.PHONY: down
down: ## Stop running instances of the compose
	@docker compose -f $(compose_file) down $(DC_ARGS)

.PHONY: clean
clean: ## Wipe all persisted data
	@docker compose -f $(compose_file) down -v --remove-orphans $(DC_ARGS)

.PHONY: java
java:
	@cd java-examples && mvn clean install

# .PHONY: java/create
# java/create: java ## Create
# 	@cd java-examples && java -cp ./target/java-examples-1.0-SNAPSHOT.jar CreateJSON
# # mvn compile exec:java -Dexec.mainClass="com.github.cm2027.lab3datalake.App"

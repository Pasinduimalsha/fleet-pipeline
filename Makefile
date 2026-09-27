.PHONY: init up down reset ps logs topics psql test

init: ## create .env from the template if missing
	@test -f .env || cp .env.example .env

up: init ## build and start the infrastructure stack
	docker compose up -d --build

down: ## stop containers, keep data
	docker compose down

reset: ## stop containers and delete all volumes and generated data
	docker compose down -v
	rm -rf data/lake/* data/landing/* data/checkpoints/* reports/*

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=100

topics: ## show Kafka topic layout
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe

psql: ## open a psql shell on the serving DB
	docker compose exec postgres psql -U fleet -d fleet

test: ## run the pure-python unit tests (no Docker required)
	pip install -q -r tests/requirements.txt
	pytest tests/ -v

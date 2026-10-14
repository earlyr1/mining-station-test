# Команды стенда и разработки (§6, §7). Порты и имя проекта берутся из .env, если он есть;
# переменные экспортируются в команды (docker compose, tools/e2e.py).
# Проверено на GNU Make 3.81 и Docker Compose v5.5.1.
-include .env
export

COMPOSE ?= docker compose
# make up поднимает и демо-генератор; `make up UP_PROFILES=` (и `make reset UP_PROFILES=`) — стенд без него.
UP_PROFILES ?= --profile demo
VQ_VIEWER_PORT ?= 8000
VQ_CONSOLE_PORT ?= 8080
VQ_GRAFANA_PORT ?= 3000
VQ_PROM_PORT ?= 9090
VQ_KAFKA_PORT ?= 19092
VQ_PG_PORT ?= 5432
# Сервисы приложения работают от пользователя хоста (user в docker-compose.yml): на Linux ./out принадлежит ему.
VQ_UID ?= $(shell id -u)
VQ_GID ?= $(shell id -g)

# Инструментам e2e на хосте — адреса опубликованных портов брокера и БД.
e2e e2e-chaos: VQ_KAFKA ?= 127.0.0.1:$(VQ_KAFKA_PORT)
e2e e2e-chaos: VQ_PG_DSN ?= postgresql://vq:vq@127.0.0.1:$(VQ_PG_PORT)/vq
# Вложенный `make up`/`make reset` из tools/e2e.py тоже не поднимет демо-генератор.
e2e e2e-chaos: UP_PROFILES :=

.DEFAULT_GOAL := help
.PHONY: help up down reset logs test test-integration e2e e2e-chaos verify bench lint fmt

help: ## список команд
	@grep -E '^[a-z0-9-]+:.*## ' $(firstword $(MAKEFILE_LIST)) | awk -F':.*## ' '{printf "  %-17s %s\n", $$1, $$2}'

# Отказ старта dispatcher (exit 2, например после смены site.toml) `up --wait` показывает только как
# «container … is unhealthy»: строку start_refused с причиной берём из его лога.
UP = $(COMPOSE) $(UP_PROFILES) up -d --build --wait --wait-timeout 180
UP_HINT = make up: стенд не поднялся, подробности — make logs. Смена site.toml или LOGIC_VERSION \
при сохранённом состоянии требует make reset

up: ## поднять стенд с демо-генератором, дождаться готовности и напечатать адреса
	@mkdir -p out
	@echo '$(UP)'
	@$(UP) || { $(COMPOSE) logs --no-log-prefix --tail=50 dispatcher 2>/dev/null | grep start_refused | tail -n 1; \
		echo '$(UP_HINT)' >&2; exit 1; }
	@echo "Витрина:    http://localhost:$(VQ_VIEWER_PORT)"
	@echo "Console:    http://localhost:$(VQ_CONSOLE_PORT)"
	@echo "Grafana:    http://localhost:$(VQ_GRAFANA_PORT)/d/vq"
	@echo "Prometheus: http://localhost:$(VQ_PROM_PORT)"

down: ## остановить стенд; тома Redpanda и Postgres сохраняются
	$(COMPOSE) --profile '*' down --remove-orphans

reset: ## удалить тома Redpanda и Postgres (только вместе) и поднять стенд заново
	$(COMPOSE) --profile '*' down -v --remove-orphans
	$(MAKE) up

logs: ## логи всех сервисов
	$(COMPOSE) --profile '*' logs -f --tail=200

test: ## тесты без брокера и БД
	uv run pytest -m "not integration"

test-integration: ## интеграционные тесты (testcontainers, нужен Docker)
	uv run pytest -m integration

# e2e начинается с `make reset` без демо-генератора: тот писал бы в telemetry.v1, и генератор e2e
# отказался бы стартовать на непустом топике (§4.4).
e2e: ## сквозной прогон: сброс стенда, генератор x20, час симуляции, затем verify
	@mkdir -p out
	$(MAKE) reset UP_PROFILES=
	uv run python tools/e2e.py

e2e-chaos: ## то же с двумя аварийными остановками dispatcher
	@mkdir -p out
	$(MAKE) reset UP_PROFILES=
	uv run python tools/e2e.py --chaos

# verify и bench — в образе приложения: от версии Python и libm зависит последний бит гаверсинуса (§2.7).
verify: ## сверка выходов с офлайн-прогоном ядра (в образе приложения, нужен поднятый стенд)
	$(COMPOSE) run --rm dispatcher vq-verify

# bench не нужны брокер и БД: --no-deps, стенд не поднимается.
bench: ## время ядра на сообщение на часе телеметрии замкнутого контура (в образе приложения)
	$(COMPOSE) run --rm --no-deps dispatcher vq-bench

lint: ## ruff check + ruff format --check + mypy --strict (src и tools)
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy src tools

fmt: ## ruff format + автоисправления ruff
	uv run ruff format .
	uv run ruff check --fix .

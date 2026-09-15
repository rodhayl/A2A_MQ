.PHONY: check test up down
check:
	python tools/check.py
	python -m pytest -m 'not live and not postgres'
test: check
up:
	docker compose -f deploy/compose.yaml up --build -d
down:
	docker compose -f deploy/compose.yaml down

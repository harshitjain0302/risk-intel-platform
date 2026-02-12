up:
	docker compose up -d

down:
	docker compose down -v

status:
	docker compose ps

logs:
	docker compose logs -f --tail=200

check:
	@echo "Airflow status code:"
	@curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080 || true
	@echo "MLflow status code:"
	@curl -s -o /dev/null -w "%{http_code}\n" http://localhost:5050 || true
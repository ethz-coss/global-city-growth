# Makefile

# Test the dbt connection to the database
dbt-debug:
	docker compose exec orchestrator bash -c "cd warehouse && dbt debug --profiles-dir ."
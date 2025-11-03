run-all:
	docker compose -f docker-compose.yaml up --build -d
down-rm:
	docker compose down -v
build-spark:
	docker build -t spark-nyc:latest ./spark

JOB ?=nyc_test.py
spark-submit:
	docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/$(JOB)

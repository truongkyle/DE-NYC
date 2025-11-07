run-all:
	docker compose -f docker-compose.yaml up --build -d
down-rm:
	docker compose down -v
build-spark:
	docker-compose up -d --build spark-master spark-worker-1 spark-worker-2

JOB ?=nyc_test.py
pool ?=bronze
spark-submit:
	docker exec -it spark-master /opt/spark/bin/spark-submit \
# 	--conf spark.scheduler.pool=${pool} \
# 	--conf spark.scheduler.pool=silver \
#     --conf spark.executor.instances=1 \
#     --conf spark.executor.cores=1 \
#     --conf spark.executor.memory=2g \
#     --conf spark.driver.memory=1g \
	/opt/spark-apps/$(JOB)
batch-spark-submit:
	docker exec -it spark-master /opt/spark/bin/spark-submit \
	/opt/spark-apps/$(JOB)
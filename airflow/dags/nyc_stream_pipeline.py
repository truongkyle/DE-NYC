from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {'owner': 'data_engineer', 'retries': 1}

with DAG(
    dag_id="nyc_stream_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 11, 1),
    schedule='@daily',
    catchup=False,
) as dag:

    bronze = BashOperator(
        task_id='spark_bronze',
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.scheduler.pool=bronze \
        --conf spark.executor.instances=1 \
        --conf spark.executor.cores=1 \
        --conf spark.executor.memory=2g \
        --conf spark.driver.memory=1g \
        /opt/spark-apps/streaming_bronze_nyc.py
        """
    )

    silver = BashOperator(
        task_id='spark_silver',
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.scheduler.pool=silver \
        --conf spark.executor.instances=1 \
        --conf spark.executor.cores=1 \
        --conf spark.executor.memory=2g \
        --conf spark.driver.memory=1g \
        /opt/spark-apps/streaming_silver_nyc.py
        """
    )

    gold = BashOperator(
        task_id='spark_gold',
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.scheduler.pool=gold \
        --conf spark.executor.instances=1 \
        --conf spark.executor.cores=1 \
        --conf spark.executor.memory=2g \
        --conf spark.driver.memory=1g \
        /opt/spark-apps/streaming_gold_nyc.py
        """
    )

    register = BashOperator(
        task_id='register_hive_tables',
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark-apps/streaming_register_tables.py
        """
    )

[bronze, silver, gold] >> register

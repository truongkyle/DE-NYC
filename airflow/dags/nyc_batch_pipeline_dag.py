from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,  
}

with DAG(
    dag_id='nyc_taxi_batch_pipeline',
    default_args=default_args,
    start_date=datetime(2025,11,1),
    schedule ='@daily',
    catchup=False,
    tags=['nyc', 'spark', 'batch']
) as dag:
    bronze = BashOperator(
        task_id='spark_bronze',
        bash_command='docker exec spark-master /opt/spark/bin/spark-submit '
                     '--master spark://spark-master:7077 /opt/spark-apps/bronze_nyc.py'
    )

    silver = BashOperator(
        task_id='spark_silver',
        bash_command='docker exec spark-master /opt/spark/bin/spark-submit '
                     '--master spark://spark-master:7077 /opt/spark-apps/silver_trips.py'
    )

    gold = BashOperator(
        task_id='spark_gold',
        bash_command='docker exec spark-master /opt/spark/bin/spark-submit '
                     '--master spark://spark-master:7077 /opt/spark-apps/gold_kpi.py'
    )

    register = BashOperator(
        task_id='register_hive_tables',
        bash_command='docker exec spark-master /opt/spark/bin/spark-submit '
                     '--master spark://spark-master:7077 /opt/spark-apps/register_tables.py'
    )

bronze >> silver >> gold >> register
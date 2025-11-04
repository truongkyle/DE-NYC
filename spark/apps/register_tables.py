from pyspark.sql import SparkSession
spark = (SparkSession.builder
         .appName("Register-Tables")
         .master("spark://spark-master:7077")
         .config("hive.metastore.uris", "thrift://hive-metastore:9083")
         .enableHiveSupport()
         .getOrCreate())

spark.sql("CREATE DATABASE IF NOT EXISTS nyc_gold")
spark.sql("""
  CREATE TABLE IF NOT EXISTS nyc_gold.daily_revenue_by_zone
  USING DELTA
  LOCATION 's3a://datalake/gold/nyc_taxi/daily_revenue_by_zone'
""")
spark.sql("""
  CREATE TABLE IF NOT EXISTS nyc_gold.hourly_demand_by_zone
  USING DELTA
  LOCATION 's3a://datalake/gold/nyc_taxi/hourly_demand_by_zone'
""")
spark.stop()

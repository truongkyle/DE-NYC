from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, to_timestamp, hour, date_format

spark = (SparkSession.builder
         .appName("check-database")
         .master("spark://spark-master:7077")
         .getOrCreate())
spark.sql("SHOW DATABASES").show(truncate=False)
print("--"*20)
spark.sql("SHOW TABLES IN nyc_gold").show(truncate=False)
print("--"*20)
spark.sql("DESCRIBE EXTENDED nyc_gold.daily_revenue_by_zone").show(truncate=False)
print("==========="*20)
spark.stop
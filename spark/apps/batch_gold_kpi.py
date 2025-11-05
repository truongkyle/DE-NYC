from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, date_format, hour

spark = (SparkSession.builder
         .appName("NYC-Gold-KPI")
         .master("spark://spark-master:7077")
         .getOrCreate())

silver = "s3a://datalake/silver/nyc_taxi"
gold   = "s3a://datalake/gold/nyc_taxi"

trips = spark.read.format("delta").load(f"{silver}/trips")

# 1) daily_revenue_by_zone
daily_zone = (trips
    .groupBy("pickup_date","pu_location_id","service_type")
    .agg(
        count("*").alias("trips"),
        _sum("total_amount").alias("revenue"),
        avg("trip_distance").alias("avg_miles"),
        avg("duration_min").alias("avg_duration_min")
    ))
(daily_zone.write.format("delta").mode("overwrite")
 .partitionBy("pickup_date","service_type")
 .save(f"{gold}/daily_revenue_by_zone"))

# 2) hourly_demand_by_zone

hourly = (trips
    .groupBy("pickup_date","pickup_hour","pu_location_id","service_type")
    .agg(count("*").alias("trips")))
(hourly.write.format("delta").mode("overwrite")
 .partitionBy("pickup_date","service_type")
 .save(f"{gold}/hourly_demand_by_zone"))

spark.stop()

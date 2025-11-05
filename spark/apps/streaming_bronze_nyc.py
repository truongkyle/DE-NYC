from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, year, month
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, DoubleType, StringType

spark = (SparkSession.builder
         .appName("NYC-Streaming-Bronze")
         .master("spark://spark-master:7077")
         .config("spark.sql.shuffle.partitions","8")
         .getOrCreate()

)

schema = StructType([
    StructField("ride_id", IntegerType()),
    StructField("pickup_ts", TimestampType()),
    StructField("dropoff_ts", TimestampType()),
    StructField("fare", DoubleType()),
    StructField("tip", DoubleType()),
    StructField("total", DoubleType())
])

payload_schema = StructType([
    StructField("op", StringType()), # c,u,d,r
    StructField("ts_ms", StringType()),
    StructField("after", schema),
    StructField("before", StructType([ #for delete/update
        StructField("ride_id", IntegerType())
    ]))
])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers","broker:29092")
       .option("subscribe","nyc.public.rides")
       .option("startingOffsets","earliest")
       .load())

v = raw.selectExpr("CAST(value AS STRING) as json") \
       .select(from_json(col("json"), payload_schema).alias("p")) \
       .select("p.*")

bronze = (v
  .withColumn("ride_id",    col("after.ride_id"))
  .withColumn("pickup_ts",  col("after.pickup_ts"))
  .withColumn("dropoff_ts", col("after.dropoff_ts"))
  .withColumn("fare",       col("after.fare"))
  .withColumn("tip",        col("after.tip"))
  .withColumn("total",      col("after.total"))
  .withColumn("op_type",    col("op"))
  .withColumn("year",       year(col("pickup_ts")))
  .withColumn("month",      month(col("pickup_ts"))))

(bronze.writeStream
 .format("delta")
 .outputMode("append")
 .option("checkpointLocation","s3a://datalake/checkpoints/bronze_rides")
 .partitionBy("year", "month")
 .start("s3a://datalake/bronze/nyc_taxi"))

spark.streams.awaitAnyTermination()
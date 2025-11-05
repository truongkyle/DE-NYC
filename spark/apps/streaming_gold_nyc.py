# streaming_gold_kpi.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, sum as _sum

spark = (SparkSession.builder
  .appName("NYC-Streaming-Gold")
  .master("spark://spark-master:7077")
  .config("spark.sql.shuffle.partitions","8")
  .getOrCreate())

silver = spark.readStream.format("delta").load("s3a://datalake/silver/nyc_taxi/rides")

# chỉ lấy record chưa bị xóa
silver_clean = silver.where(col("is_deleted") == False)

# Aggregation theo cửa sổ 1h (có watermark trễ tối đa 2h)
agg = (silver_clean
  .withWatermark("pickup_ts","2 hours")
  .groupBy(window(col("pickup_ts"), "1 hour").alias("w"))
  .agg(_sum("total").alias("revenue"), _sum((col("total")>0).cast("int")).alias("rides"))
  .selectExpr(
      "date_format(w.start,'yyyy-MM-dd') as pickup_date",
      "hour(w.start) as pickup_hour",
      "revenue","rides"))

(agg.writeStream
  .format("delta")
  .outputMode("append")    # update để cập nhật cửa sổ đang mở
  .option("checkpointLocation","s3a://datalake/checkpoints/gold_hourly")
  .start("s3a://datalake/gold/nyc_taxi/hourly_kpi"))

spark.streams.awaitAnyTermination()

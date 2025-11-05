# streaming_silver_merge.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from delta.tables import DeltaTable

SILVER_PATH = "s3a://datalake/silver/nyc_taxi/rides"

spark = (SparkSession.builder
  .appName("NYC-Streaming-Silver")
  .master("spark://spark-master:7077")
  .config("spark.sql.shuffle.partitions","8")
  .config("spark.databricks.delta.schema.autoMerge.enabled","true")
  .getOrCreate())

bronze_stream = spark.readStream.format("delta").load("s3a://datalake/bronze/nyc_taxi")

def upsert_to_silver(microbatch_df, batch_id):
    # Chuẩn hóa microbatch (lọc cột cần, flag delete)
    cleaned = (microbatch_df
      .select("ride_id","pickup_ts","dropoff_ts","fare","tip","total","op_type")
      .withColumn("is_deleted", when(col("op_type")=="d", True).otherwise(False)))

    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        cleaned.filter(~col("is_deleted")) \
               .write.format("delta").mode("overwrite").save(SILVER_PATH)

    silver = DeltaTable.forPath(spark, SILVER_PATH)

    # MERGE logic: upsert & delete
    (silver.alias("t")
      .merge(cleaned.alias("s"), "t.ride_id = s.ride_id")
      .whenMatchedUpdate(
          condition = "s.is_deleted = false",
          set = {
              "pickup_ts":"s.pickup_ts",
              "dropoff_ts":"s.dropoff_ts",
              "fare":"s.fare",
              "tip":"s.tip",
              "total":"s.total",
              "is_deleted":"false"
          })
      .whenMatchedDelete(condition="s.is_deleted = true")
      .whenNotMatchedInsert(values={
          "ride_id":"s.ride_id",
          "pickup_ts":"s.pickup_ts",
          "dropoff_ts":"s.dropoff_ts",
          "fare":"s.fare",
          "tip":"s.tip",
          "total":"s.total",
          "is_deleted":"s.is_deleted"
      })
      .execute())

query = (bronze_stream.writeStream
  .foreachBatch(upsert_to_silver)
  .option("checkpointLocation","s3a://datalake/checkpoints/silver_upsert")
  .outputMode("update")  # (không ghi trực tiếp, foreachBatch xử lý)
  .start())

spark.streams.awaitAnyTermination()

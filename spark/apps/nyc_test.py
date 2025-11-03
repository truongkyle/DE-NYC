# from pyspark.sql import SparkSession

# spark = (
#     SparkSession.builder
#     .appName("NYC-Test-Delta")
#     .master("spark://spark-master:7077")
#     # Delta Lake
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
#     # MinIO (S3A)
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
#     .config("spark.hadoop.fs.s3a.access.key", "minio_access_key")
#     .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key")
#     .config("spark.hadoop.fs.s3a.path.style.access", "true")
#     .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
#     .config("spark.eventLog.enabled", "false")
#     .getOrCreate()
# )

# src_csv = "s3a://datalake/warehouse/data.csv"
# dst_parquet = "s3a://datalake/warehouse/parquet_file/orders"

# try:
#     df = spark.read.option("header", True).csv(src_csv)
#     df2 = df.dropna(subset=["CustomerID"]).dropDuplicates(["CustomerID"])
#     df2.write.mode("overwrite").parquet(dst_parquet)
# except Exception as e:
#     raise Exception
# finally:
#     spark.stop
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("NYC-Test-Delta")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

df = spark.createDataFrame(
    [(1, "Alice", 30), (2, "Bob", 28)],
    ["id", "name", "age"]
)

df.write.format("delta").mode("overwrite").save("s3a://datalake/test_delta2")

print(" Write Delta thành công!")
spark.stop()


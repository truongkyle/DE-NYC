from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Delta-Fix")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

src_csv = "s3a://datalake/warehouse/data.csv"
dst_parquet = "s3a://datalake/warehouse/parquet_file/orders"

df = spark.read.option("header", True).option("inferSchema", True).csv(src_csv)

df2 = df.dropna(subset=["CustomerID"]).dropDuplicates(["CustomerID"])
df2.write.format("delta").mode("overwrite").save(dst_parquet)
spark.stop

from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, lit, col

spark = (SparkSession.builder
         .appName("NYC-Bronze")
         .master("spark://spark-master:7077")
         .getOrCreate())

base_raw = "s3a://datalake/raw/nyc_taxi"
base_bronze = "s3a://datalake/bronze/nyc_taxi"

def load_write(dataset, ts_col_pickup, ts_col_frop):
    df = spark.read.parquet(f"{base_raw}/{dataset}/*")

    df = df.withColumn("_srcfile", input_file_name()) \
           .withColumn("year", regexp_extract(col("_srcfile"), r"_(\d{4})-\d{2}\.parquet$", 1)) \
           .withColumn("month", regexp_extract(col("_srcfile"), r"_\d{4}-(\d{2})\.parquet$", 1)) \
           .drop("_srcfile")
    (df.write
        .format("delta")
        .mode("append")
        .partitionBy("year", "month")
        .save(f"{base_bronze}/{dataset}"))
load_write("yellow", "tpep_pickup_datetime", "tpep_dropoff_datetime")
load_write("green",  "lpep_pickup_datetime", "lpep_dropoff_datetime")
load_write("fhvhv",  "pickup_datetime",      "dropoff_datetime")
load_write("fhv",    "pickup_datetime",      "dropOff_datetime")

spark.stop()
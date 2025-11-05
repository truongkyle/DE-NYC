from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("NYC-Bronze")
         .master("spark://spark-master:7077")
         .getOrCreate())

base_raw = "s3a://datalake/raw/nyc_taxi"
base_bronze = "s3a://datalake/bronze/nyc_taxi"

def load_write(dataset, ts_col_pickup, ts_col_frop):
    df = spark.read.parquet(f"{base_raw}/{dataset}/*")

    # df = df.withColumn("_srcfile", input_file_name()) \
    #        .withColumn("year", regexp_extract(col("_srcfile"), r"_(\d{4})-\d{2}\.parquet$", 1)) \
    #        .withColumn("month", regexp_extract(col("_srcfile"), r"_\d{4}-(\d{2})\.parquet$", 1)) \
    #        .drop("_srcfile")
    df = df.withColumn("_src_file", F.input_file_name()) \
            .withColumn("_file_name", F.split(F.col("_src_file"), "/").getItem(-1)) \
            .withColumn("_year_and_month", F.split(F.col("_file_name"), "_").getItem(-1)) \
            .withColumn("year", F.split(F.col("_year_and_month"), "-").getItem(0))\
            .withColumn("month", F.split(F.split(F.col("_year_and_month"), "-").getItem(-1), ".").getItem(0)) \
            .drop("_src_file", "_file_name", "_year_and_month")
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
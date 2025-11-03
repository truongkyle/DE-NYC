from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, to_timestamp, hour, date_format

spark = (SparkSession.builder
         .appName("NYC-Silver-Trips")
         .master("spark://spark-master:7077")
         .getOrCreate())

bronze = "s3a://datalake/bronze/nyc_taxi"
silver = "s3a://datalake/silver/nyc_taxi"

def base_trips(df, pickup, dropoff, pu, do, dist, fare, tip, total, service):
    return (df
        .withColumn("pickup_datetime",  col(pickup).cast("timestamp"))
        .withColumn("dropoff_datetime", col(dropoff).cast("timestamp"))
        .withColumn("pu_location_id",   col(pu).cast("int"))
        .withColumn("do_location_id",   col(do).cast("int"))
        .withColumn("trip_distance",    col(dist).cast("double"))
        .withColumn("fare_amount",      when(col(fare).isNotNull(), col(fare)).otherwise(lit(0.0)).cast("double"))
        .withColumn("tip_amount",       when(col(tip).isNotNull(), col(tip)).otherwise(lit(0.0)).cast("double"))
        .withColumn("total_amount",     when(col(total).isNotNull(), col(total)).otherwise(lit(0.0)).cast("double"))
        .withColumn("service_type",     lit(service))
        .select("pickup_datetime","dropoff_datetime","pu_location_id","do_location_id",
                "trip_distance","fare_amount","tip_amount","total_amount","service_type"))

# yellow
y = spark.read.format("delta").load(f"{bronze}/yellow")
y = base_trips(y, "tpep_pickup_datetime","tpep_dropoff_datetime","PULocationID","DOLocationID",
               "trip_distance","fare_amount","tip_amount","total_amount","yellow")

# green
g = spark.read.format("delta").load(f"{bronze}/green")
g = base_trips(g, "lpep_pickup_datetime","lpep_dropoff_datetime","PULocationID","DOLocationID",
               "trip_distance","fare_amount","tip_amount","total_amount","green")

# fhvhv (đủ giàu để hợp nhất)
h = spark.read.format("delta").load(f"{bronze}/fhvhv")
h = (h
     .withColumn("trip_distance", col("trip_miles").cast("double"))
     .withColumn("fare_amount",   col("base_passenger_fare").cast("double"))
     .withColumn("tip_amount",    col("tips").cast("double"))
     .withColumn("total_amount",  (col("base_passenger_fare")+col("tips")+col("tolls")+col("sales_tax")+col("congestion_surcharge")).cast("double")))
h = base_trips(h, "pickup_datetime","dropoff_datetime","PULocationID","DOLocationID",
               "trip_distance","fare_amount","tip_amount","total_amount","fhvhv")

trips = y.unionByName(g, allowMissingColumns=True).unionByName(h, allowMissingColumns=True)

# làm sạch cơ bản
trips = (trips
    .where("pickup_datetime is not null and dropoff_datetime is not null")
    .where("trip_distance >= 0 and trip_distance <= 200")
    .where("dropoff_datetime >= pickup_datetime")
    .withColumn("duration_min", (col("dropoff_datetime").cast("long")-col("pickup_datetime").cast("long"))/60.0)
    .where("duration_min >= 0 and duration_min <= 600")  # <= 10 giờ
    .withColumn("pickup_date",  date_format(col("pickup_datetime"), "yyyy-MM-dd"))
    .withColumn("pickup_hour",  hour(col("pickup_datetime"))))

(trips.write
   .format("delta")
   .mode("overwrite")
   .partitionBy("pickup_date","service_type")
   .save(f"{silver}/trips"))

spark.stop()

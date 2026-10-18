import time
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.master("local[*]").appName("prepare")
         .config("spark.driver.memory", "6g")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.showConsoleProgress", "false")
         .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("data/raw/yellow_tripdata_2024-*.parquet")
df = (df.withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp"))
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp"))
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .where("pickup_date BETWEEN DATE'2024-01-01' AND DATE'2024-06-30'"))
df.printSchema()

t = time.time()
df.write.mode("overwrite").option("header", True).csv("data/csv")
print(f"csv: {time.time() - t:.1f} s")

t = time.time()
df.repartition("pickup_date").write.mode("overwrite").partitionBy("pickup_date").parquet("data/parquet")
print(f"parquet: {time.time() - t:.1f} s")

print("rows:", spark.read.parquet("data/parquet").count())

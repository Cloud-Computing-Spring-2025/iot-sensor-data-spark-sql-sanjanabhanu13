from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, hour, avg

# Start Spark session
spark = SparkSession.builder \
    .appName("IoT Sensor Task 5") \
    .getOrCreate()

# Load data
df = spark.read.option("header", True).option("inferSchema", True).csv("sensor_data.csv")

# Convert timestamp and extract hour
df = df.withColumn("timestamp", to_timestamp("timestamp")) \
       .withColumn("hour_of_day", hour("timestamp"))

# Pivot: average temperature by location vs hour
pivot_df = df.groupBy("location") \
    .pivot("hour_of_day", list(range(24))) \
    .agg(avg("temperature"))

# Show results
pivot_df.show(truncate=False)

# Save to CSV
pivot_df.write.mode("overwrite").option("header", True).csv("task5_output.csv")

# Stop session
spark.stop()

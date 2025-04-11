from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, hour, avg

# Start Spark session
spark = SparkSession.builder \
    .appName("IoT Sensor Task 3") \
    .getOrCreate()

# Load the CSV
df = spark.read.option("header", True).option("inferSchema", True).csv("sensor_data.csv")

# Convert string to timestamp and extract hour
df = df.withColumn("timestamp", to_timestamp("timestamp"))
df = df.withColumn("hour_of_day", hour("timestamp"))

# Group by hour and get average temperature
avg_temp_by_hour = df.groupBy("hour_of_day") \
    .agg(avg("temperature").alias("avg_temp")) \
    .orderBy("avg_temp", ascending=False)

# Show results
avg_temp_by_hour.show(24)

# Save to CSV
avg_temp_by_hour.write.mode("overwrite").option("header", True).csv("task3_output.csv")

# Stop Spark session
spark.stop()

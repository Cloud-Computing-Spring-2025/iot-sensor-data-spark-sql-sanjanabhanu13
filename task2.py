from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Start Spark session
spark = SparkSession.builder \
    .appName("IoT Sensor Task 2") \
    .getOrCreate()

# Load the CSV
df = spark.read.option("header", True).option("inferSchema", True).csv("sensor_data.csv")

# Filter rows by temperature range
in_range = df.filter((df.temperature >= 18) & (df.temperature <= 30))
out_of_range = df.filter((df.temperature < 18) | (df.temperature > 30))

print("✅ In-Range Records Count:", in_range.count())
print("✅ Out-of-Range Records Count:", out_of_range.count())

# Group by location to compute average temperature and humidity
agg_df = in_range.groupBy("location") \
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    ) \
    .orderBy("avg_temperature", ascending=False)

agg_df.show()

# Save to CSV
agg_df.write.mode("overwrite").option("header", True).csv("task2_output.csv")

# Stop Spark session
spark.stop()

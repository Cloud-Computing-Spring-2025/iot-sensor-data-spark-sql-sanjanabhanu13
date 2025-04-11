from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

# Start Spark session
spark = SparkSession.builder \
    .appName("IoT Sensor Task 4") \
    .getOrCreate()

# Load the CSV
df = spark.read.option("header", True).option("inferSchema", True).csv("sensor_data.csv")

# Calculate avg temperature per sensor
avg_temp_df = df.groupBy("sensor_id") \
    .agg(avg("temperature").alias("avg_temp"))

# Define ranking window
window_spec = Window.orderBy(avg_temp_df["avg_temp"].desc())

# Add ranking column
ranked_df = avg_temp_df.withColumn("rank_temp", dense_rank().over(window_spec))

# Show top 5
ranked_df.orderBy("rank_temp").show(5)

# Save all ranked sensors
ranked_df.write.mode("overwrite").option("header", True).csv("task4_output.csv")

# Stop session
spark.stop()

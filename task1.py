from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder \
    .appName("IoT Sensor Task 1") \
    .getOrCreate()

# Load sensor_data.csv
df = spark.read.option("header", True).option("inferSchema", True).csv("sensor_data.csv")

# Create temporary view
df.createOrReplaceTempView("sensor_readings")

# Show first 5 rows
print("=== First 5 Rows ===")
df.show(5)

# Count total records
print("=== Total Number of Records ===")
print(df.count())

# Distinct locations
print("=== Distinct Locations ===")
spark.sql("SELECT DISTINCT location FROM sensor_readings").show()

# Save full DataFrame (optional)
df.write.mode("overwrite").option("header", True).csv("task1_output.csv")

# Stop Spark session
spark.stop()

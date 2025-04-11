import csv
import random
from faker import Faker

fake = Faker()

LOCATIONS = ["BuildingA_Floor1", "BuildingA_Floor2", "BuildingB_Floor1", "BuildingB_Floor2"]
SENSOR_TYPES = ["TypeA", "TypeB", "TypeC"]

def generate_sensor_data(num_records=1000, output_file="sensor_data.csv"):
    """
    Generates a CSV with fields:
    sensor_id, timestamp, temperature, humidity, location, sensor_type
    """
    fieldnames = ["sensor_id", "timestamp", "temperature", "humidity", "location", "sensor_type"]
    
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for _ in range(num_records):
            writer.writerow({
                "sensor_id": random.randint(1000, 1100),
                "timestamp": fake.date_time_between(start_date="-3d", end_date="now").strftime("%Y-%m-%d %H:%M:%S"),
                "temperature": round(random.uniform(15.0, 35.0), 1),
                "humidity": round(random.uniform(50.0, 80.0), 1),
                "location": random.choice(LOCATIONS),
                "sensor_type": random.choice(SENSOR_TYPES)
            })

# Run the generator
if __name__ == "__main__":
    generate_sensor_data()
    print("✅ sensor_data.csv generated successfully.")

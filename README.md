# iot-sensor-data-spark-sql
# IoT Sensor Data Analysis Using PySpark
This project focuses on analyzing IoT sensor data using PySpark. The dataset contains simulated readings collected from sensors across different building floors. It includes temperature, humidity, timestamp, and other metadata. The analysis is broken down into five tasks, each highlighting key features of Spark SQL.

**Files Included**
sensor_data.csv – Fake sensor readings

data_generator.py – Code to generate the dataset

task1.py to task5.py – Spark SQL analysis tasks

taskX_output.csv – Output folders for each task



**Task 1: Load & Explore the Data**
Loaded sensor_data.csv into Spark and inferred schema.
Created a temporary view sensor_readings.

**Ran basic queries:**
Displayed the first 5 rows
Counted total number of records
Retrieved distinct sensor locations
Output saved to task1_output.csv
**OUTPUT**
+----------+---------------------+------------+----------+-------------------+------------+
| sensor_id| timestamp           | temperature| humidity | location          | sensor_type|
+----------+---------------------+------------+----------+-------------------+------------+
|     1001 | 2025-04-11T11:12:06 |       28.7 |     57.3 | BuildingA_Floor1  | TypeC      |
|     1064 | 2025-04-10T09:51:48 |       34.5 |     74.6 | BuildingA_Floor1  | TypeB      |
|     1096 | 2025-04-09T17:21:51 |       21.8 |     57.7 | BuildingB_Floor1  | TypeA      |
|     1032 | 2025-04-11T06:44:18 |       24.3 |     53.4 | BuildingA_Floor1  | TypeB      |
|     1055 | 2025-04-11T10:30:28 |       15.0 |     65.3 | BuildingA_Floor1  | TypeA      |
+----------+---------------------+------------+----------+-------------------+------------+

**Task 2: Filter & Aggregate**
Filtered out readings with temperature below 18°C or above 30°C

Counted in-range vs out-of-range records

Grouped by location to compute:

Average temperature

Average humidity

Sorted locations by average temperature

Output saved to task2_output.csv

**OUTPUT**
+-------------------+------------------+------------------+
| location          | avg_temperature  | avg_humidity     |
+-------------------+------------------+------------------+
| BuildingB_Floor2  | 24.12            | 64.52            |
| BuildingA_Floor1  | 24.01            | 64.33            |
| BuildingB_Floor1  | 23.93            | 64.91            |
| BuildingA_Floor2  | 23.91            | 66.53            |
+-------------------+------------------+------------------+


**Task 3: Time-Based Analysis**
Converted the timestamp column to a datetime format
Extracted the hour of the day from the timestamp
Calculated average temperature for each hour
Found that 9 PM (21:00) was the warmest time overall
Output saved to task3_output.csv
**OUTPUT**
+-------------+-----------+
| hour_of_day | avg_temp  |
+-------------+-----------+
|     21      | 26.69     |
|     19      | 26.66     |
|     18      | 26.16     |
|      9      | 25.87     |
|     17      | 25.67     |
|      6      | 25.55     |
|      3      | 25.53     |
|     14      | 25.32     |
|      7      | 25.29     |
|     20      | 25.28     |     
+-------------+-----------+

**Task 4: Sensor Ranking Using Window Function**
Computed average temperature for each sensor
Used dense_rank() to rank sensors from hottest to coolest
Displayed the top 5 hottest sensors
 Output saved to task4_output.csv
 **OUTPUT**

+----------+-----------+-----------+
| sensor_id| avg_temp  | rank_temp |
+----------+-----------+-----------+
|     1039 |     29.70 |     1     |
|     1060 |     28.75 |     2     |
|     1097 |     28.68 |     3     |
|     1040 |     28.55 |     4     |
|     1084 |     28.53 |     5     |
+----------+-----------+-----------+




Task 5: Pivot Table by Hour and Location
Created a pivot table:
Rows: sensor locations
Columns: hours of the day (0–23)
Values: average temperature
Analyzed which floors are hottest at what times
For example, BuildingB_Floor1 was hottest around 9 PM
Output saved to task5_output.csv
**output**

|location|   0    |   1    |   2    |   3    |   4    |   5    |   6    |   7    |   8    |   9    |  10    |  11    |  12    |  13    |  14    |  15    |  16    |  17    |  18    |  19    |  20    |  21    |  22    |  23    |

| BuildingA_Floor1  | 26.24  | 26.31  | 24.83  | 25.21  | 27.92  | 23.92  | 24.47  | 26.38  | 25.81  | 27.50  | 26.00  | 27.91  | 28.42  | 24.71  | 26.01  | 26.75  | 25.39  | 25.00  | 27.83  | 28.57  | 29.17  | 27.27  | 26.85  | 25.81  |


| BuildingB_Floor2  | 24.76  | 22.57  | 26.14  | 26.04  | 24.94  | 24.23  | 27.15  | 26.43  | 25.37  | 27.08  | 27.85  | 25.04  | 23.16  | 26.09  | 26.18  | 25.09  | 24.67  | 25.24  | 24.63  | 23.49  | 24.64  | 27.21  | 22.90  | 22.23  |

| BuildingA_Floor2  | 23.14  | 22.17  | 25.12  | 24.76  | 22.97  | 20.41  | 27.47  | 24.62  | 24.21  | 26.70  | 24.41  | 25.05  | 23.33  | 24.71  | 22.82  | 23.53  | 22.52  | 27.56  | 26.83  | 24.76  | 25.40  | 26.92  | 22.40  | 26.85  |

| BuildingB_Floor1  | 25.74  | 24.90  | 22.43  | 26.80  | 22.13  | 25.40  | 22.17  | 24.12  | 26.29  | 23.79  | 20.11  | 26.16  | 24.55  | 24.95  | 26.23  | 26.65  | 27.63  | 26.65  | 27.11  | 30.24  | 25.36  | 25.78  | 22.75  | 24.87  |




**Run Each Task**
python task1.py
python task2.py
python task3.py
python task4.py
python task5.py

**To push everything to GitHub:**
git add .
git commit -m "Completed all tasks for IoT Sensor Data Analysis"
git push origin master
















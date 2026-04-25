# Weather Data Analysis using Hadoop - Big Data Project

## Project Title
**Weather Data Analysis using Hadoop MapReduce**

---

## Problem Statement
Weather data is generated continuously across multiple cities in massive volumes. Analyzing this data manually is time-consuming and inefficient. This project uses Hadoop's MapReduce framework to process large-scale weather datasets and extract meaningful insights such as:
- Average temperature per city
- Total rainfall per city
- Average humidity per city
- Frequency of weather conditions (Clear, Rainy, Cloudy)

---

## Dataset Description
- **File:** `dataset.csv`
- **Format:** CSV (Comma Separated Values)
- **Size:** ~180 records
- **Cities Covered:** Delhi, Mumbai, Bangalore, Chennai, Kolkata
- **Time Period:** January 2024 – December 2024

### Columns:
| Column       | Description                          |
|--------------|--------------------------------------|
| date         | Date of observation (YYYY-MM-DD)     |
| city         | Name of the city                     |
| temperature  | Temperature in Celsius               |
| humidity     | Humidity percentage                  |
| rainfall     | Rainfall in millimeters              |
| wind_speed   | Wind speed in km/h                   |
| condition    | Weather condition (Clear/Rainy/Cloudy)|

---

## Architecture
```
Raw CSV Data → HDFS → MapReduce (Mapper + Reducer) → Hive → Insights
```

---

## Steps to Run

### Prerequisites
- Hadoop installed and configured (HDFS + YARN)
- Python 3.x installed
- Cloudera / HDP environment (optional but recommended)

### Step 1: Start Hadoop Services
```bash
start-dfs.sh
start-yarn.sh
```

### Step 2: Upload Dataset to HDFS
```bash
hdfs dfs -mkdir -p /user/hadoop/weather/input
hdfs dfs -put dataset.csv /user/hadoop/weather/input/
```

### Step 3: Run MapReduce Job
```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -input /user/hadoop/weather/input/dataset.csv \
  -output /user/hadoop/weather/output \
  -mapper Mapper.py \
  -reducer Reducer.py \
  -file Mapper.py \
  -file Reducer.py
```

### Step 4: View Output
```bash
hdfs dfs -cat /user/hadoop/weather/output/part-00000
```

### Step 5: Load into Hive (Optional)
```sql
CREATE TABLE weather_data (
  date STRING,
  city STRING,
  temperature FLOAT,
  humidity FLOAT,
  rainfall FLOAT,
  wind_speed FLOAT,
  condition STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/hadoop/weather/input/dataset.csv' INTO TABLE weather_data;

-- Query: Average temperature per city
SELECT city, AVG(temperature) AS avg_temp FROM weather_data GROUP BY city;

-- Query: Total rainfall per city
SELECT city, SUM(rainfall) AS total_rain FROM weather_data GROUP BY city;
```

---

## Sample Output
```
Bangalore    Average Temperature    25.43 C
Bangalore    Average Humidity       70.12 %
Bangalore    Total Rainfall         236.80 mm
Chennai      Average Temperature    32.76 C
Chennai      Average Humidity       84.30 %
Chennai      Total Rainfall         354.20 mm
Delhi        Average Temperature    26.58 C
Delhi        Average Humidity       64.85 %
Delhi        Total Rainfall         184.40 mm
Mumbai       Average Temperature    30.92 C
Mumbai       Average Humidity       83.60 %
Mumbai       Total Rainfall         412.50 mm
Kolkata      Average Temperature    26.14 C
Kolkata      Average Humidity       75.20 %
Kolkata      Total Rainfall         302.10 mm
```

---

## Technologies Used
- **Hadoop** – HDFS for storage, MapReduce for processing
- **Hive** – SQL-like querying on Hadoop
- **Python 3** – Mapper and Reducer scripts
- **Cloudera** – Hadoop distribution platform
- **Linux** – Operating system environment

---

## Repository Structure
```
├── Project-Report.pdf
├── Mapper.py
├── Reducer.py
├── dataset.csv
└── README.md
```

---


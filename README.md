
# TP06 - Real-Time Data Processing Using Apache Spark

## 1. Project Description

This project implements a real-time data processing pipeline
using Apache Kafka and Apache Spark Structured Streaming.

The scenario simulates student check-in events in a smart campus.

## 2. Technologies Used

- Python
- Apache Kafka
- Apache Spark 3.5.3
- PySpark 3.5.3
- Docker
- Apache Flink SQL

## 3. Project Architecture

Python Producer
       |
       v
Apache Kafka
       |
       v
Spark Structured Streaming
       |
       v
Real-Time Analytics and Late Check-in Detection

## 4. Project Files

- producer.py: Generates synthetic campus events.
- spark_stream.py: Processes Kafka events using Spark.
- requirements.txt: Python dependencies.
- flink_window.sql: Flink SQL window query.
- screenshots/: Execution evidence.

## 5. How to Run

### Step 1: Activate environment

source .venv/bin/activate

### Step 2: Install dependencies

python3 -m pip install -r requirements.txt

### Step 3: Start Spark Streaming

spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
spark_stream.py

### Step 4: Run Producer in another terminal

python3 producer.py

## 6. Processing Features

- Kafka event ingestion
- JSON parsing
- Check-in filtering
- Department aggregation
- Event-time windows
- Watermarking
- Checkpointing
- Late check-in detection

## 7. Conclusion

The project demonstrates how Kafka and Spark Structured
Streaming can be integrated to process real-time events
and generate useful smart-campus attendance analytics.

# Real-Time-Server-Monitoring-Pipeline
# 🧠 Team 103 – Real-Time Server Monitoring Pipeline 

## 📘 Overview
This project implements a **real-time server performance monitoring pipeline** using the **Apache Kafka–Spark ecosystem**.  
It simulates a distributed setup where multiple students (producers, brokers, and consumers) work across different laptops to form a complete streaming data pipeline.

The pipeline continuously ingests server metrics — **CPU**, **Memory**, **Network**, and **Disk** — and performs analytics and alert generation using **Apache Spark**.

---

## ⚙️ System Architecture

### **🔹 Components**
| **Component** | **Technology Used** | **Role** |
|----------------|---------------------|-----------|
| **Producer** | Python (Kafka Producer API) | Reads data from CSV and streams records to Kafka topics. |
| **Kafka Broker** | Apache Kafka + Zookeeper | Acts as a message broker between producers and consumers. |
| **Consumer 1** | PySpark Batch Job | Processes CPU and Memory utilization data, computes averages, and generates alerts. |
| **Consumer 2** | PySpark Batch Job | Processes Network and Disk usage data, computes maximums, and generates alerts. |

---

## 🏗️ Architecture Diagram (Text Description)
     ┌────────────────────┐
     │      Producer      │
     │ (CSV → Kafka Topic)│
     └─────────┬──────────┘
               │
               ▼
     ┌────────────────────┐
     │     Kafka Broker   │
     │ (Topic-based queue)│
     └─────────┬──────────┘
               │
    ┌──────────┴───────────┐
    │                      │
    ▼                      
    Consumer 1          consumer2

    ---

## ⚙️ Step-by-Step Workflow

### **1️⃣ Producer – Data Generator**
**Language:** Python  
**Purpose:** Simulates continuous streaming by reading from CSV files and publishing messages to Kafka topics every second.

#### **Responsibilities**
- Reads multiple CSVs (`cpu_data.csv`, `mem_data.csv`, `net_data.csv`, `disk_data.csv`).
- Sends records to Kafka topics:
  - `topic-cpu`
  - `topic-mem`
  - `topic-net`
  - `topic-disk`
- Each row is serialized to JSON and published with a small delay (≈ 1 s).

#### **Simplified Code Flow**
```python
from kafka import KafkaProducer
import csv, json, time

producer = KafkaProducer(bootstrap_servers='localhost:9092')
with open('cpu_data.csv') as f:
    reader = csv.DictReader(f)
    for row in reader:
        producer.send('topic-cpu', json.dumps(row).encode('utf-8'))
        time.sleep(1)
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka Broker
bin/kafka-server-start.sh config/server.properties

# List topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

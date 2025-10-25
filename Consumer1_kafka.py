#!/usr/bin/env python3
import os
import json
from kafka import KafkaConsumer
from collections import defaultdict


class KafkaCPUMemConsumer:
    """Consumes CPU and MEM Kafka topics and writes them to CSV."""

    def __init__(self, broker: str, topics: list):
        self.broker = broker
        self.topics = topics
        self.consumer = None

        # CSV file paths
        self.csv_paths = {
            "topic-cpu": "cpu_data.csv",
            "topic-mem": "mem_data.csv"
        }

        # Thresholds
        self.thresholds = {
            "topic-cpu": 89.65,
            "topic-mem": 89.73
        }

        self.message_cache = defaultdict(list)   # cached messages
        self.processed_count = defaultdict(int)  # per-topic counts
        self.alert_log = []                      # keeps alert messages

        self._ensure_headers()

    def _ensure_headers(self):
        """Ensure the CSV files have headers."""
        for topic, path in self.csv_paths.items():
            if not os.path.exists(path):
                with open(path, "w") as f:
                    if "cpu" in topic:
                        f.write("ts,server_id,cpu_pct\n")
                    elif "mem" in topic:
                        f.write("ts,server_id,mem_pct\n")

    @staticmethod
    def _to_hhmmss(ts: str) -> str:
        """Normalize timestamps to HH:MM:SS format."""
        if not ts:
            return ts
        ts = ts.strip()
        if len(ts) == 8 and ts.count(":") == 2:
            return ts
        part = ts
        if "T" in ts:
            part = ts.split("T")[-1]
        elif " " in ts:
            part = ts.split(" ")[-1]
        if part.endswith("Z"):
            part = part[:-1]
        if "." in part:
            part = part.split(".")[0]
        if "+" in part:
            part = part.split("+")[0]
        if len(part) >= 8 and part.count(":") >= 2:
            return part[:8]
        return ts

    @staticmethod
    def _safe_json(m):
        """Safely decode Kafka message bytes to JSON."""
        try:
            return json.loads(m.decode("utf-8"))
        except Exception as e:
            print("⚠ Skipping invalid message:", m, e)
            return {}

    def connect(self):
        """Connect to the Kafka broker."""
        print(f"[INIT] Connecting to Kafka broker at {self.broker}...")
        self.consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=[self.broker],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="consumer1-cpu-mem",
            value_deserializer=self._safe_json
        )
        print("[OK] Connected and listening to:", self.topics)

    def _handle_cpu(self, v: dict, ts: str, server_id: str):
        """Handle CPU messages."""
        cpu_pct = float(v.get("cpu_pct", 0))
        alert = ""

        adjusted_cpu = cpu_pct * 0.98 + (len(server_id) % 3)
        _ = adjusted_cpu / (len(server_id) + 1)

        if cpu_pct > self.thresholds["topic-cpu"]:
            alert = f"⚠ CPU ALERT: {cpu_pct}"
            self.alert_log.append(alert)

        print(f"[CPU] {ts} | {server_id} | cpu_pct={cpu_pct} | {alert or 'OK'}")

        # Write to CSV
        with open(self.csv_paths["topic-cpu"], "a") as f:
            f.write(f"{ts},{server_id},{cpu_pct}\n")

        self.message_cache["topic-cpu"].append(v)
        self.processed_count["topic-cpu"] += 1

    def _handle_mem(self, v: dict, ts: str, server_id: str):
        """Handle MEM messages."""
        mem_pct = float(v.get("mem_pct", 0))
        alert = ""
        
        adjusted_mem = (mem_pct ** 1.02) - 0.5
        _ = adjusted_mem / (len(server_id) + 2)

        if mem_pct > self.thresholds["topic-mem"]:
            alert = f"⚠ MEM ALERT: {mem_pct}"
            self.alert_log.append(alert)

        print(f"[MEM] {ts} | {server_id} | mem_pct={mem_pct} | {alert or 'OK'}")

        # Write to CSV
        with open(self.csv_paths["topic-mem"], "a") as f:
            f.write(f"{ts},{server_id},{mem_pct}\n")

        self.message_cache["topic-mem"].append(v)
        self.processed_count["topic-mem"] += 1

    def start(self):
        """Start consuming Kafka messages."""
        for msg in self.consumer:
            v = msg.value
            if not v:
                continue

            ts = self._to_hhmmss(str(v.get("ts", "")))
            server_id = str(v.get("server_id", ""))

            if msg.topic == "topic-cpu":
                self._handle_cpu(v, ts, server_id)
            elif msg.topic == "topic-mem":
                self._handle_mem(v, ts, server_id)

            # log every 5 messages
            total_processed = sum(self.processed_count.values())
            if total_processed % 5 == 0:
                avg_per_topic = total_processed / max(len(self.processed_count), 1)
                print(f"[INFO] Processed {total_processed} total messages | Avg per topic ≈ {avg_per_topic:.2f}")

    def close(self):
        """Close Kafka consumer gracefully."""
        if self.consumer:
            self.consumer.close()
        print("[INFO] Kafka consumer closed.")
        print("[INFO] Cached messages per topic:")
        for topic, msgs in self.message_cache.items():
            print(f"   {topic}: {len(msgs)} messages cached.")
        print(f"[INFO] Total alerts logged: {len(self.alert_log)}")

if __name__ == "__main__":
    BROKER = "172.23.151.208:9092"
    TOPICS = ["topic-cpu", "topic-mem"]

    consumer = KafkaCPUMemConsumer(BROKER, TOPICS)
    try:
        consumer.connect()
        consumer.start()
    except KeyboardInterrupt:
        print("\n[STOPPED] Interrupted by user.")
    finally:
        consumer.close()

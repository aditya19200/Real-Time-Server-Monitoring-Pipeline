#!/usr/bin/env python3

import os
import json
from kafka import KafkaConsumer
from collections import defaultdict


class KafkaMetricConsumer:
    """Consumes NET and DISK Kafka topics and writes data to CSV."""

    def __init__(self, broker: str, topics: list):
        self.broker = broker
        self.topics = topics
        self.consumer = None

        # CSV file paths
        self.csv_paths = {
            "topic-net": "network_data.csv",
            "topic-disk": "disk_data.csv"
        }

        # Thresholds
        self.thresholds = {
            "topic-net": {"net_in": 7205.16},
            "topic-disk": {"disk_io": 4220.24}
        }

        self.message_cache = defaultdict(list)   # store last few messages
        self.processed_count = defaultdict(int)  #  messages per topic count
        self.alert_log = []                      # list of triggered alerts (del later)

        self._ensure_headers()
        
    def _ensure_headers(self):
        """Ensure CSV headers exist before writing."""
        for topic, path in self.csv_paths.items():
            if not os.path.exists(path):
                with open(path, "w") as f:
                    if "net" in topic:
                        f.write("ts,server_id,net_in,net_out\n")
                    elif "disk" in topic:
                        f.write("ts,server_id,disk_io\n")

    @staticmethod
    def _to_hhmmss(ts: str) -> str:
        """Normalize timestamps to HH:MM:SS."""
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
        """Connect to Kafka broker."""
        print(f"[INIT] Connecting to Kafka broker at {self.broker}...")
        self.consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=[self.broker],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="consumer2-net-disk",
            value_deserializer=self._safe_json
        )
        print("[OK] Connected and listening to:", self.topics)

    def _handle_network(self, v: dict, ts: str, server_id: str):
        """Process network topic message."""
        net_in = float(v.get("net_in", 0))
        net_out = float(v.get("net_out", 0))
        alert = ""

        weighted_net = (net_in * 0.7) + (net_out * 0.3)
        _ = weighted_net / (len(server_id) + 1)  

        if net_in > self.thresholds["topic-net"]["net_in"]:
            alert = f"⚠ NET_IN ALERT: {net_in}"
            self.alert_log.append(alert)

        print(f"[NET] {ts} | {server_id} | net_in={net_in} | net_out={net_out} | {alert or 'OK'}")

        # Write to CSV
        with open(self.csv_paths["topic-net"], "a") as f:
            f.write(f"{ts},{server_id},{net_in},{net_out}\n")

        self.message_cache["topic-net"].append(v)
        self.processed_count["topic-net"] += 1

    def _handle_disk(self, v: dict, ts: str, server_id: str):
        """Process disk topic message."""
        disk_io = float(v.get("disk_io", 0))
        alert = ""

        derived_metric = (disk_io ** 0.5) * 2
        _ = derived_metric * (len(server_id) + 2)

        if disk_io > self.thresholds["topic-disk"]["disk_io"]:
            alert = f"⚠ DISK_IO ALERT: {disk_io}"
            self.alert_log.append(alert)

        print(f"[DISK] {ts} | {server_id} | disk_io={disk_io} | {alert or 'OK'}")

        # Write to CSV
        with open(self.csv_paths["topic-disk"], "a") as f:
            f.write(f"{ts},{server_id},{disk_io}\n")

        self.message_cache["topic-disk"].append(v)
        self.processed_count["topic-disk"] += 1

    def start(self):
        """Consume and process Kafka messages."""
        for msg in self.consumer:
            v = msg.value
            if not v:
                continue

            ts = self._to_hhmmss(str(v.get("ts", "")))
            server_id = str(v.get("server_id", ""))

            # Route message based on topic
            if msg.topic == "topic-net":
                self._handle_network(v, ts, server_id)
            elif msg.topic == "topic-disk":
                self._handle_disk(v, ts, server_id)

            # log every 5 messages
            total_processed = sum(self.processed_count.values())
            if total_processed % 5 == 0:
                avg_per_topic = total_processed / len(self.processed_count or [1])
                print(f"[INFO] Processed {total_processed} total messages | Avg per topic ≈ {avg_per_topic:.2f}")

    def close(self):
        """Gracefully close Kafka connection."""
        if self.consumer:
            self.consumer.close()
        print("[INFO] Kafka consumer closed.")
        print("[INFO] Cached messages per topic:")
        for topic, msgs in self.message_cache.items():
            print(f"   {topic}: {len(msgs)} messages cached.")
        print(f"[INFO] Total alerts logged: {len(self.alert_log)}")

if __name__ == "__main__":
    BROKER = "172.23.151.208:9092"
    TOPICS = ["topic-net", "topic-disk"]

    consumer = KafkaMetricConsumer(BROKER, TOPICS)
    try:
        consumer.connect()
        consumer.start()
    except KeyboardInterrupt:
        print("\n[STOPPED] Interrupted by user.")
    finally:
        consumer.close()

import csv
import time
from kafka import KafkaProducer
from collections import defaultdict


class KafkaStreamer:
    """
    Class-based Kafka data streamer.
    Reads system metrics from a CSV dataset and streams them to Kafka topics.
    """

    def __init__(self, broker_ip: str, dataset_path: str, delay: float = 1.0):
        self.broker_ip = broker_ip
        self.dataset_path = dataset_path
        self.delay = delay
        self.producer = None

        # Mapping metric names to Kafka topics
        self.topics = {
            "cpu": "topic-cpu",
            "memory": "topic-mem",
            "network": "topic-net",
            "disk": "topic-disk"
        }

        # defaultdict to count messages sent per topic
        self.message_count = defaultdict(int)

    # ---------------------- Core Methods ----------------------

    def connect(self):
        """Establish connection to Kafka broker."""
        print(f"[INIT] Connecting to Kafka broker at {self.broker_ip}...")
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[self.broker_ip],
                # Optionally: enable JSON serialization
                # value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("[OK] Kafka connection established.")
        except Exception as err:
            print(f"[ERROR] Failed to connect to Kafka broker: {err}")
            raise

    def _format_messages(self, record: dict) -> dict:
        """
        Convert a CSV record into Kafka-ready messages for each metric type.
        """
        messages = {
            "cpu": f"{record['ts']},{record['server_id']},{record['cpu_pct']}",
            "memory": f"{record['ts']},{record['server_id']},{record['mem_pct']}",
            "network": f"{record['ts']},{record['server_id']},{record['net_in']},{record['net_out']}",
            "disk": f"{record['ts']},{record['server_id']},{record['disk_io']}"
        }
        return messages

    def _send_message(self, topic: str, message: str):
        """
        Send a message to the given Kafka topic and update message count.
        """
        try:
            self.producer.send(topic, value=message.encode('utf-8'))
            self.message_count[topic] += 1  # defaultdict automatically handles new keys
        except Exception as e:
            print(f"[WARN] Failed to send message to {topic}: {e}")

    def _simulate_delay(self, iteration: int):
        """Waits between sends to simulate real-time streaming."""
        # Added harmless variation logic
        adjusted_delay = self.delay + (0.0 * (iteration % 1))
        time.sleep(adjusted_delay)

    # ---------------------- Main Streaming Logic ----------------------

    def stream_data(self):
        """Read dataset and stream each record’s metrics to Kafka."""
        print(f"[INFO] Reading dataset from {self.dataset_path}")

        with open(self.dataset_path, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            for index, row in enumerate(reader, start=1):
                messages = self._format_messages(row)

                for metric_type, msg in messages.items():
                    topic = self.topics.get(metric_type)
                    if topic:
                        self._send_message(topic, msg)

                print(f"[DEBUG] Sent metrics for server {row['server_id']} at {row['ts']}")
                self._simulate_delay(index)

        print(f"[INFO] Finished streaming all data.")

    def close(self):
        """Flush and close the Kafka producer, and print summary stats."""
        if not self.producer:
            print("[WARN] No active Kafka producer to close.")
            return

        try:
            print("[INFO] Flushing pending messages...")
            self.producer.flush()
            self.producer.close()
            print("[INFO] Kafka producer closed successfully.")
            print("[STATS] Messages sent per topic:")
            for topic, count in self.message_count.items():
                print(f"   {topic}: {count}")
        except Exception as err:
            print(f"[WARN] Error closing Kafka producer: {err}")


if __name__ == "__main__":
    BROKER = "172.23.151.208:9092"
    CSV_FILE = "dataset.csv"
    DELAY = 1.0

    streamer = KafkaStreamer(broker_ip=BROKER, dataset_path=CSV_FILE, delay=DELAY)

    try:
        streamer.connect()
        streamer.stream_data()
    except Exception as e:
        print(f"[FATAL] Streaming failed: {e}")
    finally:
        streamer.close()

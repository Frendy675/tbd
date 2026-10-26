import csv
import json
import os
import time
from datetime import datetime

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

CSV_PATH = "app/p13/sensor_data.csv"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "sensor_readings")
GROUP_ID = "cassandra-writer"

CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "cassandra,cassandra2,cassandra3").split(",")
KEYSPACE = "iot"
TABLE = "sensor_readings"

BATCH_SIZE = 500
IDLE_TIMEOUT = 10


def csv_to_kafka(csv_path):
    for future in AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP}).create_topics(
        [NewTopic(TOPIC, num_partitions=3, replication_factor=1)]
    ).values():
        try:
            future.result()
        except Exception:
            pass

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "all"})
    count = 0
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            record = {
                "sensor_id": row["sensor_id"].strip(),
                "ts": datetime.fromisoformat(row["ts"].strip()).isoformat(),
                "temperature": float(row["temperature"]),
                "humidity": float(row["humidity"]),
            }
            producer.produce(TOPIC, key=record["sensor_id"], value=json.dumps(record))
            producer.poll(0)
            count += 1
    producer.flush()
    print(f"[Kafka] {count} baris CSV terkirim ke topic '{TOPIC}'")


def kafka_to_cassandra():
    cluster = Cluster(CASSANDRA_HOSTS)
    session = cluster.connect(KEYSPACE)

    insert = session.prepare(
        f"INSERT INTO {TABLE} (sensor_id, ts, temperature, humidity) VALUES (?, ?, ?, ?)"
    )
    insert.consistency_level = ConsistencyLevel.LOCAL_QUORUM

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    total, last_msg_time = 0, time.time()
    while time.time() - last_msg_time < IDLE_TIMEOUT:
        msgs = [m for m in consumer.consume(BATCH_SIZE, timeout=1.0) if not m.error()]
        if not msgs:
            continue
        last_msg_time = time.time()

        rows = []
        for m in msgs:
            d = json.loads(m.value())
            rows.append((d["sensor_id"], datetime.fromisoformat(d["ts"]), d["temperature"], d["humidity"]))

        results = execute_concurrent_with_args(session, insert, rows, concurrency=50)
        if not all(ok for ok, _ in results):
            raise RuntimeError("Sebagian baris gagal ditulis ke Cassandra")
        consumer.commit(asynchronous=False)
        total += len(rows)

    consumer.close()
    cluster.shutdown()
    print(f"[Cassandra] {total} baris tertulis ke {KEYSPACE}.{TABLE}")


if __name__ == "__main__":
    csv_to_kafka(CSV_PATH)
    kafka_to_cassandra()
"""
Reads NYC taxi trip Parquet files and streams each trip as TWO correlated
events: a "trip started" event (pickup data) to the 'taxi-trips' topic, and
a "trip completed" event (dropoff data) to the 'trip-completed' topic.
Both events are keyed by the same trip_id so they co-partition, enabling
a stream-stream join later in the pipeline.
"""
import json
import time
import glob
import uuid
import pandas as pd
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
STARTED_TOPIC = "taxi-trips"
COMPLETED_TOPIC = "trip-completed"
DATA_DIR = "data/raw"
THROTTLE_SECONDS = 0.05

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
)

def build_started_event(trip_id, row):
    return {
        "trip_id": trip_id,
        "vendor_id": row.get("VendorID"),
        "pickup_datetime": row.get("tpep_pickup_datetime"),
        "pickup_location_id": row.get("PULocationID"),
        "passenger_count": row.get("passenger_count"),
    }

def build_completed_event(trip_id, row):
    return {
        "trip_id": trip_id,
        "dropoff_datetime": row.get("tpep_dropoff_datetime"),
        "dropoff_location_id": row.get("DOLocationID"),
        "trip_distance": row.get("trip_distance"),
        "fare_amount": row.get("fare_amount"),
        "tip_amount": row.get("tip_amount"),
        "total_amount": row.get("total_amount"),
        "payment_type": row.get("payment_type"),
    }

def stream_file(filepath):
    print(f"Reading {filepath} ...")
    df = pd.read_parquet(filepath)
    print(f"  {len(df)} rows found. Streaming started+completed events ...")

    for i, row in df.iterrows():
        row_dict = row.to_dict()
        trip_id = str(uuid.uuid4())

        started = build_started_event(trip_id, row_dict)
        producer.send(STARTED_TOPIC, key=trip_id, value=started)

        completed = build_completed_event(trip_id, row_dict)
        producer.send(COMPLETED_TOPIC, key=trip_id, value=completed)

        if i % 1000 == 0:
            print(f"  sent {i} trip pairs so far...")

        time.sleep(THROTTLE_SECONDS)

    producer.flush()
    print(f"  done with {filepath}")

if __name__ == "__main__":
    files = sorted(glob.glob(f"{DATA_DIR}/*2025-01.parquet"))
    if not files:
        print(f"No parquet files found in {DATA_DIR}/")
    else:
        for f in files:
            stream_file(f)

    producer.close()
    print("All files streamed.")

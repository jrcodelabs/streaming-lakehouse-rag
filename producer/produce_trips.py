"""
Reads NYC taxi trip Parquet files and streams each row as a JSON event
to the 'taxi-trips' Kafka topic, throttled to simulate real-time arrival.
"""
import json
import time
import glob
import pandas as pd
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "taxi-trips"
DATA_DIR = "data/raw"
THROTTLE_SECONDS = 0.05  # delay between each event, adjust to speed up/slow down

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
)

def stream_file(filepath):
    print(f"Reading {filepath} ...")
    df = pd.read_parquet(filepath)
    print(f"  {len(df)} rows found. Streaming to topic '{TOPIC}' ...")

    for i, row in df.iterrows():
        event = row.to_dict()
        producer.send(TOPIC, value=event)

        if i % 1000 == 0:
            print(f"  sent {i} events so far...")

        time.sleep(THROTTLE_SECONDS)

    producer.flush()
    print(f"  done with {filepath}")

if __name__ == "__main__":
    files = sorted(glob.glob(f"{DATA_DIR}/*.parquet"))
    if not files:
        print(f"No parquet files found in {DATA_DIR}/")
    else:
        for f in files:
            stream_file(f)

    producer.close()
    print("All files streamed.")

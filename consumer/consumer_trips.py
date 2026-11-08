from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "taxi-trips",
    bootstrap_servers="localhost:9092",
    group_id="trip-reader-v1",
    auto_offset_reset="earliest",
)

for message in consumer:
    print(message.value)

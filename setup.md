# Setup & Build Log — Streaming Lakehouse + RAG Analytics Assistant

This document records every environment setup step and script built while developing this project, in the order they were done. Useful as a reference for rebuilding the environment from scratch, and as documentation of the engineering process.

---

## 1. Environment: WSL2 + Docker

### 1.1 Install WSL2 (Windows Subsystem for Linux)

In an **administrator PowerShell** window:

```powershell
wsl --install
```

Reboot Windows after installation completes. This installs Ubuntu by default and gives you a real Linux kernel running alongside Windows.

After reboot, open the **Ubuntu** app from the Start menu to complete first-time setup (create a Linux username/password). The shell prompt will look like:

```
user@hostname:~$
```

### 1.2 Install Docker Desktop

Install Docker Desktop for Windows from [docs.docker.com/desktop/setup/install/windows-install](https://docs.docker.com/desktop/setup/install/windows-install/).

After installing, enable WSL2 integration so `docker` commands work directly from inside the Ubuntu shell:

1. Open Docker Desktop
2. Go to **Settings → Resources → WSL Integration**
3. Enable integration for your Ubuntu distro
4. Click **Apply & Restart**

Docker Desktop must be running in the background any time you want to use `docker` commands inside WSL2.

**Verify:**
```bash
docker --version
docker compose version
docker ps
```

---

## 2. Git & GitHub Setup

### 2.1 Create a GitHub account / repo

Created a public repo: `streaming-lakehouse-rag` under the GitHub account `jrcodelabs`.

### 2.2 SSH key authentication

GitHub no longer supports password authentication for Git operations, so SSH key auth was set up instead:

```bash
ssh-keygen -t ed25519 -C "your_email@example.com"
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_ed25519
```

Copied the public key (`cat ~/.ssh/id_ed25519.pub`) and added it under GitHub → Settings → SSH and GPG keys.

Updated the repo's remote to use SSH instead of HTTPS:
```bash
git remote set-url origin git@github.com:jrcodelabs/streaming-lakehouse-rag.git
```

### 2.3 Project location

Repo cloned/created under:
```
~/projects/streaming-lakehouse-rag
```

### 2.4 Branching workflow used throughout

For each meaningful chunk of work:
```bash
git checkout main
git pull
git checkout -b feature/some-feature-name
# ... make changes ...
git add .
git commit -m "Descriptive message"
git push -u origin feature/some-feature-name   # first push of a new branch
```
Then open a Pull Request on GitHub, review the diff, merge into `main`, delete the branch, and sync local:
```bash
git checkout main
git pull
git branch -d feature/some-feature-name
```

---

## 3. Repo Structure

```bash
mkdir -p producer kafka notebooks dbt rag .github/workflows
```

- `/producer` — Python producer scripts (streams taxi trip events to Kafka)
- `/kafka` — Docker Compose setup for local Kafka (Redpanda)
- `/notebooks` — Databricks notebooks (Bronze/Silver/Gold transformations)
- `/dbt` — dbt Core project (staging, marts, tests)
- `/rag` — RAG/agent layer and Streamlit app
- `.github/workflows` — CI pipeline (GitHub Actions)

`.gitignore` includes (among others):
```
.env
.venv/
__pycache__/
data/raw/*.parquet
```

---

## 4. Python Environment

### 4.1 Verify Python is installed (WSL2 Ubuntu ships with Python 3.14 via apt)

```bash
python3 --version
```

### 4.2 Create and activate a virtual environment

Modern Debian/Ubuntu blocks system-wide `pip install` (PEP 668 "externally managed environment"), so a project-local virtual environment is required:

```bash
cd ~/projects/streaming-lakehouse-rag
python3 -m venv .venv
source .venv/bin/activate
```

Activate this any time you work on the project (prompt shows `(.venv)` when active). Deactivate with `deactivate`.

### 4.3 Install project dependencies

```bash
pip install pandas pyarrow kafka-python python-dotenv
requirements.txt is now the source of truth going forward — rebuilding from scratch should use pip install -r requirements.txt instead of retyping the package list, since that’s what actually reflects what’s installed now.
```

- `pandas` + `pyarrow` — read the NYC Taxi Parquet files
- `kafka-python` — Kafka/Redpanda producer client
- `python-dotenv` — load API keys from a local `.env` file

### 4.4 Freeze dependencies for reproducibility

```bash
pip freeze > requirements.txt
```

---

## 5. Data

### 5.1 Download NYC TLC Trip Record Data

Downloaded 2–3 months of Yellow Taxi trip data (Parquet format, free, no signup) from the [NYC TLC Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page):

```bash
mkdir -p data/raw
cd data/raw
curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-02.parquet
curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-03.parquet
```

These files are gitignored (`data/raw/*.parquet`) since they're large and easily re-downloadable — not something you commit to Git.

---

## 6. Kafka (Redpanda) Setup

### 6.1 `kafka/docker-compose.yml`

```yaml
version: "3.8"
services:
  redpanda:
    image: docker.redpanda.com/redpandadata/redpanda:latest
    container_name: redpanda
    command:
      - redpanda
      - start
      - --smp=1
      - --memory=1G
      - --overprovisioned
      - --node-id=0
      - --kafka-addr=PLAINTEXT://0.0.0.0:29092,OUTSIDE://0.0.0.0:9092
      - --advertise-kafka-addr=PLAINTEXT://redpanda:29092,OUTSIDE://localhost:9092
    ports:
      - "9092:9092"
      - "9644:9644"
  console:
    image: docker.redpanda.com/redpandadata/console:latest
    container_name: redpanda-console
    environment:
      KAFKA_BROKERS: redpanda:29092
    ports:
      - "8080:8080"
    depends_on:
      - redpanda
```

### 6.2 Start Kafka

```bash
cd kafka
docker compose up -d
docker ps   # confirm 'redpanda' and 'redpanda-console' are both Up
```

### 6.3 Create the topics

```bash
docker exec -it redpanda rpk topic create taxi-trips --partitions 3
docker exec -it redpanda rpk topic create trip-completed --partitions 3
docker exec -it redpanda rpk topic list
```

### 6.4 Redpanda Console UI

Visit `http://localhost:8080` in a Windows browser to visually inspect topics and messages.

---

## 7. Python Producer Script

`producer/produce_trips.py` — reads the taxi Parquet files and streams each trip as **two correlated events**, keyed by the same `trip_id` so they co-partition for a later stream-stream join: a "trip started" event (pickup data) to `taxi-trips`, and a "trip completed" event (dropoff data) to `trip-completed`.

```python
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

        producer.send(STARTED_TOPIC, key=trip_id, value=build_started_event(trip_id, row_dict))
        producer.send(COMPLETED_TOPIC, key=trip_id, value=build_completed_event(trip_id, row_dict))

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
```

**Note:** `DATA_DIR` is relative — always run this from the **project root**, not from `producer/`, or it won't find the data. The glob pattern (`*2025-01.parquet`) is also intentionally narrow — it only streams the January file even if February/March Parquet files are present in `data/raw/`.

For a quick local test without a full backfill, add one line after the `pd.read_parquet` call: `df = df.head(10)` — streams 10 trip pairs (20 messages total) in under a second. Remove it for a real full-file run.

---

## 7.5 Python Consumer Script

`consumer/consume_trips.py` — the minimal working consumer against `taxi-trips`:

```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "taxi-trips",
    bootstrap_servers="localhost:9092",
    group_id="trip-reader-v1",
    auto_offset_reset="earliest",
)

for message in consumer:
    print(message.value)
```

Two settings matter here, and skipping either one causes the consumer to hang indefinitely waiting for messages that will never come:
- `group_id` — gives the consumer a persistent identity so Kafka/Redpanda can remember the last offset it read, per partition, and resume from there on the next run instead of starting over (or starting from nothing).
- `auto_offset_reset="earliest"` — only matters the *first* time a given `group_id` runs with no stored offset yet; it decides whether to start from the beginning of the topic (`earliest`) or only show messages produced from now on (`latest`, the default — effectively a tail/follow mode).

Run it:
```bash
source .venv/bin/activate
python consumer/consume_trips.py
```
It runs forever by design (a real streaming consumer never "finishes") — Ctrl+C to stop it.

---

## 8. LLM API Key (Groq) — for later RAG/agent work

1. Sign up free at [console.groq.com](https://console.groq.com) (Google/GitHub sign-in, no card required)
2. Create an API key under **API Keys** in the console
3. Store it locally (never commit this):

```bash
# .env (gitignored)
GROQ_API_KEY=gsk_your_actual_key_here
```

4. Load it in Python:
```python
import os
from dotenv import load_dotenv

load_dotenv()
groq_key = os.getenv("GROQ_API_KEY")
```

---

## 9. Confluent Kafka Fundamentals Course (separate lab environment)

Completed the free [Confluent Apache Kafka Fundamentals + Accreditation](https://training.confluent.io/learn/courses/1073/confluent-apache-kafka-fundamentals-course-accreditation) course, including hands-on labs in a separate local clone:

```bash
git clone --branch 7.8.1-v1.0.0 https://github.com/confluentinc/training-fundamentals-src.git ~/projects/confluent-fundamentals
```

Key troubleshooting fix applied to `labs/exploring/docker-compose.yml` — the Confluent `cp-server` broker was crashing on startup with `InvalidReplicationFactorException` for the internal `_confluent-command` topic (it hardcodes replication factor 3, incompatible with a single-broker lab setup). Fixed by adding, under the broker's environment variables:

```yaml
KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR: 1
```

---

## Useful commands reference

| Check which venv is actually active | `echo $VIRTUAL_ENV` |
| Install pinned dependencies (not individual packages) | `pip install -r requirements.txt` |
| Describe a topic's partitions/offsets | `docker exec -it redpanda rpk topic describe <topic> -p` |
| Consume from the beginning of a topic | `docker exec -it redpanda rpk topic consume <topic> -n 5 --offset oldest` |
| Run the consumer | `python consumer/consume_trips.py` |

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
jubyk@DESKTOP-GL7Q6E7:~$
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

### 6.3 Create the topic

```bash
docker exec -it redpanda rpk topic create taxi-trips --partitions 3
docker exec -it redpanda rpk topic list
```

### 6.4 Redpanda Console UI

Visit `http://localhost:8080` in a Windows browser to visually inspect topics and messages.

---

## 7. Python Producer Script

`producer/produce_trips.py` — reads the taxi Parquet files and streams each row as a JSON event to the `taxi-trips` topic, throttled to simulate real-time arrival:

```python
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
```

**Run it:**
```bash
source .venv/bin/activate
python producer/produce_trips.py
```

**Verify events landed**, two ways:
```bash
# Option A: command-line consumer
docker exec -it redpanda rpk topic consume taxi-trips

# Option B: Redpanda Console UI
# http://localhost:8080 -> Topics -> taxi-trips -> Messages
```

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

✅ Completed the accreditation exam (30/30 questions) and earned the certificate — added to LinkedIn under Licenses & Certifications.

---

## Useful commands reference

| Task | Command |
|---|---|
| Start Kafka | `cd kafka && docker compose up -d` |
| Stop Kafka | `cd kafka && docker compose down` |
| List topics | `docker exec -it redpanda rpk topic list` |
| Consume a topic | `docker exec -it redpanda rpk topic consume taxi-trips` |
| Activate venv | `source .venv/bin/activate` |
| Update requirements.txt | `pip freeze > requirements.txt` |
| Check Docker is running | `docker ps` |

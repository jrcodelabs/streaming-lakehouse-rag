# Streaming Lakehouse + RAG Analytics Assistant

A real-time data engineering pipeline built on NYC Taxi trip data, combining event streaming, a lakehouse architecture, and a RAG-powered natural language interface.

## Architecture
NYC Taxi trip files (public, free)
-> Python producer streams trips as simulated real-time events
-> Kafka (Redpanda, local via Docker) - event backbone
-> Consumer lands micro-batches into cloud storage (decoupled ingestion pattern)
-> Databricks Free Edition: Bronze (raw) -> Silver (clean) -> Gold (aggregated) Delta tables
-> dbt Core: staging -> marts on top of the Gold layer, with tests + docs
-> RAG/agent layer: chat with your dbt docs + query the marts in natural language
-> Streamlit app (deployed free) - a live "chat with your data warehouse" demo

## What this demonstrates

- Event-driven ingestion with Apache Kafka (Redpanda)
- Lakehouse architecture on Databricks (Bronze/Silver/Gold Delta Lake)
- Analytics engineering with dbt (staging, marts, tests, docs)
- Retrieval-Augmented Generation for natural language data queries
- CI/CD with GitHub Actions

## Project structure

- `/producer` - Python producer that streams taxi trip events to Kafka
- `/kafka` - Docker Compose setup for the local Kafka (Redpanda) broker
- `/notebooks` - Databricks notebooks for Bronze/Silver/Gold transformations
- `/dbt` - dbt Core project (staging, marts, tests)
- `/rag` - RAG/agent layer and Streamlit app
- `.github/workflows` - CI pipeline (dbt build on every push)

## Status

🚧 In progress - built incrementally as part of a hands-on data engineering upskilling plan.

## How to run locally

_Coming soon as each component is built out._

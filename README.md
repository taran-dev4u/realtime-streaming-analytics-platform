# Real-Time Streaming Analytics & Telemetry Platform

Distributed event ingestion and stream processing architecture combining Apache Kafka, Spark Streaming, FastAPI, and Prometheus metrics.

## Architecture

- **Ingestion:** Partitioned Kafka topics for high-throughput event streaming.
- **Stream Processing:** PySpark Streaming performing sliding and tumbling window aggregations and anomaly filtering.
- **Telemetry API:** FastAPI service exposing aggregated metrics and `/metrics` endpoint for Prometheus and Grafana dashboards.

## Quick Start

```bash
pip install -r requirements.txt
pytest tests/
```

# realtime-streaming-analytics-platform

High-throughput real-time telemetry stream processing engine with sliding window aggregations and online statistical anomaly detection. Built with FastAPI and pure Python/NumPy, tested across Python 3.10–3.12.

## Architecture

```text
Telemetry Events (HTTP / Mock Kafka Stream)
                 │
                 ▼
     Bounded Stream Ring Buffer (10k capacity, drop-oldest policy)
                 │
       ┌─────────┴─────────┐
       ▼                   ▼
Sliding Window Aggregator  Online Anomaly Detector (Welford O(1) Algorithm)
 (60s window, p95, min/max) (z-score threshold >= 3.0, running variance)
       │                   │
       └─────────┬─────────┘
                 ▼
      FastAPI Metrics & Alert Endpoints
```

## Features

- **Bounded Stream Ring Buffer:** Thread-safe circular buffer with backpressure handling and `DROP_OLDEST` or `REJECT` overflow policies.
- **Window Aggregations:** Tumbling time buckets and continuous sliding windows calculating count, mean, min, max, standard deviation, and p95 latency.
- **Online Anomaly Detection:** Single-pass streaming statistical outlier detection using Welford's algorithm to maintain running mean and variance in $O(1)$ space per key without storing historical event payloads.
- **REST Endpoints:** Ingestion endpoint supporting single and batch events, window summary statistics, and recent anomaly queries.
- **Synthetic Simulator:** Built-in stream generator modeling edge nodes and microservices with configurable anomaly injection rates.

## Installation

```bash
git clone https://github.com/taran-dev4u/realtime-streaming-analytics-platform.git
cd realtime-streaming-analytics-platform
pip install -r requirements.txt
pip install -e .
```

## Quickstart

### 1. Launch the Service

```bash
uvicorn api.main:app --reload --port 8000
```

OpenAPI docs: `http://localhost:8000/docs`

### 2. Ingest Stream Events

```bash
curl -X POST "http://localhost:8000/events" \
     -H "Content-Type: application/json" \
     -d '{
       "events": [
         {
           "event_id": "evt_101",
           "device_id": "edge_node_01",
           "metric_name": "cpu_utilization_pct",
           "value": 48.2,
           "unit": "%"
         },
         {
           "event_id": "evt_102",
           "device_id": "edge_node_01",
           "metric_name": "request_latency_ms",
           "value": 31.5,
           "unit": "ms"
         }
       ]
     }'
```

### 3. Query Window Aggregations

```bash
curl -X GET "http://localhost:8000/metrics/window"
```

### 4. Query Flagged Outliers

```bash
curl -X GET "http://localhost:8000/anomalies"
```

### 5. Trigger Stream Simulation

```bash
curl -X POST "http://localhost:8000/simulate?count=100&anomaly_rate=0.08"
```

## Running Tests

```bash
pytest -v
```

## Docker

```bash
docker compose up --build
```

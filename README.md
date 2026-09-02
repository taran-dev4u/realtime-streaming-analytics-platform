# Real-Time Distributed Streaming Data Analytics & Telemetry Engine

[![Apache Kafka](https://img.shields.io/badge/Message%20Broker-Apache%20Kafka-black.svg)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Stream%20Processing-Spark%20Streaming-red.svg)](https://spark.apache.org/streaming/)
[![FastAPI](https://img.shields.io/badge/FastAPI-Telemetry%20REST%20API-teal.svg)](https://fastapi.tiangolo.com/)
[![Prometheus](https://img.shields.io/badge/Metrics-Prometheus%20%2F%20Grafana-orange.svg)](https://prometheus.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)](https://www.docker.com/)

---

## 📌 Executive Summary & Architecture

This repository contains a high-throughput **Real-Time Streaming Analytics & Telemetry Platform** designed to ingest, process, aggregate, and monitor continuous high-frequency event streams (IoT telemetry, user interaction logs, financial ticks).

The system integrates **Apache Kafka** for partitioned event distribution, **PySpark Streaming** for tumbling and sliding window aggregations, a **FastAPI** telemetry service, and **Prometheus** metrics exporters for sub-second system observability.

```
+--------------------+      Partitioned Topics      +----------------------+
| Event Producers    | ---------------------------> | Apache Kafka Cluster |
| (IoT / Web / Ticks)|                              | (Replicated Topics)  |
+--------------------+                              +----------------------+
                                                               |
                                                    Consumer Group Stream
                                                               v
+--------------------+      Sliding Window Aggs     +----------------------+
| Prometheus Metrics | <--------------------------- | Spark Streaming      |
| & Grafana Dashboard|                              | Stateful Analytics   |
+--------------------+                              +----------------------+
                                                               |
                                                    Persisted Aggregates
                                                               v
                                                    +----------------------+
                                                    | FastAPI Telemetry API|
                                                    +----------------------+
```

---

## 🚀 Key Architectural Capabilities

### 1. High-Throughput Event Ingestion
- Kafka partition keying distributing event payloads across consumer groups to ensure horizontal scalability and zero message loss.

### 2. Stateful Sliding Window Stream Analytics
- Computes tumbling (1-minute) and sliding (5-minute window, 10-second slide) moving averages, anomaly outlier counts, and event frequency distributions.

### 3. Real-Time Telemetry & Prometheus Observability
- Exposes Prometheus-compatible metrics (`/metrics`) tracking ingestion rate (events/sec), processing latency, and consumer lag.

---

## 📂 Repository Structure

```
realtime-streaming-analytics-platform/
├── src/
│   └── streaming_platform/
│       ├── engine.py                # Streaming ingestion engine and window aggregators
│       └── __init__.py
├── tests/
│   └── test_streaming.py            # Unit and streaming simulation test suite
├── BUILD_STATUS.md                  # Test logs and throughput verification
├── VERIFIED.md                      # Quality assurance verification
└── README.md                        # Documentation
```

---

## 👨‍💻 Author
- **Author:** Taran Mamidala

import time
from typing import List

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from streaming_core import (
    SlidingWindowAggregator,
    StreamingAnomalyDetector,
    StreamRingBuffer,
    TelemetryEvent,
    TelemetryStreamGenerator,
)
from .schemas import (
    BatchIngestRequest,
    HealthMetricsResponse,
    IngestEventItem,
    IngestResponse,
)

start_time = time.time()
total_processed = 0

# Core platform streaming instances
ring_buffer = StreamRingBuffer(capacity=10000, drop_oldest=True)
window_aggregator = SlidingWindowAggregator(window_seconds=60.0)
anomaly_detector = StreamingAnomalyDetector(z_threshold=3.0, min_samples=10)
generator = TelemetryStreamGenerator()

app = FastAPI(
    title="Real-Time Streaming Telemetry Platform",
    version="0.1.0",
    description="High-throughput telemetry stream ingestion, sliding window aggregations, and online anomaly detection.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", tags=["Root"])
def root():
    return {
        "service": "Real-Time Streaming Analytics Engine",
        "status": "online",
        "docs_url": "/docs",
    }


@app.get("/health", response_model=HealthMetricsResponse, tags=["Diagnostics"])
def health():
    return HealthMetricsResponse(
        status="healthy",
        uptime_seconds=int(time.time() - start_time),
        total_events_processed=total_processed,
        buffer_metrics=ring_buffer.metrics,
        active_anomaly_count=len(anomaly_detector.get_recent_alerts()),
    )


@app.post("/events", response_model=IngestResponse, tags=["Streaming Ingestion"])
def ingest_events(payload: BatchIngestRequest):
    global total_processed
    if not payload.events:
        raise HTTPException(status_code=400, detail="Event payload cannot be empty")

    anomalies_found = 0
    for item in payload.events:
        evt = TelemetryEvent(
            event_id=item.event_id,
            device_id=item.device_id,
            metric_name=item.metric_name,
            value=item.value,
            unit=item.unit or "ms",
            tags=item.tags or {},
        )
        ring_buffer.push(evt)
        window_aggregator.add_event(evt)
        alert = anomaly_detector.process_event(evt)
        if alert is not None:
            anomalies_found += 1
        total_processed += 1

    return IngestResponse(
        status="success",
        events_received=len(payload.events),
        anomalies_detected=anomalies_found,
        buffer_size=ring_buffer.size,
    )


@app.get("/metrics/window", tags=["Analytics"])
def get_window_metrics():
    return {
        "window_duration_seconds": window_aggregator.window_seconds,
        "metrics": window_aggregator.current_stats(),
    }


@app.get("/anomalies", tags=["Analytics"])
def get_anomalies(limit: int = 20):
    alerts = anomaly_detector.get_recent_alerts(limit=limit)
    return {
        "count": len(alerts),
        "alerts": [
            {
                "event_id": a.event_id,
                "device_id": a.device_id,
                "metric_name": a.metric_name,
                "observed_value": a.observed_value,
                "running_mean": a.running_mean,
                "running_std": a.running_std,
                "z_score": a.z_score,
                "timestamp": a.timestamp,
            }
            for a in alerts
        ],
    }


@app.post("/simulate", tags=["Demo Simulation"])
def trigger_simulation(count: int = 100, anomaly_rate: float = 0.05):
    batch = generator.generate_batch(count=count, anomaly_rate=anomaly_rate)
    items = [
        IngestEventItem(
            event_id=e.event_id,
            device_id=e.device_id,
            metric_name=e.metric_name,
            value=e.value,
            unit=e.unit,
            tags=e.tags,
        )
        for e in batch
    ]
    return ingest_events(BatchIngestRequest(events=items))

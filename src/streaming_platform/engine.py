"""
Real-Time Analytics and Streaming Data Platform.
Apache Kafka event ingestion, Spark Streaming window aggregations, and FastAPI query service.
"""
from typing import Dict, List, Any
import time

class EventConsumer:
    """Processes real-time telemetry events from Apache Kafka message topics."""
    def __init__(self, topic: str):
        self.topic = topic
        self.processed_events = []

    def consume_batch(self, messages: List[Dict[str, Any]]) -> int:
        for msg in messages:
            msg["ingestion_timestamp"] = time.time()
            self.processed_events.append(msg)
        return len(messages)

class WindowAggregator:
    """Computes tumbling and sliding window analytics over event streams."""
    def __init__(self, window_size_seconds: int = 60):
        self.window_size_seconds = window_size_seconds

    def compute_metrics(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not events:
            return {"count": 0, "avg_latency_ms": 0.0, "p99_latency_ms": 0.0}
        latencies = [e.get("latency_ms", 0.0) for e in events]
        return {
            "count": len(events),
            "avg_latency_ms": sum(latencies) / len(latencies),
            "max_latency_ms": max(latencies),
            "min_latency_ms": min(latencies)
        }

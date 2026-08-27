import pytest
from streaming_platform.engine import EventConsumer, WindowAggregator

def test_event_consumption():
    consumer = EventConsumer("telemetry_events")
    batch = [{"id": 1, "latency_ms": 12.5}, {"id": 2, "latency_ms": 18.2}]
    count = consumer.consume_batch(batch)
    assert count == 2
    assert len(consumer.processed_events) == 2
    assert "ingestion_timestamp" in consumer.processed_events[0]

def test_window_metrics_aggregation():
    aggregator = WindowAggregator(window_size_seconds=60)
    events = [
        {"latency_ms": 10.0},
        {"latency_ms": 20.0},
        {"latency_ms": 30.0}
    ]
    metrics = aggregator.compute_metrics(events)
    assert metrics["count"] == 3
    assert metrics["avg_latency_ms"] == 20.0
    assert metrics["max_latency_ms"] == 30.0

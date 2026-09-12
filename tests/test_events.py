import pytest
from streaming_core.events import TelemetryEvent, TelemetryStreamGenerator


def test_telemetry_event_creation():
    evt = TelemetryEvent(
        event_id="evt_01",
        device_id="dev_01",
        metric_name="cpu_utilization_pct",
        value=55.4,
        unit="%",
    )
    assert evt.event_id == "evt_01"
    assert evt.value == 55.4
    d = evt.to_dict()
    assert d["metric_name"] == "cpu_utilization_pct"


def test_telemetry_event_validation():
    with pytest.raises(ValueError):
        TelemetryEvent("", "dev_01", "cpu", 50.0)
    with pytest.raises(ValueError):
        TelemetryEvent("evt", "   ", "cpu", 50.0)
    with pytest.raises(ValueError):
        TelemetryEvent("evt", "dev", "", 50.0)


def test_stream_generator():
    gen = TelemetryStreamGenerator(seed=99)
    batch = gen.generate_batch(count=30, anomaly_rate=0.1)
    assert len(batch) == 30
    assert all(isinstance(e, TelemetryEvent) for e in batch)

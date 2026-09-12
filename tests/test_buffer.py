import pytest
from streaming_core.buffer import StreamRingBuffer
from streaming_core.events import TelemetryEvent


def test_ring_buffer_push_and_drain():
    buf = StreamRingBuffer(capacity=5)
    for i in range(5):
        buf.push(TelemetryEvent(f"e_{i}", "d1", "cpu", float(i)))

    assert buf.size == 5
    drained = buf.drain(max_items=3)
    assert len(drained) == 3
    assert drained[0].event_id == "e_0"
    assert buf.size == 2


def test_ring_buffer_drop_oldest():
    buf = StreamRingBuffer(capacity=3, drop_oldest=True)
    for i in range(5):
        buf.push(TelemetryEvent(f"e_{i}", "d1", "cpu", float(i)))

    assert buf.size == 3
    metrics = buf.metrics
    assert metrics["total_pushed"] == 5
    assert metrics["total_dropped"] == 2
    # The remaining should be e_2, e_3, e_4
    items = buf.drain()
    assert items[0].event_id == "e_2"
    assert items[-1].event_id == "e_4"


def test_ring_buffer_reject():
    buf = StreamRingBuffer(capacity=2, drop_oldest=False)
    assert buf.push(TelemetryEvent("e_1", "d1", "cpu", 10.0)) is True
    assert buf.push(TelemetryEvent("e_2", "d1", "cpu", 20.0)) is True
    assert buf.push(TelemetryEvent("e_3", "d1", "cpu", 30.0)) is False
    assert buf.size == 2

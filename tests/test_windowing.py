from streaming_core.events import TelemetryEvent
from streaming_core.windowing import SlidingWindowAggregator, TumblingWindowAggregator


def test_tumbling_window_aggregator():
    agg = TumblingWindowAggregator(window_seconds=10.0)

    # Window 0-10
    agg.add_event(TelemetryEvent("e1", "d1", "latency", 20.0, timestamp=2.0))
    agg.add_event(TelemetryEvent("e2", "d1", "latency", 40.0, timestamp=5.0))
    # Window 10-20
    agg.add_event(TelemetryEvent("e3", "d1", "latency", 100.0, timestamp=15.0))

    summaries = agg.get_summaries(close_before=12.0)
    assert len(summaries) == 1
    assert summaries[0].window_start == 0.0
    assert summaries[0].count == 2
    assert summaries[0].mean == 30.0
    assert summaries[0].min_val == 20.0
    assert summaries[0].max_val == 40.0


def test_sliding_window_aggregator():
    agg = SlidingWindowAggregator(window_seconds=5.0)

    agg.add_event(TelemetryEvent("e1", "d1", "cpu", 50.0, timestamp=100.0))
    agg.add_event(TelemetryEvent("e2", "d1", "cpu", 70.0, timestamp=102.0))
    agg.add_event(TelemetryEvent("e3", "d1", "cpu", 90.0, timestamp=104.0))

    # Stats at t=104.5 (all 3 unexpired)
    stats = agg.current_stats(current_time=104.5)
    assert stats["cpu"]["count"] == 3
    assert stats["cpu"]["mean"] == 70.0

    # Stats at t=106.0 (e1 at 100.0 is expired since 106.0 - 5.0 = 101.0 > 100.0)
    stats_later = agg.current_stats(current_time=106.0)
    assert stats_later["cpu"]["count"] == 2
    assert stats_later["cpu"]["mean"] == 80.0

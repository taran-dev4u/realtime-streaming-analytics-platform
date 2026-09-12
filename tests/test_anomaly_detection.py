from streaming_core.anomaly_detection import StreamingAnomalyDetector, WelfordStats
from streaming_core.events import TelemetryEvent


def test_welford_stats():
    stats = WelfordStats()
    values = [10.0, 20.0, 30.0, 40.0, 50.0]
    for v in values:
        stats.update(v)

    assert stats.count == 5
    assert stats.mean == 30.0
    # Sample variance of [10, 20, 30, 40, 50] = 250.0
    assert abs(stats.variance - 250.0) < 1e-4
    assert abs(stats.std_dev - 15.811) < 1e-2


def test_streaming_anomaly_detector():
    detector = StreamingAnomalyDetector(z_threshold=3.0, min_samples=10)

    # Feed 20 nominal values around 50.0
    for i in range(20):
        val = 50.0 + (i % 3) - 1.0  # 49, 50, 51
        evt = TelemetryEvent(f"nom_{i}", "dev_1", "cpu", val)
        alert = detector.process_event(evt)
        assert alert is None

    # Feed extreme outlier: 200.0 (z-score will exceed 3.0)
    outlier = TelemetryEvent("spike", "dev_1", "cpu", 200.0)
    alert = detector.process_event(outlier)

    assert alert is not None
    assert alert.event_id == "spike"
    assert alert.observed_value == 200.0
    assert alert.z_score > 3.0

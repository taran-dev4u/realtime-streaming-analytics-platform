"""Real-time streaming telemetry and analytics core."""

from .anomaly_detection import StreamingAnomalyDetector, WelfordStats
from .buffer import StreamRingBuffer
from .events import TelemetryEvent, TelemetryStreamGenerator
from .windowing import SlidingWindowAggregator, TumblingWindowAggregator, WindowSummary

__all__ = [
    "TelemetryEvent",
    "TelemetryStreamGenerator",
    "StreamRingBuffer",
    "TumblingWindowAggregator",
    "SlidingWindowAggregator",
    "WindowSummary",
    "StreamingAnomalyDetector",
    "WelfordStats",
]

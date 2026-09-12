from dataclasses import asdict, dataclass
import math
import time
from typing import Dict, List, Optional, Tuple
import numpy as np

from .events import TelemetryEvent


@dataclass(frozen=True)
class WindowSummary:
    window_start: float
    window_end: float
    metric_name: str
    count: int
    mean: float
    min_val: float
    max_val: float
    std_dev: float
    p95: float

    def to_dict(self) -> dict:
        return asdict(self)


class TumblingWindowAggregator:
    """Aggregates streaming events into discrete non-overlapping fixed-duration time buckets."""

    def __init__(self, window_seconds: float = 10.0):
        if window_seconds <= 0:
            raise ValueError("window_seconds must be strictly positive")
        self.window_seconds = window_seconds
        self._buckets: Dict[Tuple[float, str], List[float]] = {}

    def _get_window_start(self, timestamp: float) -> float:
        return math.floor(timestamp / self.window_seconds) * self.window_seconds

    def add_event(self, event: TelemetryEvent) -> None:
        win_start = self._get_window_start(event.timestamp)
        key = (win_start, event.metric_name)
        if key not in self._buckets:
            self._buckets[key] = []
        self._buckets[key].append(event.value)

    def get_summaries(self, close_before: Optional[float] = None) -> List[WindowSummary]:
        summaries: List[WindowSummary] = []
        keys_to_delete = []

        for (win_start, metric), values in sorted(self._buckets.items()):
            win_end = win_start + self.window_seconds
            if close_before is not None and win_end > close_before:
                continue

            arr = np.array(values, dtype=np.float64)
            summaries.append(
                WindowSummary(
                    window_start=win_start,
                    window_end=win_end,
                    metric_name=metric,
                    count=len(values),
                    mean=round(float(np.mean(arr)), 2),
                    min_val=round(float(np.min(arr)), 2),
                    max_val=round(float(np.max(arr)), 2),
                    std_dev=round(float(np.std(arr)), 2) if len(values) > 1 else 0.0,
                    p95=round(float(np.percentile(arr, 95)), 2),
                )
            )
            if close_before is not None:
                keys_to_delete.append((win_start, metric))

        for k in keys_to_delete:
            del self._buckets[k]

        return summaries


class SlidingWindowAggregator:
    """Sliding time-window aggregator that continually evicts expired events."""

    def __init__(self, window_seconds: float = 60.0):
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self.window_seconds = window_seconds
        self._events: Dict[str, List[Tuple[float, float]]] = {}  # metric -> list of (timestamp, value)

    def add_event(self, event: TelemetryEvent) -> None:
        if event.metric_name not in self._events:
            self._events[event.metric_name] = []
        self._events[event.metric_name].append((event.timestamp, event.value))

    def evict_expired(self, current_time: Optional[float] = None) -> int:
        now = current_time if current_time is not None else time.time()
        cutoff = now - self.window_seconds
        total_evicted = 0

        for metric in list(self._events.keys()):
            unexpired = [(t, v) for t, v in self._events[metric] if t >= cutoff]
            total_evicted += len(self._events[metric]) - len(unexpired)
            if unexpired:
                self._events[metric] = unexpired
            else:
                del self._events[metric]

        return total_evicted

    def current_stats(self, current_time: Optional[float] = None) -> Dict[str, dict]:
        now = current_time if current_time is not None else time.time()
        self.evict_expired(now)
        stats = {}

        for metric, pairs in self._events.items():
            vals = [v for _, v in pairs]
            arr = np.array(vals, dtype=np.float64)
            stats[metric] = {
                "count": len(vals),
                "mean": round(float(np.mean(arr)), 2),
                "min": round(float(np.min(arr)), 2),
                "max": round(float(np.max(arr)), 2),
                "std": round(float(np.std(arr)), 2) if len(vals) > 1 else 0.0,
            }

        return stats

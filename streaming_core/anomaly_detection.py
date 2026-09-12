from dataclasses import dataclass
import math
from typing import Dict, List, Optional, Tuple

from .events import TelemetryEvent


@dataclass
class WelfordStats:
    """Maintains running mean, sample variance, and sample count in O(1) space."""

    count: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def update(self, x: float) -> None:
        self.count += 1
        delta = x - self.mean
        self.mean += delta / self.count
        delta2 = x - self.mean
        self.m2 += delta * delta2

    @property
    def variance(self) -> float:
        if self.count < 2:
            return 0.0
        return self.m2 / (self.count - 1)

    @property
    def std_dev(self) -> float:
        return math.sqrt(self.variance)


@dataclass(frozen=True)
class AnomalyAlert:
    event_id: str
    device_id: str
    metric_name: str
    observed_value: float
    running_mean: float
    running_std: float
    z_score: float
    timestamp: float


class StreamingAnomalyDetector:
    """Online streaming statistical outlier detector using Welford's single-pass algorithm."""

    def __init__(self, z_threshold: float = 3.0, min_samples: int = 10):
        if z_threshold <= 0:
            raise ValueError("z_threshold must be strictly positive")
        if min_samples < 2:
            raise ValueError("min_samples must be at least 2")
        self.z_threshold = z_threshold
        self.min_samples = min_samples
        self._stats: Dict[Tuple[str, str], WelfordStats] = {}
        self._recent_alerts: List[AnomalyAlert] = []

    def process_event(self, event: TelemetryEvent) -> Optional[AnomalyAlert]:
        key = (event.device_id, event.metric_name)
        if key not in self._stats:
            self._stats[key] = WelfordStats()

        stats = self._stats[key]
        alert: Optional[AnomalyAlert] = None

        if stats.count >= self.min_samples and stats.std_dev > 1e-6:
            z = (event.value - stats.mean) / stats.std_dev
            if abs(z) >= self.z_threshold:
                alert = AnomalyAlert(
                    event_id=event.event_id,
                    device_id=event.device_id,
                    metric_name=event.metric_name,
                    observed_value=event.value,
                    running_mean=round(stats.mean, 2),
                    running_std=round(stats.std_dev, 2),
                    z_score=round(z, 2),
                    timestamp=event.timestamp,
                )
                self._recent_alerts.append(alert)
                if len(self._recent_alerts) > 100:
                    self._recent_alerts.pop(0)

        # Update stats
        stats.update(event.value)
        return alert

    def get_recent_alerts(self, limit: int = 20) -> List[AnomalyAlert]:
        return self._recent_alerts[-limit:]

    def clear(self) -> None:
        self._stats.clear()
        self._recent_alerts.clear()

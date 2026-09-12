from dataclasses import asdict, dataclass, field
import time
from typing import Any, Dict, List, Optional
import numpy as np


@dataclass(frozen=True)
class TelemetryEvent:
    event_id: str
    device_id: str
    metric_name: str
    value: float
    timestamp: float = field(default_factory=time.time)
    unit: str = "ms"
    tags: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.event_id.strip():
            raise ValueError("event_id must not be empty")
        if not self.device_id.strip():
            raise ValueError("device_id must not be empty")
        if not self.metric_name.strip():
            raise ValueError("metric_name must not be empty")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class TelemetryStreamGenerator:
    """Generates synthetic high-throughput metric streams with optional anomaly injection."""

    def __init__(self, device_ids: Optional[List[str]] = None, seed: int = 42):
        self.device_ids = device_ids or ["edge_node_01", "edge_node_02", "edge_node_03", "gateway_us_east"]
        self.rng = np.random.default_rng(seed)
        self._seq = 0

    def next_event(self, inject_anomaly: bool = False) -> TelemetryEvent:
        self._seq += 1
        dev = str(self.rng.choice(self.device_ids))
        metric = str(self.rng.choice(["cpu_utilization_pct", "request_latency_ms", "memory_used_mb", "packet_loss_rate"]))

        # Base nominal values
        if metric == "cpu_utilization_pct":
            base = float(self.rng.normal(45.0, 5.0))
            val = base * 2.5 if inject_anomaly else base
            unit = "%"
        elif metric == "request_latency_ms":
            base = float(self.rng.exponential(25.0) + 10.0)
            val = base * 4.0 if inject_anomaly else base
            unit = "ms"
        elif metric == "memory_used_mb":
            base = float(self.rng.normal(2048.0, 100.0))
            val = base + 1500.0 if inject_anomaly else base
            unit = "MB"
        else:
            base = float(max(0.0, self.rng.normal(0.01, 0.005)))
            val = 0.25 if inject_anomaly else base
            unit = "ratio"

        return TelemetryEvent(
            event_id=f"evt_{self._seq:07d}",
            device_id=dev,
            metric_name=metric,
            value=round(float(val), 2),
            timestamp=time.time(),
            unit=unit,
            tags={"anomaly": inject_anomaly, "env": "prod"},
        )

    def generate_batch(self, count: int = 50, anomaly_rate: float = 0.05) -> List[TelemetryEvent]:
        events: List[TelemetryEvent] = []
        for _ in range(count):
            is_anomaly = bool(self.rng.random() < anomaly_rate)
            events.append(self.next_event(inject_anomaly=is_anomaly))
        return events

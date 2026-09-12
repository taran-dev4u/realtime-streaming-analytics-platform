from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class IngestEventItem(BaseModel):
    event_id: str
    device_id: str
    metric_name: str
    value: float
    unit: Optional[str] = "ms"
    tags: Optional[Dict[str, Any]] = Field(default_factory=dict)


class BatchIngestRequest(BaseModel):
    events: List[IngestEventItem]


class IngestResponse(BaseModel):
    status: str
    events_received: int
    anomalies_detected: int
    buffer_size: int


class MetricStat(BaseModel):
    count: int
    mean: float
    min: float
    max: float
    std: float


class HealthMetricsResponse(BaseModel):
    status: str
    uptime_seconds: int
    total_events_processed: int
    buffer_metrics: dict
    active_anomaly_count: int

from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)


def test_api_root():
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.json()["status"] == "online"


def test_api_health():
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert "buffer_metrics" in data


def test_api_ingest_and_window():
    payload = {
        "events": [
            {"event_id": "e_1", "device_id": "d1", "metric_name": "latency", "value": 25.0},
            {"event_id": "e_2", "device_id": "d1", "metric_name": "latency", "value": 35.0},
        ]
    }
    resp = client.post("/events", json=payload)
    assert resp.status_code == 200
    assert resp.json()["events_received"] == 2

    # Check window metrics
    win_resp = client.get("/metrics/window")
    assert win_resp.status_code == 200
    metrics = win_resp.json()["metrics"]
    assert "latency" in metrics
    assert metrics["latency"]["count"] >= 2


def test_api_simulate():
    resp = client.post("/simulate?count=50&anomaly_rate=0.1")
    assert resp.status_code == 200
    data = resp.json()
    assert data["events_received"] == 50

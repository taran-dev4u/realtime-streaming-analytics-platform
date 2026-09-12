from collections import deque
from threading import Lock
from typing import List, Optional

from .events import TelemetryEvent


class StreamRingBuffer:
    """Bounded, thread-safe in-memory ring buffer with configurable drop policies."""

    def __init__(self, capacity: int = 5000, drop_oldest: bool = True):
        if capacity <= 0:
            raise ValueError("Buffer capacity must be strictly positive")
        self.capacity = capacity
        self.drop_oldest = drop_oldest
        self._deque: deque = deque(maxlen=capacity if drop_oldest else None)
        self._lock = Lock()
        self._total_pushed = 0
        self._total_dropped = 0

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._deque)

    @property
    def metrics(self) -> dict:
        with self._lock:
            return {
                "size": len(self._deque),
                "capacity": self.capacity,
                "total_pushed": self._total_pushed,
                "total_dropped": self._total_dropped,
            }

    def push(self, event: TelemetryEvent) -> bool:
        with self._lock:
            if not self.drop_oldest and len(self._deque) >= self.capacity:
                self._total_dropped += 1
                return False

            if self.drop_oldest and len(self._deque) == self.capacity:
                self._total_dropped += 1

            self._deque.append(event)
            self._total_pushed += 1
            return True

    def drain(self, max_items: Optional[int] = None) -> List[TelemetryEvent]:
        with self._lock:
            limit = max_items if max_items is not None else len(self._deque)
            items: List[TelemetryEvent] = []
            for _ in range(min(limit, len(self._deque))):
                items.append(self._deque.popleft())
            return items

    def clear(self) -> None:
        with self._lock:
            self._deque.clear()

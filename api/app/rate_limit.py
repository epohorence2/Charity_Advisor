from __future__ import annotations

from collections import defaultdict, deque
from threading import Lock
from time import monotonic


class RateLimiter:
  def __init__(self, max_requests: int, window_seconds: int = 60) -> None:
    self.max_requests = max_requests
    self.window_seconds = window_seconds
    self._hits: dict[str, deque[float]] = defaultdict(deque)
    self._lock = Lock()

  def hit(self, token: str) -> None:
    now = monotonic()
    with self._lock:
      bucket = self._hits[token]
      while bucket and now - bucket[0] > self.window_seconds:
        bucket.popleft()
      if len(bucket) >= self.max_requests:
        raise RateLimitExceeded(self.max_requests, self.window_seconds)
      bucket.append(now)


class RateLimitExceeded(Exception):
  def __init__(self, max_requests: int, window_seconds: int) -> None:
    super().__init__(f'Rate limit exceeded: {max_requests} requests per {window_seconds} seconds')
    self.max_requests = max_requests
    self.window_seconds = window_seconds

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from itsdangerous import BadSignature, BadTimeSignature, URLSafeTimedSerializer

from .config import settings

_serializer = URLSafeTimedSerializer(settings.secret_key, salt='charity-recommender-cursor')


def encode_cursor(payload: dict[str, Any]) -> str:
  enriched = {**payload, 'issued_at': datetime.now(timezone.utc).isoformat()}
  return _serializer.dumps(enriched)


def decode_cursor(token: str) -> dict[str, Any] | None:
  try:
    return _serializer.loads(token, max_age=settings.cursor_ttl_seconds)
  except BadTimeSignature:
    return None
  except BadSignature as exc:  # includes expired signature with tampering
    raise ValueError('Invalid cursor signature') from exc

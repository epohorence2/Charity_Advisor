from __future__ import annotations

import hashlib
import json
import random
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware

from .config import settings
from .cursor import decode_cursor, encode_cursor
from .data import CHARITY_POOL
from .models import (
  DailyPickResponse,
  RecommendRequest,
  RecommendResponse,
  StatusResponse,
)
from .rate_limit import RateLimitExceeded, RateLimiter

ISSUE_TO_NTEE = {
  'health': 'E70',
  'education': 'B82',
  'environment': 'C32',
  'human_services': 'K30',
  'arts': 'A20',
  'international': 'Q35',
  'animals': 'D30',
  'other': 'Z99',
}

VERSION = None


def _detect_version() -> str:
  global VERSION
  if VERSION:
    return VERSION
  try:
    result = subprocess.run(
      ['git', 'rev-parse', '--short', 'HEAD'],
      capture_output=True,
      check=True,
      text=True,
    )
    VERSION = result.stdout.strip() or 'dev'
  except Exception:
    VERSION = datetime.now(timezone.utc).strftime('%Y%m%d')
  return VERSION


def build_app() -> FastAPI:
  limiter = RateLimiter(settings.rate_limit_per_minute)

  app = FastAPI(
    title='Charity Recommender API',
    version=_detect_version(),
    docs_url='/docs' if settings.app_env != 'production' else None,
  )

  allowed_origins = [origin.strip() for origin in settings.cors_allow_origin.split(',') if origin.strip()]
  app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins or ['*'],
    allow_methods=['GET', 'POST'],
    allow_headers=['Content-Type'],
    allow_credentials=False,
  )

  async def enforce_rate_limit(request: Request) -> None:
    client_ip = request.client.host if request.client else 'anonymous'
    try:
      limiter.hit(client_ip)
    except RateLimitExceeded as exc:
      raise HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail={'message': 'Too many requests, please slow down.', 'limit': exc.max_requests},
      ) from exc

  @app.middleware('http')
  async def add_env_headers(request: Request, call_next):  # type: ignore[override]
    response = await call_next(request)
    response.headers['x-app-env'] = settings.app_env
    return response

  @app.get('/api/status', response_model=StatusResponse)
  async def status() -> StatusResponse:
    return StatusResponse(ok=True, version=app.version or 'dev', env=settings.app_env)

  @app.get('/api/daily-picks', response_model=DailyPickResponse, dependencies=[Depends(enforce_rate_limit)])
  async def daily_picks(limit: int = Query(default=3, ge=1, le=12)) -> DailyPickResponse:
    charities = select_daily_charities(limit)
    return DailyPickResponse(charities=charities)

  @app.post('/api/recommend', response_model=RecommendResponse, dependencies=[Depends(enforce_rate_limit)])
  async def recommend(payload: RecommendRequest) -> RecommendResponse:
    if not payload.answers:
      raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='answers are required')

    answer_map = {answer.question_id: answer.value for answer in payload.answers}
    issue_family = answer_map.get('q_issue_family')
    impact_mode = answer_map.get('q_impact_mode')
    geography = answer_map.get('q_geography')
    location = normalize_location(answer_map.get('q_location'))
    topics = ensure_list(answer_map.get('q_topics'))
    limit = payload.limit

    query_signature = build_query_signature(issue_family, impact_mode, geography, location, topics)
    page_index = 0

    if payload.cursor:
      decoded = None
      try:
        decoded = decode_cursor(payload.cursor)
      except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

      if decoded is None:
        return RecommendResponse(charities=[], cursor=None, explain=build_explain(issue_family, impact_mode, geography, location, topics, expired=True))

      if decoded.get('signature') != query_signature:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='Cursor does not match current answers.')

      page_index = int(decoded.get('page', 0))
      limit = int(decoded.get('page_size', limit))

    suggested = filter_charities(issue_family, impact_mode, geography, location, topics)
    total = len(suggested)
    start = page_index * limit
    end = start + limit
    charities_slice = suggested[start:end]

    next_cursor = None
    if end < total:
      next_cursor = encode_cursor({'page': page_index + 1, 'page_size': limit, 'signature': query_signature})

    explain = build_explain(issue_family, impact_mode, geography, location, topics)
    return RecommendResponse(charities=charities_slice, cursor=next_cursor, explain=explain)

  return app


def ensure_list(value: Any) -> List[str]:
  if value is None:
    return []
  if isinstance(value, list):
    return [str(item) for item in value]
  return [str(value)]


def normalize_location(value: Any) -> str | None:
  if not value:
    return None
  value_str = str(value).strip()
  return value_str or None


def build_query_signature(issue: Any, impact: Any, geography: Any, location: Any, topics: Sequence[str]) -> str:
  payload = {
    'issue': issue,
    'impact': impact,
    'geography': geography,
    'location': location,
    'topics': sorted(topics),
  }
  encoded = json.dumps(payload, sort_keys=True)
  return hashlib.sha1(encoded.encode('utf-8')).hexdigest()


def filter_charities(
  issue_family: Any,
  impact_mode: Any,
  geography: Any,
  location: str | None,
  topics: Sequence[str],
):
  def matches(charity: Dict[str, Any]) -> bool:
    if issue_family and charity['issue_family'] != issue_family:
      return False
    if impact_mode and impact_mode not in charity['impact_modes']:
      return False
    if geography and geography not in charity['geographies']:
      return False
    return True

  pool = [charity for charity in CHARITY_POOL if matches(charity)]
  if not pool:
    pool = CHARITY_POOL.copy()

  def score(charity: Dict[str, Any]) -> tuple[int, str]:
    score_value = 0
    if location and location.lower() in charity['location'].lower():
      score_value -= 10
    if topics:
      overlap = len(set(topics) & set(charity['topics']))
      score_value -= overlap
    return (score_value, charity['name'])

  return sorted(pool, key=score)


def build_explain(issue, impact, geography, location, topics, expired: bool = False):
  rationale = []
  if issue:
    rationale.append(f'Issue focus: {issue}')
  if impact:
    rationale.append(f'Impact mode: {impact}')
  if geography:
    rationale.append(f'Geography preference: {geography}')
  if location:
    rationale.append(f'Location hint: {location}')
  if topics:
    rationale.append('Topics matched: ' + ', '.join(topics))
  if expired:
    rationale.append('Cursor expired. Please submit the survey again for fresh results.')
  code = ISSUE_TO_NTEE.get(issue, 'Z99')
  return {'ntee': code, 'rationale': rationale}


def select_daily_charities(limit: int):
  seed = datetime.utcnow().strftime('%Y-%m-%d') + settings.secret_key
  rng = random.Random(seed)
  pool = CHARITY_POOL.copy()
  rng.shuffle(pool)
  return pool[:limit]


app = build_app()


if __name__ == '__main__':
  import uvicorn

  uvicorn.run('app.main:app', host='0.0.0.0', port=8000, reload=True)

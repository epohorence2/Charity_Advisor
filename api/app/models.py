from __future__ import annotations

from typing import Any, List, Optional, Union

from pydantic import BaseModel, Field


class Answer(BaseModel):
  question_id: str
  value: Union[str, List[str]]


class RecommendRequest(BaseModel):
  answers: List[Answer]
  cursor: Optional[str] = None
  limit: int = Field(default=3, ge=1, le=12)


class Charity(BaseModel):
  ein: str
  name: str
  url: Optional[str] = None
  summary: Optional[str] = None
  location: Optional[str] = None
  ntee: Optional[str] = None


class Explain(BaseModel):
  ntee: Optional[str] = None
  rationale: List[str] = Field(default_factory=list)


class RecommendResponse(BaseModel):
  charities: List[Charity]
  cursor: Optional[str] = None
  explain: Explain


class DailyPickResponse(BaseModel):
  charities: List[Charity]


class StatusResponse(BaseModel):
  ok: bool
  version: str
  env: str


class ErrorResponse(BaseModel):
  detail: Any

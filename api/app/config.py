import os
from dataclasses import dataclass


@dataclass(slots=True)
class Settings:
  api_base: str = os.getenv('API_BASE', 'http://localhost:8000')
  everyorg_api_key: str | None = os.getenv('EVERYORG_API_KEY')
  cors_allow_origin: str = os.getenv('CORS_ALLOW_ORIGIN', 'http://localhost:4173')
  app_env: str = os.getenv('APP_ENV', 'development')
  secret_key: str = os.getenv('SECRET_KEY', 'dev-secret-key-change-me')
  rate_limit_per_minute: int = int(os.getenv('RATE_LIMIT_PER_MINUTE', '60'))
  cursor_ttl_seconds: int = int(os.getenv('CURSOR_TTL_SECONDS', '600'))


settings = Settings()

# app/config.py
# ============================================================
# LEAN-BRIDGE — CENTRALISED SETTINGS
# All env vars loaded and validated here once at startup
# ============================================================

from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Supabase
    supabase_url: str
    supabase_service_key: str

    # Security
    webhook_secret: str
    encryption_key: str

    # Redis / Celery
    redis_url: str = "redis://localhost:6379/0"
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_result_backend: str = "redis://localhost:6379/0"

    # Signal processing
    batch_size: int = 10
    max_retries: int = 3
    retry_countdown_seconds: int = 5
    signal_timestamp_tolerance_seconds: int = 30

    # MT5
    mt5_slippage_deviation: int = 20
    overleverage_threshold: float = 0.50

    # Heartbeat thresholds
    heartbeat_warning_seconds: int = 90
    heartbeat_offline_seconds: int = 180

    # App
    environment: str = "development"
    log_level: str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """
    Returns cached settings singleton.
    Called via FastAPI Depends(get_settings) or direct import.
    """
    return Settings()

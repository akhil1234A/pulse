"""Application configuration management using Pydantic Settings."""

from functools import lru_cache
from typing import Optional
from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    # Kafka
    kafka_bootstrap_servers: str = Field(
        default="localhost:9094",
        validation_alias="KAFKA_BOOTSTRAP_SERVERS",
        description="Kafka broker address(es)",
    )
    kafka_events_topic: str = Field(
        default="pulsestream.events",
        validation_alias="KAFKA_EVENTS_TOPIC",
        description="Primary topic for normalized events",
    )
    kafka_dlq_topic: str = Field(
        default="pulsestream.events.dlq",
        validation_alias="KAFKA_DLQ_TOPIC",
        description="Dead letter queue topic for rejected/failed events",
    )
    kafka_consumer_group: str = Field(
        default="ps-aggregation",
        validation_alias="KAFKA_CONSUMER_GROUP",
        description="Default Kafka consumer group id",
    )

    # Redis
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        validation_alias="REDIS_URL",
        description="Redis connection URL",
    )

    # PostgreSQL
    postgres_host: str = Field(default="localhost", validation_alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, validation_alias="POSTGRES_PORT")
    postgres_db: str = Field(default="pulsestream", validation_alias="POSTGRES_DB")
    postgres_user: str = Field(default="pulsestream", validation_alias="POSTGRES_USER")
    postgres_password: str = Field(default="pulsestream", validation_alias="POSTGRES_PASSWORD")
    postgres_dsn_override: Optional[str] = Field(default=None, validation_alias="POSTGRES_DSN")

    # Logging & Observability
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")
    environment: str = Field(default="development", validation_alias="ENVIRONMENT")
    metrics_port: int = Field(default=9100, validation_alias="METRICS_PORT")

    # Ingestion Service
    ingestion_wikipedia_enabled: bool = Field(
        default=True, validation_alias="INGESTION_WIKIPEDIA_ENABLED"
    )
    ingestion_wikipedia_url: str = Field(
        default="https://stream.wikimedia.org/v2/stream/recentchange",
        validation_alias="INGESTION_WIKIPEDIA_URL",
    )
    ingestion_simulator_enabled: bool = Field(
        default=True, validation_alias="INGESTION_SIMULATOR_ENABLED"
    )
    ingestion_simulator_mode: str = Field(
        default="steady", validation_alias="INGESTION_SIMULATOR_MODE"
    )
    ingestion_simulator_rate: int = Field(
        default=50, validation_alias="INGESTION_SIMULATOR_RATE"
    )
    ingestion_chaos_duplicate_pct: float = Field(
        default=0.05, validation_alias="INGESTION_CHAOS_DUPLICATE_PCT"
    )
    ingestion_chaos_malformed_pct: float = Field(
        default=0.02, validation_alias="INGESTION_CHAOS_MALFORMED_PCT"
    )

    # Worker Settings
    worker_batch_flush_size: int = Field(
        default=1000, validation_alias="WORKER_BATCH_FLUSH_SIZE"
    )
    worker_batch_flush_interval_seconds: float = Field(
        default=5.0, validation_alias="WORKER_BATCH_FLUSH_INTERVAL_SECONDS"
    )
    worker_poll_timeout_seconds: float = Field(
        default=1.0, validation_alias="WORKER_POLL_TIMEOUT_SECONDS"
    )
    worker_max_batch_size: int = Field(
        default=500, validation_alias="WORKER_MAX_BATCH_SIZE"
    )
    worker_health_port: int = Field(
        default=9101, validation_alias="WORKER_HEALTH_PORT"
    )

    @computed_field
    @property
    def postgres_dsn(self) -> str:
        if self.postgres_dsn_override:
            return self.postgres_dsn_override
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}@"
            f"{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    """Returns a cached instance of application settings."""
    return Settings()

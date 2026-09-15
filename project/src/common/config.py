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

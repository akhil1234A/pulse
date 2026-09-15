"""Unit tests for configuration module."""

import os
import pytest
from src.common.config import Settings, get_settings


def test_default_settings():
    settings = Settings()
    assert settings.kafka_bootstrap_servers in ["localhost:9094", "kafka:9092"]
    assert settings.kafka_events_topic == "pulsestream.events"
    assert settings.kafka_dlq_topic == "pulsestream.events.dlq"
    assert settings.kafka_consumer_group == "ps-aggregation"
    assert settings.redis_url.startswith("redis://")
    assert settings.postgres_port in [5432, 5433]
    assert "pulsestream" in settings.postgres_dsn


def test_computed_postgres_dsn():
    settings = Settings(
        postgres_host="dbhost",
        postgres_port=5433,
        postgres_db="testdb",
        postgres_user="testuser",
        postgres_password="secretpassword",
    )
    assert settings.postgres_dsn == "postgresql://testuser:secretpassword@dbhost:5433/testdb"


def test_postgres_dsn_override():
    custom_dsn = "postgresql://custom:custom@remote:5432/customdb"
    settings = Settings(postgres_dsn_override=custom_dsn)
    assert settings.postgres_dsn == custom_dsn


def test_cached_get_settings():
    s1 = get_settings()
    s2 = get_settings()
    assert s1 is s2

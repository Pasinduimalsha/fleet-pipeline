"""Central configuration, read once from environment variables.

Every service (simulators, streaming job, Airflow tasks, API) imports the
module-level ``settings`` singleton instead of reading ``os.environ``
directly, so connection details, topic names and the simulated-clock scale
factor have exactly one source of truth.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, str(default)))


@dataclass(frozen=True)
class Settings:
    # Simulated clock: wall-clock seconds that make up one simulated day.
    sim_day_seconds: int = field(default_factory=lambda: _env_int("SIM_DAY_SECONDS", 300))

    # Kafka
    kafka_bootstrap_servers: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    kafka_topic: str = field(default_factory=lambda: _env("KAFKA_TOPIC", "trips.telemetry"))
    kafka_dlq_topic: str = field(default_factory=lambda: _env("KAFKA_DLQ_TOPIC", "trips.dlq"))
    kafka_partitions: int = field(default_factory=lambda: _env_int("KAFKA_PARTITIONS", 3))

    # Postgres serving store
    db_host: str = field(default_factory=lambda: _env("FLEET_DB_HOST", "postgres"))
    db_port: int = field(default_factory=lambda: _env_int("FLEET_DB_PORT", 5432))
    db_name: str = field(default_factory=lambda: _env("FLEET_DB_NAME", "fleet"))
    db_user: str = field(default_factory=lambda: _env("FLEET_DB_USER", "fleet"))
    db_password: str = field(default_factory=lambda: _env("FLEET_DB_PASSWORD", "fleet_pw"))

    # Shared filesystem paths (bind-mounted volumes)
    lake_path: str = field(default_factory=lambda: _env("LAKE_PATH", "/data/lake"))
    landing_path: str = field(default_factory=lambda: _env("LANDING_PATH", "/data/landing"))
    checkpoints_path: str = field(default_factory=lambda: _env("CHECKPOINTS_PATH", "/data/checkpoints"))
    reports_path: str = field(default_factory=lambda: _env("REPORTS_PATH", "/reports"))

    # Fleet simulation parameters
    num_vehicles: int = field(default_factory=lambda: _env_int("NUM_VEHICLES", 40))
    event_interval_seconds: float = field(default_factory=lambda: _env_float("EVENT_INTERVAL_SECONDS", 3.0))
    idle_alert_minutes: float = field(default_factory=lambda: _env_float("IDLE_ALERT_MINUTES", 3.0))
    malformed_event_rate: float = field(default_factory=lambda: _env_float("MALFORMED_EVENT_RATE", 0.02))
    unprofitable_threshold: float = field(default_factory=lambda: _env_float("UNPROFITABLE_THRESHOLD", 0.0))

    # Metrics ports
    metrics_port_producer: int = field(default_factory=lambda: _env_int("METRICS_PORT_PRODUCER", 8001))
    metrics_port_streaming: int = field(default_factory=lambda: _env_int("METRICS_PORT_STREAMING", 8002))
    api_port: int = field(default_factory=lambda: _env_int("API_PORT", 8000))

    @property
    def db_dsn(self) -> str:
        return (
            f"host={self.db_host} port={self.db_port} dbname={self.db_name} "
            f"user={self.db_user} password={self.db_password}"
        )

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


settings = Settings()

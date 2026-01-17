"""
Configuration for Standardized Processor
=========================================
Centralizes all configuration values with environment variable support.
Enables flexible deployment across dev/staging/prod environments.
"""

import os
import logging
from dataclasses import dataclass, field
from typing import FrozenSet, Dict


# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
LOG_LEVEL = logging.INFO


def get_logger(name: str) -> logging.Logger:
    """
    Get a configured logger instance.
    
    Args:
        name: Logger name (typically __name__)
    
    Returns:
        Configured logging.Logger instance
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logger.addHandler(handler)
    logger.setLevel(LOG_LEVEL)
    return logger


# =============================================================================
# ENVIRONMENT VARIABLE HELPERS
# =============================================================================

def get_env(key: str, default: str = "") -> str:
    """Get environment variable with default."""
    return os.environ.get(key, default)


def get_env_bool(key: str, default: bool = False) -> bool:
    """Get boolean from environment variable."""
    val = os.environ.get(key, "").lower()
    if val in ("true", "1", "yes", "on"):
        return True
    if val in ("false", "0", "no", "off"):
        return False
    return default


def get_env_int(key: str, default: int = 0) -> int:
    """Get integer from environment variable."""
    try:
        return int(os.environ.get(key, default))
    except (ValueError, TypeError):
        return default


# =============================================================================
# CONFIG DATACLASS
# =============================================================================

@dataclass(frozen=True)
class Config:
    """
    Immutable configuration for the Standardized Processor.
    
    All values can be overridden via environment variables.
    Use Config.from_env() to create an instance from environment.
    
    Example:
        config = Config.from_env()
        config = Config.from_env(topic_name="custom_topic")
    """
    # Pipeline settings
    topic_name: str = "events"
    raw_database: str = "iceberg_raw_db"
    standardized_database: str = "iceberg_standardized_db"
    curated_database: str = "iceberg_curated_db"  # New: for typed/governed data
    checkpoint_table: str = "lean-ops-checkpoints"
    iceberg_bucket: str = "lean-ops-development-iceberg"
    
    # Feature flags
    enable_deep_flatten: bool = True
    enable_schema_evolution: bool = True
    enable_safe_cast: bool = True
    
    # Processing limits
    max_flatten_depth: int = 5
    max_new_columns_per_run: int = 50
    flatten_sample_size: int = 1000
    
    # Glue job settings
    glue_worker_count: int = 2
    glue_timeout_minutes: int = 60
    
    @classmethod
    def from_env(cls, **overrides) -> "Config":
        """
        Create Config from environment variables with optional overrides.
        
        Environment variables:
            TOPIC_NAME, RAW_DATABASE, CURATED_DATABASE, CHECKPOINT_TABLE,
            ICEBERG_BUCKET, ENABLE_DEEP_FLATTEN, ENABLE_SCHEMA_EVOLUTION,
            ENABLE_SAFE_CAST, MAX_FLATTEN_DEPTH, MAX_NEW_COLUMNS_PER_RUN,
            FLATTEN_SAMPLE_SIZE, GLUE_WORKER_COUNT, GLUE_TIMEOUT_MINUTES
        
        Args:
            **overrides: Values to override (e.g., topic_name="custom")
        
        Returns:
            Config instance
        """
        defaults = {
            "topic_name": get_env("TOPIC_NAME", "events"),
            "raw_database": get_env("RAW_DATABASE", "iceberg_raw_db"),
            "standardized_database": get_env("STANDARDIZED_DATABASE", "iceberg_standardized_db"),
            "curated_database": get_env("CURATED_DATABASE", "iceberg_curated_db"),
            "checkpoint_table": get_env("CHECKPOINT_TABLE", "lean-ops-checkpoints"),
            "iceberg_bucket": get_env("ICEBERG_BUCKET", "lean-ops-development-iceberg"),
            "enable_deep_flatten": get_env_bool("ENABLE_DEEP_FLATTEN", True),
            "enable_schema_evolution": get_env_bool("ENABLE_SCHEMA_EVOLUTION", True),
            "enable_safe_cast": get_env_bool("ENABLE_SAFE_CAST", True),
            "max_flatten_depth": get_env_int("MAX_FLATTEN_DEPTH", 5),
            "max_new_columns_per_run": get_env_int("MAX_NEW_COLUMNS_PER_RUN", 50),
            "flatten_sample_size": get_env_int("FLATTEN_SAMPLE_SIZE", 1000),
            "glue_worker_count": get_env_int("GLUE_WORKER_COUNT", 2),
            "glue_timeout_minutes": get_env_int("GLUE_TIMEOUT_MINUTES", 60),
        }
        defaults.update(overrides)
        return cls(**defaults)
    
    @property
    def iceberg_warehouse(self) -> str:
        """S3 warehouse path for Iceberg."""
        return f"s3://{self.iceberg_bucket}/"
    
    @property
    def raw_table(self) -> str:
        """Fully qualified RAW table name."""
        return f"glue_catalog.{self.raw_database}.{self.topic_name}_staging"
    
    @property
    def standardized_table(self) -> str:
        """Fully qualified Standardized table name."""
        return f"glue_catalog.{self.standardized_database}.events"
    
    @property
    def curated_table(self) -> str:
        """Fully qualified Curated table name (typed/governed)."""
        return f"glue_catalog.{self.curated_database}.events"


# =============================================================================
# SCHEMA EVOLUTION CONSTANTS
# =============================================================================

# Envelope column variants to exclude from flatten and schema evolution
# These prevent conflicts with existing envelope columns in the RAW table
ENVELOPE_COLUMN_VARIANTS: FrozenSet[str] = frozenset({
    'idempotencykey', 'idempotency_key',
    'messageid', 'message_id',
    'publishtime', 'publish_time',
    'topicname', 'topic_name',
    'correlationid', 'correlation_id',
    'periodreference', 'period_reference',
    'ingestionts', 'ingestion_ts',
    'json_payload',
})

# Columns that should preserve their original type (not cast to STRING)
PRESERVE_TYPE_COLUMNS: FrozenSet[str] = frozenset({
    'ingestion_ts',  # Keep as BIGINT for timestamp ordering
})

# Legacy constants for backward compatibility (deprecated, use Config instead)
MAX_NEW_COLUMNS_PER_RUN = 50
MAX_FLATTEN_DEPTH = 5
FLATTEN_SAMPLE_SIZE = 1000


# =============================================================================
# ICEBERG CONFIGURATION
# =============================================================================

# Iceberg catalog settings for Glue Spark
ICEBERG_CATALOG_SETTINGS: Dict[str, str] = {
    "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
}


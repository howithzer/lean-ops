"""
Configuration Constants for Curated Processor
==============================================
Centralizes all configuration values for easier maintenance.
"""

import logging

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
# SCHEMA EVOLUTION CONFIGURATION
# =============================================================================

# Envelope column variants to exclude from flatten and schema evolution
# These prevent conflicts with existing envelope columns in the RAW table
ENVELOPE_COLUMN_VARIANTS = frozenset({
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
PRESERVE_TYPE_COLUMNS = frozenset({
    'ingestion_ts',  # Keep as BIGINT for timestamp ordering
})

# Maximum number of new columns to add per run (safety limit)
MAX_NEW_COLUMNS_PER_RUN = 50

# Maximum depth for recursive JSON flattening
MAX_FLATTEN_DEPTH = 5

# Sample size for key discovery during flattening
FLATTEN_SAMPLE_SIZE = 1000


# =============================================================================
# ICEBERG CONFIGURATION
# =============================================================================

# Iceberg catalog settings for Glue Spark
ICEBERG_CATALOG_SETTINGS = {
    "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
}

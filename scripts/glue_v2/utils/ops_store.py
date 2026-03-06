"""
Operational Data Store Helpers
================================
Read/write utilities for the `operational_data_store` Iceberg database.
Used by the compaction job and any other operational scripts that need to
record or query pipeline state.

Tables managed here:
    - table_inventory   : One row per subscription. Source of truth for what gets compacted.
    - maintenance_log   : One row per table per compaction run.
"""

from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
import logging

logger = logging.getLogger(__name__)

OPS_DB = "operational_data_store"
CATALOG = "glue_catalog"

TABLE_INVENTORY   = f"{CATALOG}.{OPS_DB}.table_inventory"
MAINTENANCE_LOG   = f"{CATALOG}.{OPS_DB}.maintenance_log"


# ---------------------------------------------------------------------------
# Table Inventory
# ---------------------------------------------------------------------------

def get_active_inventory(spark: SparkSession) -> list:
    """
    Returns all active subscription rows from table_inventory as a list of Row objects.
    Only rows where is_active = true are returned.

    Each Row has:
        subscription_name, topic_name,
        raw_database, raw_table,
        standardized_database, standardized_table,
        curated_database, curated_table
    """
    logger.info("Reading active inventory from %s", TABLE_INVENTORY)
    df = spark.table(TABLE_INVENTORY).filter(F.col("is_active") == True)
    rows = df.collect()
    logger.info("Found %d active subscription(s) in inventory.", len(rows))
    return rows


def register_subscription(spark: SparkSession, subscription_name: str, topic_name: str,
                          raw_db: str, raw_tbl: str,
                          std_db: str, std_tbl: str,
                          cur_db: str, cur_tbl: str):
    """
    Inserts or updates a subscription entry in table_inventory.
    Sets auto_ingested_on on first insert; does not overwrite intake_completed_on.
    Called by the discovery pipeline when it first detects a new subscription.
    """
    now = datetime.now(timezone.utc).isoformat()

    spark.sql(f"""
        MERGE INTO {TABLE_INVENTORY} t
        USING (
            SELECT
                '{subscription_name}'  AS subscription_name,
                '{topic_name}'         AS topic_name,
                '{raw_db}'             AS raw_database,
                '{raw_tbl}'            AS raw_table,
                '{std_db}'             AS standardized_database,
                '{std_tbl}'            AS standardized_table,
                '{cur_db}'             AS curated_database,
                '{cur_tbl}'            AS curated_table,
                true                   AS is_active,
                TIMESTAMP '{now}'      AS auto_ingested_on,
                CAST(NULL AS TIMESTAMP) AS intake_completed_on
        ) s ON t.subscription_name = s.subscription_name
        WHEN NOT MATCHED THEN INSERT *
    """)
    logger.info("Registered subscription '%s' in table_inventory.", subscription_name)


# ---------------------------------------------------------------------------
# Maintenance Log
# ---------------------------------------------------------------------------

def write_maintenance_entry(spark: SparkSession,
                            run_id: str,
                            database_name: str,
                            table_name: str,
                            layer: str,
                            partition_date: str,
                            files_before: int,
                            files_after: int,
                            status: str,
                            duration_ms: int,
                            error_message: str = None):
    """
    Appends a single compaction result row to maintenance_log.
    Called once per table per compaction run.
    """
    entry = Row(
        run_id=run_id,
        run_ts=datetime.now(timezone.utc).isoformat(),
        database_name=database_name,
        table_name=table_name,
        layer=layer,
        partition_date=partition_date,
        files_before=files_before,
        files_after=files_after,
        status=status,
        error_message=error_message or "",
        duration_ms=duration_ms,
    )
    df = spark.createDataFrame([entry])
    df.writeTo(MAINTENANCE_LOG).append()
    logger.info("[%s] %s.%s → status=%s, files %d→%d (%dms)",
                layer, database_name, table_name, status, files_before, files_after, duration_ms)

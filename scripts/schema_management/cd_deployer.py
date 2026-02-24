"""
cd_deployer.py — CD Iceberg Deployer
======================================
Runs in the ADO CD pipeline after a PR merges to main.
Authenticates to AWS via OIDC — no static credentials.

Responsibilities:
  1. Re-classify the operation from metadata.json + Glue state
     (guards against race conditions between CI and CD)
  2. Execute the appropriate DDL against Glue Catalog via Athena
  3. Upload metadata.json to S3 config bucket for the Enforcer
  4. Trigger Step Functions if nuclear_reset=true

DDL is executed via Athena because:
  - Athena is the standard SQL interface for Iceberg on Glue Catalog
  - No Spark cluster required in a CI/CD pipeline
  - Athena DDL is async — we poll for completion with backoff

Type mapping: OpenAPI 3.0 → Iceberg DDL (Athena/Trino syntax)

Required environment variables (set by ADO pipeline from variable groups):
  AWS_REGION
  ATHENA_DATABASE          e.g. standardized
  ATHENA_WORKGROUP         e.g. schema-mgmt
  ATHENA_OUTPUT_S3         e.g. s3://athena-results/schema-mgmt/
  ICEBERG_TABLE_S3_BASE    e.g. s3://datalake/standardized/
  CONFIG_BUCKET            e.g. config-bucket
  REBUILDER_SFN_ARN        e.g. arn:aws:states:...:stateMachine:HistoricalRebuilder

Usage (ADO pipeline step):
  python cd_deployer.py \
    --swagger  $(Build.SourcesDirectory)/schemas/topic_trades_v2/swagger.yaml \
    --metadata $(Build.SourcesDirectory)/schemas/topic_trades_v2/metadata.json
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import boto3
import yaml
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("cd_deployer")

# ---------------------------------------------------------------------------
# Environment config — fail fast if any required variable is absent
# ---------------------------------------------------------------------------
_REQUIRED_ENV_VARS = [
    "AWS_REGION",
    "ATHENA_DATABASE",
    "ATHENA_WORKGROUP",
    "ATHENA_OUTPUT_S3",
    "ICEBERG_TABLE_S3_BASE",
    "CONFIG_BUCKET",
    "REBUILDER_SFN_ARN",
    "PIPELINE_REGISTRY_TABLE",
]

def _load_env() -> dict:
    missing = [v for v in _REQUIRED_ENV_VARS if not os.environ.get(v)]
    if missing:
        log.error(
            "Missing required environment variables: %s\n"
            "Set these in the ADO variable group linked to this pipeline.",
            missing,
        )
        sys.exit(2)
    return {v: os.environ[v] for v in _REQUIRED_ENV_VARS}


# ---------------------------------------------------------------------------
# OpenAPI 3.0 → Iceberg DDL type mapping (Athena/Trino syntax)
# ---------------------------------------------------------------------------
_OPENAPI_TO_ICEBERG: dict[tuple[str, Optional[str]], str] = {
    ("string",  None):         "STRING",
    ("string",  "date"):       "DATE",
    ("string",  "date-time"):  "TIMESTAMP",
    ("string",  "byte"):       "BINARY",
    ("string",  "email"):      "STRING",
    ("string",  "uri"):        "STRING",
    ("string",  "password"):   "STRING",
    ("integer", None):         "BIGINT",
    ("integer", "int32"):      "BIGINT",    # Default to BIGINT to accommodate varying payload sizes
    ("integer", "int64"):      "BIGINT",
    ("number",  None):         "DOUBLE",
    ("number",  "float"):      "FLOAT",
    ("number",  "double"):     "DOUBLE",
    ("boolean", None):         "BOOLEAN",
    ("object",  None):         "STRING",    # nested objects stored as JSON strings
    ("array",   None):         "ARRAY<STRING>",
}

# Envelope columns always prepended to every STD table
# These come from the RAW layer and are not in the swagger contract
_ENVELOPE_COLUMNS: list[tuple[str, str]] = [
    ("message_id",       "STRING"),
    ("publish_time",     "TIMESTAMP"),
    ("idempotency_key",  "STRING"),
    ("period_reference", "STRING"),
    ("ingest_uuid",      "STRING"),
    ("gcp_publish_ts",   "TIMESTAMP"),
    ("_std_loaded_at",   "TIMESTAMP"),
]


def _openapi_to_iceberg(openapi_type: str, openapi_format: Optional[str] = None,
                         items: Optional[dict] = None) -> str:
    """Map an OpenAPI field type + format to an Iceberg DDL type string."""
    if openapi_type == "array" and items:
        item_type = _openapi_to_iceberg(
            items.get("type", "string"), items.get("format")
        )
        return f"ARRAY<{item_type}>"

    key = (openapi_type, openapi_format)
    if key in _OPENAPI_TO_ICEBERG:
        return _OPENAPI_TO_ICEBERG[key]

    # Try type-only fallback (ignore unrecognised format)
    fallback = (openapi_type, None)
    if fallback in _OPENAPI_TO_ICEBERG:
        log.warning(
            "Unknown format '%s' for type '%s' — falling back to base type mapping.",
            openapi_format, openapi_type,
        )
        return _OPENAPI_TO_ICEBERG[fallback]

    log.warning("Unknown type '%s' (format '%s') — defaulting to STRING.", openapi_type, openapi_format)
    return "STRING"


# ---------------------------------------------------------------------------
# File loaders
# ---------------------------------------------------------------------------

def _load_yaml(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return yaml.safe_load(fh)
    except FileNotFoundError:
        log.error("Swagger file not found: %s", path)
        sys.exit(2)
    except yaml.YAMLError as exc:
        log.error("YAML parse error in %s: %s", path, exc)
        sys.exit(2)


def _load_json(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        log.error("metadata.json not found: %s", path)
        sys.exit(2)
    except json.JSONDecodeError as exc:
        log.error("JSON parse error in %s: %s", path, exc)
        sys.exit(2)


# ---------------------------------------------------------------------------
# Schema extraction from swagger
# ---------------------------------------------------------------------------

def _extract_columns(swagger: dict, layer: str, sensitive_columns: Optional[list[str]] = None) -> list[dict]:
    """
    Extract an ordered column list from the OpenAPI swagger.
    layer: 'standardized' (all STRING, with _raw) or 'curated' (strongly typed, masked only)
    Returns: [{name, iceberg_type, comment, required}]
    """
    if sensitive_columns is None:
        sensitive_columns = []
        
    schemas = swagger.get("components", {}).get("schemas", {})
    if not schemas:
        log.error("No 'components.schemas' found in swagger. Cannot extract columns.")
        sys.exit(2)

    model_name, model_def = next(iter(schemas.items()))
    properties = model_def.get("properties", {})
    required_fields = set(model_def.get("required", []))

    def _flatten_swagger_props(props: dict, prefix: str = "", current_depth: int = 1) -> dict:
        flattened = {}
        for k, v in props.items():
            new_key = f"{prefix}_{k}" if prefix else k
            new_key = new_key.replace("-", "_").replace(".", "_").lower()

            # Resolve same-document $ref before checking if it's an object
            field_def = v
            if "$ref" in field_def:
                ref_key = field_def["$ref"].split("/")[-1]
                field_def = schemas.get(ref_key, field_def)

            if field_def.get("type") == "object" and "properties" in field_def and current_depth < 5:
                flattened.update(_flatten_swagger_props(field_def["properties"], new_key, current_depth + 1))
            else:
                flattened[new_key] = field_def
        return flattened

    flat_properties = _flatten_swagger_props(properties)

    columns = []
    for field_name, field_def in flat_properties.items():
        if layer == "standardized":
            # Standardized layer strictly uses STRING for all payload elements to auto-drift safely
            iceberg_type = "STRING"
        else:
            # Curated layer uses strict mapped types
            iceberg_type = _openapi_to_iceberg(
                field_def.get("type", "string"),
                field_def.get("format"),
                field_def.get("items"),
            )
        
        comment = field_def.get("description", "")
        if field_name in sensitive_columns:
            comment = f"{comment} SENSITIVE - Masked.".strip()
            
        columns.append({
            "name":         field_name,
            "iceberg_type": iceberg_type,
            "comment":      comment,
            "required":     field_name in required_fields, # Note: Deep required resolution done in linter
        })
        
        if layer == "standardized" and field_name in sensitive_columns:
            columns.append({
                "name":         f"{field_name}_raw",
                "iceberg_type": "STRING",
                "comment":      f"RAW unmasked value for {field_name}. SENSITIVE.",
                "required":     False,
            })

    log.info("Extracted %d flattened payload columns from swagger model '%s' for layer '%s'.", len(columns), model_name, layer)
    return columns


# ---------------------------------------------------------------------------
# AWS clients — credentials from ambient OIDC-assumed role
# ---------------------------------------------------------------------------

def _make_clients(region: str) -> tuple:
    """Return (glue, athena, s3, sfn, dynamodb) boto3 clients."""
    session = boto3.Session(region_name=region)
    return (
        session.client("glue"),
        session.client("athena"),
        session.client("s3"),
        session.client("stepfunctions"),
        session.resource("dynamodb"),
    )

# ---------------------------------------------------------------------------
# DynamoDB Registry integration
# ---------------------------------------------------------------------------

def _update_pipeline_registry(dynamodb, table_name: str, topic_name: str, status: str) -> None:
    """
    Update the DynamoDB Pipeline Registry.
    status should be 'ENABLED' or 'PAUSED'
    """
    table = dynamodb.Table(table_name)
    try:
        table.update_item(
            Key={'topic_name': topic_name},
            UpdateExpression="SET flow_control_flag = :s, last_updated_at = :t, is_active = :a",
            ExpressionAttributeValues={
                ':s': status,
                ':t': datetime.now(timezone.utc).isoformat(),
                ':a': True
            }
        )
        log.info("DynamoDB Registry updated: %s -> %s", topic_name, status)
    except ClientError as e:
        log.error("Failed to update DynamoDB Registry for %s: %s", topic_name, e)
        # We don't fail the deployment if the registry update fails, 
        # but we do log it prominently.
        
# ---------------------------------------------------------------------------
# Glue Catalog helpers
# ---------------------------------------------------------------------------

def _table_exists(glue, database: str, table_name: str) -> bool:
    try:
        glue.get_table(DatabaseName=database, Name=table_name)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "EntityNotFoundException":
            return False
        raise


def _get_existing_column_names(glue, database: str, table_name: str) -> set[str]:
    resp = glue.get_table(DatabaseName=database, Name=table_name)
    cols = resp["Table"]["StorageDescriptor"]["Columns"]
    return {c["Name"] for c in cols}


def _archive_table(glue, database: str, table_name: str) -> str:
    """
    Create an archive catalog entry pointing to the SAME S3 prefix,
    then delete the original catalog entry.

    The S3 data is NEVER touched — only the Glue catalog metadata is modified.
    The archive table remains fully queryable after this operation.

    Returns the archive table name.
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    archive_name = f"{table_name}_archive_{ts}"

    log.info("Archiving Glue table '%s' → '%s'", table_name, archive_name)

    # Fetch existing definition
    existing = glue.get_table(DatabaseName=database, Name=table_name)["Table"]

    # Strip read-only fields that Glue rejects on CreateTable
    _read_only = {
        "DatabaseName", "CreateTime", "UpdateTime", "CreatedBy",
        "IsRegisteredWithLakeFormation", "CatalogId", "VersionId",
    }
    table_input = {k: v for k, v in existing.items() if k not in _read_only}
    table_input["Name"] = archive_name
    table_input.setdefault("Parameters", {}).update({
        "archived_from":    table_name,
        "archived_at":      datetime.now(timezone.utc).isoformat(),
        "archive_reason":   "nuclear_reset",
    })

    glue.create_table(DatabaseName=database, TableInput=table_input)
    log.info("Archive catalog entry created: '%s'", archive_name)

    glue.delete_table(DatabaseName=database, Name=table_name)
    log.info(
        "Original catalog entry '%s' deleted. "
        "S3 data preserved at original location.",
        table_name,
    )

    return archive_name


# ---------------------------------------------------------------------------
# Athena DDL execution with async polling
# ---------------------------------------------------------------------------

def _run_athena_ddl(athena, sql: str, database: str, workgroup: str,
                    output_s3: str, label: str) -> str:
    """
    Submit a DDL statement to Athena and poll until SUCCEEDED or FAILED.
    Uses exponential backoff (capped at 15 s) to avoid hot-polling.

    Returns the QueryExecutionId on success.
    Calls sys.exit(1) on failure.
    """
    log.info("Submitting Athena DDL [%s]...", label)
    log.debug("SQL:\n%s", sql)

    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
        ResultConfiguration={"OutputLocation": output_s3},
    )
    exec_id = resp["QueryExecutionId"]
    log.info("QueryExecutionId: %s", exec_id)

    interval  = 2.0
    max_wait  = 300   # 5 minutes — more than enough for any schema DDL
    elapsed   = 0.0

    while elapsed < max_wait:
        status_resp = athena.get_query_execution(QueryExecutionId=exec_id)
        state = status_resp["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            log.info("Athena DDL [%s] SUCCEEDED (id=%s).", label, exec_id)
            return exec_id

        if state in ("FAILED", "CANCELLED"):
            reason = status_resp["QueryExecution"]["Status"].get(
                "StateChangeReason", "No reason provided"
            )
            log.error(
                "Athena DDL [%s] %s: %s (id=%s)", label, state, reason, exec_id
            )
            sys.exit(1)

        log.debug("Athena state=%s, retrying in %.0fs...", state, interval)
        time.sleep(interval)
        elapsed  += interval
        interval  = min(interval * 1.5, 15.0)

    log.error(
        "Athena DDL [%s] timed out after %ds (id=%s).", label, max_wait, exec_id
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# DDL builders
# ---------------------------------------------------------------------------

def _build_create_table_ddl(
    database: str,
    table_name: str,
    columns: list[dict],
    s3_location: str,
) -> str:
    """
    Build CREATE TABLE DDL for a new Iceberg table.
    Envelope columns are prepended before the payload columns.
    """
    # Envelope columns
    env_col_defs = [
        f"`{name}` {dtype} COMMENT 'RAW layer envelope column'"
        for name, dtype in _ENVELOPE_COLUMNS
    ]

    # Payload columns from swagger
    payload_col_defs = []
    for col in columns:
        defn = f"`{col['name']}` {col['iceberg_type']}"
        if col["comment"]:
            # Escape single quotes in comment text
            safe_comment = col["comment"].replace("'", "''")
            defn += f" COMMENT '{safe_comment}'"
        payload_col_defs.append(defn)

    all_col_defs = ",\n  ".join(env_col_defs + payload_col_defs)

    return f"""
CREATE TABLE {database}.{table_name} (
  {all_col_defs}
)
PARTITIONED BY (period_reference, days(publish_time))
LOCATION '{s3_location}'
TBLPROPERTIES (
  'table_type'         = 'ICEBERG',
  'format'             = 'parquet',
  'write_compression'  = 'snappy'
)
""".strip()


def _build_add_columns_ddl(
    database: str,
    table_name: str,
    new_columns: list[dict],
) -> str:
    """Build ALTER TABLE ADD COLUMNS DDL for safe schema evolution."""
    col_defs = ", ".join(
        f"`{col['name']}` {col['iceberg_type']}"
        for col in new_columns
    )
    return f"ALTER TABLE {database}.{table_name} ADD COLUMNS ({col_defs})"


# ---------------------------------------------------------------------------
# S3 config upload
# ---------------------------------------------------------------------------

def _upload_metadata(s3, config_bucket: str, topic_name: str, local_path: str) -> str:
    """
    Upload metadata.json to:
      s3://<config_bucket>/topics/<topic_name>/metadata.json

    Only metadata.json is uploaded — NOT the swagger.
    The swagger lives in Git. The schema lives in Glue Catalog.
    The Enforcer reads the schema from Glue at runtime, not from S3.

    Returns the S3 URI of the uploaded file.
    """
    key = f"topics/{topic_name}/metadata.json"
    with open(local_path, "rb") as fh:
        s3.put_object(
            Bucket=config_bucket,
            Key=key,
            Body=fh.read(),
            ContentType="application/json",
        )
    uri = f"s3://{config_bucket}/{key}"
    log.info("Uploaded metadata.json → %s", uri)
    return uri


# ---------------------------------------------------------------------------
# Step Functions trigger (nuclear reset tail)
# ---------------------------------------------------------------------------

def _trigger_rebuilder(sfn, sfn_arn: str, target_table: str,
                       source_topics: list[str], archive_table: str) -> str:
    """
    Start the Historical Rebuilder Step Functions state machine.
    Returns the execution ARN.
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    execution_name = f"rebuild-{target_table}-{ts}"

    payload = {
        "target_std_table": target_table,
        "source_topics":    source_topics,
        "archive_table":    archive_table,
    }

    resp = sfn.start_execution(
        stateMachineArn=sfn_arn,
        name=execution_name,
        input=json.dumps(payload),
    )
    arn = resp["executionArn"]
    log.info("Step Functions Rebuilder started: %s", arn)
    return arn


# ---------------------------------------------------------------------------
# Main deployment orchestrator
# ---------------------------------------------------------------------------

def deploy(swagger_path: str, metadata_path: str) -> None:
    """
    Full CD deployment pipeline:
      1. Load inputs
      2. Classify operation against live Glue state (not just metadata.json)
      3. Execute DDL
      4. Upload metadata.json to S3
      5. Update Pipeline Registry (DynamoDB)
      6. Trigger Rebuilder if nuclear_reset
    """
    env = _load_env()
    glue, athena, s3, sfn, dynamodb = _make_clients(env["AWS_REGION"])

    # ── Load inputs ────────────────────────────────────────────────────────
    swagger  = _load_yaml(swagger_path)
    metadata = _load_json(metadata_path)

    target_table   = metadata["target_std_table"]
    source_topics  = metadata["source_topics"]
    nuclear_reset  = metadata.get("nuclear_reset", False)
    primary_topic  = source_topics[0]   # used as the config bucket key
    database       = env["ATHENA_DATABASE"]

    log.info("=" * 60)
    log.info("CD Deployer — Start")
    log.info("=" * 60)
    log.info("Target table  : %s.%s", database, target_table)
    log.info("Source topics : %s", source_topics)
    log.info("Nuclear reset : %s", nuclear_reset)

    # ── Extract columns (Both Layers) ───────────────────────────────────────
    std_columns = _extract_columns(swagger, layer="standardized", sensitive_columns=metadata.get("sensitive_columns", []))
    cur_columns = _extract_columns(swagger, layer="curated", sensitive_columns=metadata.get("sensitive_columns", []))

    # Determine table names
    std_table_name = target_table
    cur_table_name = metadata.get("target_cur_table", target_table.replace("std_", "cur_", 1) if target_table.startswith("std_") else f"cur_{target_table}")

    # Determine databases (Allows direct specification in metadata, falls back to env vars)
    database_std = metadata.get("database_std", env["ATHENA_DATABASE"])
    database_cur = metadata.get("database_cur", env.get("ATHENA_DATABASE_CURATED", database_std.replace("standardized", "curated")))

    # To simplify dual-deployment, define a payload config tuple
    deploy_targets = [
        {"layer": "standardized", "db": database_std, "table": std_table_name, "cols": std_columns},
        {"layer": "curated",      "db": database_cur, "table": cur_table_name, "cols": cur_columns},
    ]

    nuclear_archive_tables = []

    for target in deploy_targets:
        layer      = target["layer"]
        t_db       = target["db"]
        t_table    = target["table"]
        t_cols     = target["cols"]

        log.info("--- Processing Layer: %s [%s.%s] ---", layer.upper(), t_db, t_table)

        table_exists = _table_exists(glue, t_db, t_table)

        # ── Execute DDL ────────────────────────────────────────────────────────
        if not table_exists:
            # ── NEW TABLE ──────────────────────────────────────────────────────
            log.info("Table '%s' not found in Glue Catalog. Creating new table.", t_table)

            # Metadata explicit S3 path takes absolute precedence. Otherwise, build from Env Var default
            explicit_s3 = metadata.get(f"s3_path_{layer[:3]}")
            if explicit_s3:
                s3_location = f"{explicit_s3.rstrip('/')}/"
            else:
                base_s3 = env['ICEBERG_TABLE_S3_BASE'].rstrip('/')
                if layer == "curated":
                    base_s3 = base_s3.replace("standardized", "curated")
                s3_location = f"{base_s3}/{t_table}/"
            
            ddl = _build_create_table_ddl(t_db, t_table, t_cols, s3_location)
            _run_athena_ddl(
                athena, ddl, t_db,
                env["ATHENA_WORKGROUP"], env["ATHENA_OUTPUT_S3"],
                label=f"CREATE TABLE {t_table} ({layer})",
            )

        elif nuclear_reset:
            # ── NUCLEAR RESET ──────────────────────────────────────────────────
            log.warning("NUCLEAR RESET: Archiving '%s' and recreating with new schema.", t_table)

            archive_table_name = _archive_table(glue, t_db, t_table)
            if layer == "standardized":
                 nuclear_archive_tables.append(archive_table_name) # Used to trigger Rebuilder

            # New table gets a fresh S3 prefix — old data stays at the archive prefix
            ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            explicit_s3 = metadata.get(f"s3_path_{layer[:3]}")
            
            if explicit_s3:
                # If explicit path provided, strip trailing slash and append timestamp to avoid collisions
                s3_location = f"{explicit_s3.rstrip('/')}_{ts}/"
            else:
                base_s3 = env['ICEBERG_TABLE_S3_BASE'].rstrip('/')
                if layer == "curated":
                    base_s3 = base_s3.replace("standardized", "curated")
                s3_location = f"{base_s3}/{t_table}_{ts}/"
            
            ddl = _build_create_table_ddl(t_db, t_table, t_cols, s3_location)
            _run_athena_ddl(
                athena, ddl, t_db,
                env["ATHENA_WORKGROUP"], env["ATHENA_OUTPUT_S3"],
                label=f"CREATE TABLE {t_table} (post nuclear reset) ({layer})",
            )

        else:
            # ── SCHEMA EVOLVE or PURE REMAP ────────────────────────────────────
            existing_cols = _get_existing_column_names(glue, t_db, t_table)
            swagger_col_names = {c["name"] for c in t_cols}
            new_col_names  = swagger_col_names - existing_cols
            new_columns    = [c for c in t_cols if c["name"] in new_col_names]

            if not new_columns:
                log.info(
                    "No new columns to add — schema already matches Glue Catalog. "
                    "This is a PURE REMAP or a no-op evolution. Skipping DDL."
                )
            else:
                log.info(
                    "SCHEMA EVOLVE: Adding %d new column(s): %s",
                    len(new_columns), [c["name"] for c in new_columns],
                )
                ddl = _build_add_columns_ddl(t_db, t_table, new_columns)
                _run_athena_ddl(
                    athena, ddl, t_db,
                    env["ATHENA_WORKGROUP"], env["ATHENA_OUTPUT_S3"],
                    label=f"ALTER TABLE {t_table} ADD COLUMNS ({layer})",
                )

    # ── Upload metadata.json to S3 ─────────────────────────────────────────
    _upload_metadata(s3, env["CONFIG_BUCKET"], primary_topic, metadata_path)

    # ── Update DynamoDB Pipeline Registry ──────────────────────────────────
    # If it's a nuclear reset, we PAUSE the normal orchestrator until the rebuilder finishes.
    # Otherwise, we ensure it is ENABLED in the registry so the Step Function orchestrator runs it.
    registry_table = env["PIPELINE_REGISTRY_TABLE"]
    status = "PAUSED" if nuclear_reset else "ENABLED"
    _update_pipeline_registry(dynamodb, registry_table, primary_topic, status)
    
    # ── Trigger Rebuilder if nuclear reset ─────────────────────────────────
    if nuclear_reset and nuclear_archive_tables:
        log.info("Triggering Historical Rebuilder via Step Functions...")
        std_archive = nuclear_archive_tables[0] # First one was Standardized layer
        exec_arn = _trigger_rebuilder(
            sfn,
            sfn_arn=env["REBUILDER_SFN_ARN"],
            target_table=target_table,
            source_topics=source_topics,
            archive_table=std_archive,
        )
        log.info(
            "Rebuilder started. Monitor at: "
            "https://console.aws.amazon.com/states/home#/executions/details/%s",
            exec_arn,
        )

    log.info("=" * 60)
    log.info("CD Deployer — Complete")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CD Iceberg Deployer — applies schema to Glue Catalog via Athena."
    )
    parser.add_argument(
        "--swagger",
        required=True,
        help="Path to swagger.yaml from the merged PR commit.",
    )
    parser.add_argument(
        "--metadata",
        required=True,
        help="Path to metadata.json from the merged PR commit.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        deploy(args.swagger, args.metadata)
    except Exception as exc:
        log.exception("Unhandled exception in CD deployer: %s", exc)
        sys.exit(1)

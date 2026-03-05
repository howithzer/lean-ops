# Lean-Ops Glue Framework (v2)

This repository contains reusable Spark/Glue utilities for building schema-driven, scalable data ingestion pipelines into Apache Iceberg. It acts as a standard toolkit to unify different ingestion patterns (e.g., real-time streaming, batch file processing, database CDC).

## Design Philosophy

The v2 framework separates the **Data Source** (reading files, polling APIs, streaming topics) from **Data Governance** (schema validation, deduplication, auditing, type enforcement). 

By extracting the governance rules into a shared `utils/` library, any Spark application can rapidly implement a production-ready Iceberg pipeline that conforms to organizational standards.

## Technical Capabilities

### 1. Schema-Driven Iceberg Operations (`utils/iceberg_ops.py`)
- **Dynamic Provisioning:** Uses JSON schema definitions to automatically generate Iceberg DDL (`CREATE TABLE IF NOT EXISTS`) at runtime.
- **Dynamic Partitioning:** Inspects the JSON schema to construct Iceberg `PARTITIONED BY` clauses automatically without requiring manual Terraform metadata definitions.
- **Standardized Catalog:** Provides a unified helper for configuring Spark sessions for Glue Data Catalogs.

### 2. Streamlined Data Transformation (`utils/transforms.py`)
- **Business Deduplication:** Implements a generic LIFO (Last-In-First-Out) deduplication strategy. It groups by a configurable primary key and retains the latest record based on a sort timestamp.
- **Audit Tracking:** Standardizes governance tracking across pipelines by automatically appending consistent audit columns (`first_seen_ts`, `last_updated_ts`, `_schema_version`).
- **Dynamic Type Casting:** Inspects a JSON schema definition and safely casts generic string DataFrames to their proper native Spark SQL types (e.g., `INT`, `DECIMAL(10,2)`, `TIMESTAMP`).

### 3. Critical Data Validation (`utils/validation.py`)
- **CDE (Critical Data Element) Enforcement:** Parses the JSON schema to identify mandatory fields. Scans incoming DataFrames and rejects records missing required CDEs before processing.
- **Error Routing:** Generates a unified error report DataFrame containing the raw payload and validation failures, ready to be routed to Dead Letter or Error tracking tables.

### 4. Payload Flattening (`utils/flatten.py`)
- **Nested JSON Unpacking:** Automatically flattens complex, deeply nested JSON structures into a flat column layout, replacing nested delimiters with standard underscores (e.g., `event_meta_id`).

## Example Usage

The framework allows developers to focus purely on extracting the data from its source, while offloading the complex Big Data operations to the library:

```python
# 1. Custom Data Source Logic
df_raw = my_custom_csv_reader("s3://landing-zone/data.csv")
schema = load_schema("s3://schemas/data_schema.json")

# 2. Framework Governance & Transformation
from glue_v2.utils import validation, transforms, iceberg_ops

df_valid, df_errors = validation.validate_cdes(df_raw, schema)
df_typed = transforms.cast_types(df_valid, schema)
df_final = transforms.add_audit_columns(df_typed, "1.0")
df_dedup = transforms.dedup_keep_latest(df_final, "transaction_id", "timestamp")

# 3. Iceberg Write
iceberg_ops.create_table_from_schema(spark, schema, "glue_catalog.db.table", "s3://path")
df_dedup.writeTo("glue_catalog.db.table").append()
```

## Directory Structure
- `/utils/` - Core library modules (transforms, validation, config, etc.)
- `file_processor.py` - Reference implementation for batch S3 CSV/JSON ingestion.
- `stream_processor.py` - Reference implementation for continuous stream ingestion.

"""
Iceberg Operations Utilities
============================
Handles Spark catalog integration and dynamic Iceberg DDL statements.
"""

from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)


def apply_iceberg_catalog_settings(spark: SparkSession, warehouse_path: str):
    """
    Configures the current Spark Session to talk to AWS Glue as an Iceberg Catalog.
    Must be called before executing any Iceberg SQL commands.
    """
    settings = {
        "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.glue_catalog.warehouse": warehouse_path
    }
    
    for key, value in settings.items():
        spark.conf.set(key, value)
        
    logger.info(f"Spark configured for Iceberg Glue Catalog (warehouse: {warehouse_path})")


def create_table_from_schema(spark: SparkSession, schema: dict, table_name: str, location: str):
    """
    Creates a partitioned Iceberg table dynamically based on a JSON schema definition.
    If the table already exists, it does nothing (safe to run repeatedly).
    
    Args:
        spark: The active SparkSession
        schema: Parsed JSON dictionary defining 'columns', 'audit_columns', and 'partitioning'
        table_name: Fully qualified target (e.g. glue_catalog.my_db.my_table)
        location: S3 path for Iceberg data/metadata
    """
    # Check if table exists (avoids error logs from Spark)
    try:
        db_name, tbl_name = table_name.split('.')[-2:]
        if spark.catalog.tableExists(f"{db_name}.{tbl_name}"):
            logger.info(f"Table {table_name} already exists, skipping dynamic DDL.")
            return
    except Exception:
        # If catalog split logic fails, let the CREATE TABLE IF NOT EXISTS handle it
        pass

    logger.info(f"Provisioning new Iceberg table {table_name} from schema...")
    
    # 1. Build column definitions from schema
    column_defs = []
    for col_name, col_spec in schema.get('columns', {}).items():
        col_type = str(col_spec.get('type', 'STRING')).upper()
        # Clean Spark SQL compatibility
        if col_type.startswith('DECIMAL'):
            spark_type = col_type
        else:
            spark_type = col_type if col_type in ('INT', 'BIGINT', 'DOUBLE', 'TIMESTAMP', 'BOOLEAN') else 'STRING'
        
        column_defs.append(f"{col_name} {spark_type}")
    
    # 2. Append standard audit columns defined in schema
    for audit_col, spec in schema.get('audit_columns', {}).items():
        audit_type = str(spec.get('type', 'STRING')).upper()
        spark_type = 'TIMESTAMP' if audit_type == 'TIMESTAMP' else 'STRING'
        column_defs.append(f"{audit_col} {spark_type}")
    
    columns_sql = ",\n        ".join(column_defs)

    # 3. Dynamic partitioning
    partition_fields = schema.get('partitioning', {}).get('fields', [])
    partitioned_by_clause = f"PARTITIONED BY ({', '.join(partition_fields)})" if partition_fields else ""

    # 4. Construct and execute the DDL
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql}
    )
    USING iceberg
    {partitioned_by_clause}
    LOCATION '{location}'
    TBLPROPERTIES (
        'format-version' = '2',
        'write.format.default' = 'parquet'
    )
    """

    logger.info(f"Executing DDL for {table_name}:\n{create_sql}")
    spark.sql(create_sql)
    logger.info(f"✅ Table {table_name} provisioned successfully.")

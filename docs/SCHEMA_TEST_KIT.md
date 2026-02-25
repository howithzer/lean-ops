# Schema Test Kit Guide

The Schema Test Kit is a standalone suite for testing how `lean-ops` schema management scripts will interpret, validate, and flatten complex schema definitions before they are deployed to the AWS Data Lake.

It allows developers to verify schema mappings—including nested objects, arrays, `$ref` pointers, and PII sensitivity tagging—without needing AWS credentials or a live database.

## Prerequisites
- **Python 3.9+**
- The `pyyaml` library (installed via `pip install pyyaml`)

## Test Kit Structure
The test kit is organized around isolated **scenarios**.

```text
schema_test_kit/
├── cd_deployer.py             # A local mirror of the CD schema deployment logic
├── ci_linter.py               # A local mirror of the CI schema validation logic
└── scenarios/
    ├── 01_new_table/          # A basic scenario
    ├── 07_massive_schema/     # A complex enterprise schema with deep references
    └── 09_extended_nesting/   # A stress-test schema validating depth limits
```

### Anatomy of a Scenario
Each scenario must reside in its own subdirectory and contain:
1. **`swagger_v2.yaml` / `swagger_v1.yaml`**: The target OpenAPI 3.0 schema definitions. Providing a _v1_ allows the linter to simulate and analyze schema evolution (drift). Providing only _v2_ simulates an initial table deployment.
2. **`metadata.json`**: The configuration file that binds the schema to topics, specifies target tables, and configures `sensitive_columns` for PII masking. 

## Flattening Constraints
The schema management tools (`cd_deployer.py` and `ci_linter.py`) recursively flatten properties. 
- **Maximum Depth**: Recursion is strictly capped at **5 levels of depth**. Objects deeper than level 5 will not explode into individual leaf properties; they will instead be persisted collectively as a single STRING representation (effectively a JSON payload).
- This ensures Athena/Iceberg columns do not infinitely explode out of control while preserving data structure for downstream systems.

## Running Tests
Run the linter locally from the test kit root directory using:

```bash
# Example: Running the linter against Scenario 09
python ci_linter.py --v2 scenarios/09_extended_nesting/swagger_v2.yaml --metadata scenarios/09_extended_nesting/metadata.json --first-deploy
```

### Viewing Results
The `ci_linter.py` script will output the exact `CREATE TABLE` and `ALTER TABLE` DDL queries that will be executed against both the **Standardized** and **Curated** databases during a production deployment. 

You can inspect the generated DDL to confirm:
- `$ref` objects are properly expanded.
- Properties beyond 5 levels are accurately truncated to STRING mappings.
- Output from `metadata.json`'s `sensitive_columns` list successfully applies the `SENSITIVE` tag and creates duplicate `_raw` columns in the standardized layer.

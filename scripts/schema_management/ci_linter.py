"""
ci_linter.py — Schema CI Linter
================================
Runs in the ADO Schema Validate pipeline (manual trigger on feature branch)
and the ADO Schema CI Gate pipeline (automatic trigger on PR open).

Both pipelines invoke this same script — the difference is only the trigger
mechanism and whether a failure blocks a merge.

Exit codes (ADO reads these):
  0  = pass — safe to proceed
  1  = fail — breaking change or invalid files (blocks PR merge)
  2  = configuration error — missing files, bad YAML/JSON (pipeline error)

Usage (ADO pipeline step):
  python ci_linter.py \
    --v1  $(Pipeline.Workspace)/main/schemas/topic_trades_v2/swagger.yaml \
    --v2  $(Build.SourcesDirectory)/schemas/topic_trades_v2/swagger.yaml \
    --metadata $(Build.SourcesDirectory)/schemas/topic_trades_v2/metadata.json \
    [--first-deploy]   # set this flag when no v1 exists on main yet

OpenAPI 3.0 expected structure for swagger.yaml:
  components:
    schemas:
      MyModel:
        required: [field_a]
        properties:
          field_a:
            type: string
          field_b:
            type: integer
            format: int32
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Optional

import yaml  # pip install pyyaml

# ---------------------------------------------------------------------------
# Logging — structured output that ADO log streaming renders correctly
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("ci_linter")

# ---------------------------------------------------------------------------
# Safe numeric format widenings — these are NOT breaking changes
# e.g. producer bumped int32 field to int64 — downstream can always read wider
# ---------------------------------------------------------------------------
SAFE_FORMAT_WIDENINGS: set[tuple[Optional[str], Optional[str]]] = {
    ("int32", "int64"),
    ("float", "double"),
}

# ---------------------------------------------------------------------------
# Required fields in metadata.json — pipeline fails fast if any are absent
# ---------------------------------------------------------------------------
REQUIRED_METADATA_FIELDS = ["source_topics", "target_std_table", "sensitive_columns"]
NUCLEAR_EXTRA_FIELDS     = ["nuclear_justification", "impact_assessment"]


# ---------------------------------------------------------------------------
# File loaders — clean error messages on bad input, no stack traces in ADO log
# ---------------------------------------------------------------------------

def _load_yaml(path: str) -> dict:
    """Load and parse a YAML file. Exits with code 2 on any failure."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            content = yaml.safe_load(fh)
        if not isinstance(content, dict):
            log.error("YAML file is not a mapping (dict) at root level: %s", path)
            sys.exit(2)
        return content
    except FileNotFoundError:
        log.error("File not found: %s", path)
        sys.exit(2)
    except yaml.YAMLError as exc:
        log.error("YAML parse error in %s: %s", path, exc)
        sys.exit(2)


def _load_json(path: str) -> dict:
    """Load and parse a JSON file. Exits with code 2 on any failure."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            content = json.load(fh)
        if not isinstance(content, dict):
            log.error("JSON file is not an object at root level: %s", path)
            sys.exit(2)
        return content
    except FileNotFoundError:
        log.error("File not found: %s", path)
        sys.exit(2)
    except json.JSONDecodeError as exc:
        log.error("JSON parse error in %s: %s", path, exc)
        sys.exit(2)


# ---------------------------------------------------------------------------
# Swagger property extraction
# OpenAPI 3.0 stores schema under components.schemas.<ModelName>.properties
# This is NOT the same as root-level 'properties'
# ---------------------------------------------------------------------------

def _extract_properties(swagger: dict, source_label: str) -> tuple[dict, set[str]]:
    """
    Extract the flat property map and required-field set from an OpenAPI 3.0
    swagger dict.

    Returns:
        properties : {field_name: {type, format, ...}}
        required   : set of field names declared as required

    Exits with code 2 if the swagger structure is not as expected.
    """
    schemas = swagger.get("components", {}).get("schemas", {})
    if not schemas:
        log.error(
            "[%s] No 'components.schemas' found. "
            "Swagger must follow OpenAPI 3.0 structure with a model defined under "
            "components.schemas.<ModelName>.",
            source_label,
        )
        sys.exit(2)

    # Use the first model defined — single-model-per-topic convention
    model_name, model_def = next(iter(schemas.items()))
    log.info("[%s] Extracted model '%s' from components.schemas", source_label, model_name)

    properties = model_def.get("properties", {})
    if not properties:
        log.error(
            "[%s] Model '%s' has no properties defined.",
            source_label, model_name,
        )
        sys.exit(2)

    required = set(model_def.get("required", []))

    # Helper function to recursively flatten swagger properties
    def _flatten_swagger_props(props: dict, prefix: str = "") -> dict:
        flattened = {}
        for k, v in props.items():
            new_key = f"{prefix}_{k}" if prefix else k
            # Sanitize column name (matching utils.flatten)
            new_key = new_key.replace("-", "_").replace(".", "_").lower()

            if v.get("type") == "object" and "properties" in v:
                flattened.update(_flatten_swagger_props(v["properties"], new_key))
            else:
                flattened[new_key] = v
        return flattened

    def _flatten_required(reqs: list, props: dict, prefix: str = "") -> set:
        flattened_reqs = set()
        for k in reqs:
            v = props.get(k, {})
            new_key = f"{prefix}_{k}" if prefix else k
            new_key = new_key.replace("-", "_").replace(".", "_").lower()
            
            if v.get("type") == "object" and "properties" in v:
                # If an object is required, are its children required? 
                # Open API defines required children locally. So here we just
                # add the nested required fields if present.
                nested_reqs = v.get("required", [])
                flattened_reqs.update(_flatten_required(nested_reqs, v["properties"], new_key))
            else:
                flattened_reqs.add(new_key)
        return flattened_reqs

    flat_properties = _flatten_swagger_props(properties)
    flat_required = _flatten_required(list(required), properties)

    return flat_properties, flat_required


# ---------------------------------------------------------------------------
# Metadata validation
# ---------------------------------------------------------------------------

def _validate_metadata(metadata: dict, swagger_properties: dict) -> tuple[bool, str]:
    """
    Validate metadata.json structure and cross-reference sensitive_columns
    against the swagger field list.

    Returns (is_valid, nuclear_reset_flag)
    Calls sys.exit(2) on hard configuration errors.
    """
    # Check all required top-level fields are present
    missing = [f for f in REQUIRED_METADATA_FIELDS if f not in metadata]
    if missing:
        log.error(
            "metadata.json is missing required fields: %s",
            missing,
        )
        sys.exit(2)

    if not isinstance(metadata["source_topics"], list) or not metadata["source_topics"]:
        log.error("metadata.json 'source_topics' must be a non-empty list.")
        sys.exit(2)

    if not isinstance(metadata["target_std_table"], str) or not metadata["target_std_table"].strip():
        log.error("metadata.json 'target_std_table' must be a non-empty string.")
        sys.exit(2)

    nuclear_reset = metadata.get("nuclear_reset", False)

    # When nuclear_reset is true, justification fields are mandatory
    if nuclear_reset:
        missing_nuclear = [f for f in NUCLEAR_EXTRA_FIELDS if not metadata.get(f, "").strip()]
        if missing_nuclear:
            log.error(
                "nuclear_reset=true requires these fields to be non-empty in metadata.json: %s\n"
                "  nuclear_justification: explain WHY the breaking change is unavoidable\n"
                "  impact_assessment: describe the impact on consumers and the recovery plan",
                missing_nuclear,
            )
            sys.exit(2)

    # Cross-validate sensitive_columns against swagger properties
    sensitive = metadata.get("sensitive_columns", [])
    unknown_sensitive = [c for c in sensitive if c not in swagger_properties]
    if unknown_sensitive:
        log.error(
            "sensitive_columns references field(s) not present in swagger.yaml: %s\n"
            "Either add these fields to the swagger or remove them from sensitive_columns.",
            unknown_sensitive,
        )
        sys.exit(2)

    return nuclear_reset


# ---------------------------------------------------------------------------
# Operation classification
# Determines what the CD deployer will actually execute
# Returned as a string so it can be logged and surfaced to the developer
# ---------------------------------------------------------------------------

def _classify_operation(
    is_first_deploy: bool,
    nuclear_reset: bool,
    v1_props: dict,
    v2_props: dict,
) -> str:
    """
    Classify what CD operation will be performed.
    Returns one of: NEW_TABLE, SCHEMA_EVOLVE, NUCLEAR_RESET, PURE_REMAP
    """
    if is_first_deploy:
        return "NEW_TABLE"
    if nuclear_reset:
        return "NUCLEAR_RESET"
    if v1_props == v2_props:
        return "PURE_REMAP"
    return "SCHEMA_EVOLVE"


# ---------------------------------------------------------------------------
# Backward compatibility check
# ---------------------------------------------------------------------------

def _check_backward_compatibility(
    v1_props: dict,
    v1_required: set[str],
    v2_props: dict,
    v2_required: set[str],
) -> list[str]:
    """
    Compare v1 and v2 schema properties and return a list of all breaking
    change descriptions. An empty list means the change is safe.

    Rules:
      FAIL — any existing field removed
      FAIL — any existing field type changed (except safe numeric widenings)
      FAIL — any existing field format changed (except safe numeric widenings)
      FAIL — any new field added as required
               (existing records have no value for it)
      PASS — new field added as optional
      PASS — format widened: int32→int64, float→double
      WARN — optional field promoted to required
               (not a struct break but operationally risky — logged, not failed)
    """
    breaking: list[str] = []

    # 1. Removed fields
    for field in v1_props:
        if field not in v2_props:
            msg = f"FIELD REMOVED: '{field}' exists in v1 but is absent in v2."
            log.error("  %s", msg)
            breaking.append(msg)

    # 2. Type and format changes on surviving fields
    for field, v1_def in v1_props.items():
        if field not in v2_props:
            continue  # already reported above

        v2_def = v2_props[field]
        v1_type   = v1_def.get("type")
        v2_type   = v2_def.get("type")
        v1_format = v1_def.get("format")
        v2_format = v2_def.get("format")

        if v1_type != v2_type:
            msg = (
                f"TYPE CHANGE: '{field}' changed from '{v1_type}' to '{v2_type}'. "
                f"All type changes are breaking — consumers cannot cast automatically."
            )
            log.error("  %s", msg)
            breaking.append(msg)
            continue  # no point checking format if type already changed

        if v1_format != v2_format:
            pair = (v1_format, v2_format)
            if pair in SAFE_FORMAT_WIDENINGS:
                log.info(
                    "  FORMAT WIDENING (safe): '%s' widened from '%s' to '%s'.",
                    field, v1_format, v2_format,
                )
            else:
                msg = (
                    f"FORMAT CHANGE: '{field}' format changed from '{v1_format}' "
                    f"to '{v2_format}'. Only safe widenings (int32→int64, float→double) "
                    f"are permitted."
                )
                log.error("  %s", msg)
                breaking.append(msg)

    # 3. New fields — optional is fine, required is breaking
    new_fields = set(v2_props) - set(v1_props)
    for field in sorted(new_fields):
        if field in v2_required:
            msg = (
                f"NEW REQUIRED FIELD: '{field}' added as required. "
                f"Existing records have no value for it — downstream parsing will fail."
            )
            log.error("  %s", msg)
            breaking.append(msg)
        else:
            log.info("  NEW OPTIONAL FIELD (safe): '%s' added.", field)

    # 4. Optional → required promotions (warn only — not a structural break)
    promoted = (v1_props.keys() & v2_required) - v1_required
    for field in sorted(promoted):
        log.warning(
            "  REQUIRED PROMOTION (warning): '%s' was optional in v1 but is now required. "
            "Existing records with null values may fail downstream validation.",
            field,
        )

    return breaking


# ---------------------------------------------------------------------------
# Operation preview — tells the developer what CD will do before they open a PR
# ---------------------------------------------------------------------------

def _log_cd_preview(
    operation: str,
    target_table: str,
    v1_props: dict,
    v2_props: dict,
    sensitive_columns: list[str],
    nuclear_justification: str = "",
) -> None:
    """Log a human-readable preview of what the CD deployer will execute."""
    log.info("")
    log.info("CD DEPLOYER PREVIEW — what will be executed on merge:")
    
    try:
        # Import cd_deployer internally so linter doesn't strictly depend on it 
        # for standard logic, but is used if available.
        import cd_deployer
    except ImportError:
        log.warning("Could not import cd_deployer. Generating standard preview instead.")
        if operation == "NEW_TABLE":
            log.info(
                "  CREATE TABLE standardized.%s with %d columns from swagger.",
                target_table, len(v2_props),
            )
        elif operation == "SCHEMA_EVOLVE":
            # Rough approximation if cd_deployer fails to import
            new_cols = []
            for c in sorted(set(v2_props) - set(v1_props)):
                new_cols.append(c)
                if c in sensitive_columns:
                    new_cols.append(f"{c}_raw")
            if new_cols:
                log.info(
                    "  ALTER TABLE standardized.%s ADD COLUMNS: %s",
                    target_table, new_cols,
                )
            else:
                log.info(
                    "  No column additions needed. "
                    "Only format widenings detected — no DDL will be executed."
                )
        elif operation == "NUCLEAR_RESET":
            log.info(
                "  RENAME TABLE standardized.%s TO %s_archive_<timestamp>",
                target_table, target_table,
            )
            log.info(
                "  CREATE TABLE standardized.%s with NEW schema (%d columns).",
                target_table, len(v2_props),
            )
            log.info("  Justification: %s", nuclear_justification)
        elif operation == "PURE_REMAP":
            log.info(
                "  Zero DDL. Schema unchanged. Only topic routing config will be updated."
            )
        return

    # Use cd_deployer to generate the actual DDL string
    # We must mock some fields that cd_deployer internal methods expect
    s3_location = f"s3://datalake/standardized/{target_table}/"
    database = "standardized"

    def props_to_columns(props: dict) -> list[dict]:
        # Translates our properties map back up to mock an extracted columns map
        cols = []
        for name, defn in props.items():
            # Standardized layer is strictly STRING
            iceberg_type = "STRING"
            
            comment = defn.get("description", "")
            if name in sensitive_columns:
                comment = f"{comment} SENSITIVE - Masked.".strip()

            cols.append({
                "name": name,
                "iceberg_type": iceberg_type,
                "comment": comment,
                "required": False, # doesn't matter for DDL
            })
            
            if name in sensitive_columns:
                cols.append({
                    "name": f"{name}_raw",
                    "iceberg_type": "STRING",
                    "comment": f"RAW unmasked value for {name}. SENSITIVE.",
                    "required": False,
                })
        return cols

    if operation == "NEW_TABLE":
        cols = props_to_columns(v2_props)
        ddl = cd_deployer._build_create_table_ddl(database, target_table, cols, s3_location)
        log.info("\n%s\n", ddl)

    elif operation == "SCHEMA_EVOLVE":
        v1_cols = props_to_columns(v1_props)
        v2_cols = props_to_columns(v2_props)
        
        v1_col_names = {c["name"] for c in v1_cols}
        v2_col_names = {c["name"] for c in v2_cols}
        
        new_names = sorted(v2_col_names - v1_col_names)
        if new_names:
            cols = [c for c in v2_cols if c["name"] in new_names]
            ddl = cd_deployer._build_add_columns_ddl(database, target_table, cols)
            log.info("\n%s\n", ddl)
        else:
            log.info(
                "  No column additions needed. "
                "Only configuration changes detected — no DDL will be executed."
            )

    elif operation == "NUCLEAR_RESET":
        cols = props_to_columns(v2_props)
        ddl = cd_deployer._build_create_table_ddl(database, target_table, cols, s3_location)
        log.info(
            "  RENAME TABLE standardized.%s TO %s_archive_<timestamp>",
            target_table, target_table,
        )
        log.info("\n%s\n", ddl)
        log.info("  Justification: %s", nuclear_justification)

    elif operation == "PURE_REMAP":
        log.info(
            "  Zero DDL. Schema unchanged. Only topic routing config will be updated."
        )


# ---------------------------------------------------------------------------
# Main linter orchestrator
# ---------------------------------------------------------------------------

class SchemaLinter:
    """
    Orchestrates the full schema validation and classification pipeline.
    Instantiate once per run — one swagger pair, one metadata file.
    """

    def __init__(
        self,
        v2_swagger_path: str,
        metadata_path: str,
        v1_swagger_path: Optional[str] = None,
    ) -> None:
        """
        Args:
            v2_swagger_path  : Path to the proposed (PR branch) swagger.yaml
            metadata_path    : Path to metadata.json
            v1_swagger_path  : Path to the current main-branch swagger.yaml
                               Pass None (or omit) for first-deployment scenarios
        """
        self.is_first_deploy = v1_swagger_path is None or not Path(v1_swagger_path).exists()

        log.info("Loading v2 swagger: %s", v2_swagger_path)
        self.v2_swagger = _load_yaml(v2_swagger_path)

        if not self.is_first_deploy:
            log.info("Loading v1 swagger (main branch reference): %s", v1_swagger_path)
            self.v1_swagger = _load_yaml(v1_swagger_path)
        else:
            log.info("No v1 swagger provided — treating as first deployment.")
            self.v1_swagger = None

        log.info("Loading metadata: %s", metadata_path)
        self.metadata = _load_json(metadata_path)

    def run(self) -> int:
        """
        Execute the full linting and classification pipeline.

        Returns:
            0 — pass
            1 — fail (breaking change without nuclear override)
            2 — configuration error (should not reach here — _load_* calls sys.exit(2))
        """
        log.info("=" * 60)
        log.info("Schema CI Linter — Start")
        log.info("=" * 60)

        # Step 1: Extract v2 properties (always needed)
        v2_props, v2_required = _extract_properties(self.v2_swagger, "v2")

        # Step 2: Validate metadata against v2 swagger
        nuclear_reset = _validate_metadata(self.metadata, v2_props)
        target_table  = self.metadata["target_std_table"]
        source_topics = self.metadata["source_topics"]

        log.info("Target table   : %s", target_table)
        log.info("Source topics  : %s", source_topics)
        log.info("Nuclear reset  : %s", nuclear_reset)
        log.info("Sensitive cols : %s", self.metadata.get("sensitive_columns", []))

        # Step 3: First-deployment shortcut — nothing to diff
        if self.is_first_deploy:
            log.info("")
            log.info("RESULT: PASS — First deployment. No v1 schema to compare against.")
            _log_cd_preview("NEW_TABLE", target_table, {}, v2_props, self.metadata.get("sensitive_columns", []))
            return 0

        # Step 4: Extract v1 properties
        v1_props, v1_required = _extract_properties(self.v1_swagger, "v1")

        # Step 5: Classify operation
        operation = _classify_operation(
            self.is_first_deploy, nuclear_reset, v1_props, v2_props
        )
        log.info("Operation type : %s", operation)

        # Step 6: Nuclear reset — bypass compat check, validate justification fields
        if operation == "NUCLEAR_RESET":
            log.warning("")
            log.warning("NUCLEAR RESET declared. Bypassing backward compatibility checks.")
            log.warning("Breaking changes will be applied. All downstream consumers will be affected.")

            # Run the compat check anyway so we can log what IS breaking
            breaking = _check_backward_compatibility(
                v1_props, v1_required, v2_props, v2_required
            )
            if not breaking:
                log.warning(
                    "nuclear_reset=true but NO breaking changes were detected. "
                    "Consider whether nuclear_reset is actually needed here."
                )

            log.warning(
                "Justification : %s", self.metadata.get("nuclear_justification", "")
            )
            log.warning(
                "Impact        : %s", self.metadata.get("impact_assessment", "")
            )
            _log_cd_preview(
                operation, target_table, v1_props, v2_props, self.metadata.get("sensitive_columns", []),
                self.metadata.get("nuclear_justification", ""),
            )
            log.info("")
            log.info("RESULT: PASS (nuclear_reset override) — second approver required on PR.")
            return 0

        # Step 7: Pure remap — schema unchanged
        if operation == "PURE_REMAP":
            _log_cd_preview(operation, target_table, v1_props, v2_props, self.metadata.get("sensitive_columns", []))
            log.info("")
            log.info("RESULT: PASS — Pure topic remap. Schema unchanged.")
            return 0

        # Step 8: Schema evolve — run backward compat check
        log.info("")
        log.info("Running backward compatibility check (%d v1 fields, %d v2 fields)...",
                 len(v1_props), len(v2_props))

        breaking = _check_backward_compatibility(
            v1_props, v1_required, v2_props, v2_required
        )

        _log_cd_preview(operation, target_table, v1_props, v2_props, self.metadata.get("sensitive_columns", []))

        if breaking:
            log.error("")
            log.error("RESULT: FAIL — %d breaking change(s) detected.", len(breaking))
            log.error(
                "To force this deployment, set nuclear_reset=true in metadata.json "
                "and provide nuclear_justification and impact_assessment fields. "
                "A second approver will be required on the PR."
            )
            return 1

        log.info("")
        log.info("RESULT: PASS — Schema is backward compatible.")
        return 0


# ---------------------------------------------------------------------------
# CLI entrypoint — driven by ADO pipeline arguments
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Schema backward-compatibility linter for ADO CI/CD pipelines."
    )
    parser.add_argument(
        "--v1",
        required=False,
        default=None,
        help="Path to current (main branch) swagger.yaml. Omit for first deployments.",
    )
    parser.add_argument(
        "--v2",
        required=True,
        help="Path to proposed (PR branch) swagger.yaml.",
    )
    parser.add_argument(
        "--metadata",
        required=True,
        help="Path to metadata.json.",
    )
    parser.add_argument(
        "--first-deploy",
        action="store_true",
        default=False,
        help=(
            "Explicit flag for first-time deployments where no v1 exists on main. "
            "Skips the diff entirely."
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    # If --first-deploy flag set, treat v1 path as absent regardless of --v1 argument
    v1_path = None if args.first_deploy else args.v1

    linter = SchemaLinter(
        v2_swagger_path=args.v2,
        metadata_path=args.metadata,
        v1_swagger_path=v1_path,
    )

    exit_code = linter.run()
    sys.exit(exit_code)

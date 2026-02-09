"""
Schema Backward Compatibility Validator

Validates that schema changes are backward compatible:
- Allowed: Add optional columns, widen types (INT → BIGINT)
- Blocked: Remove columns, narrow types, change required fields

Usage:
    result = validate_backward_compatibility(old_schema, new_schema)
    if result.has_breaking_changes:
        raise ValidationError(result.breaking_changes)
"""

from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum


class ChangeType(Enum):
    """Classification of schema changes"""
    ADD_COLUMN = "ADD_COLUMN"
    REMOVE_COLUMN = "REMOVE_COLUMN"
    TYPE_CHANGE = "TYPE_CHANGE"
    REQUIRED_CHANGE = "REQUIRED_CHANGE"
    CDE_CHANGE = "CDE_CHANGE"


class CompatibilityLevel(Enum):
    """Compatibility classification"""
    COMPATIBLE = "COMPATIBLE"           # Safe, no action needed
    ENHANCEMENT = "ENHANCEMENT"         # Safe, may need backfill
    BREAKING = "BREAKING"               # Unsafe, blocks deployment


@dataclass
class SchemaChange:
    """Represents a single schema change"""
    change_type: ChangeType
    field_name: str
    old_value: Optional[str]
    new_value: Optional[str]
    compatibility: CompatibilityLevel
    reason: str


@dataclass
class ValidationResult:
    """Result of backward compatibility validation"""
    is_compatible: bool
    is_enhancement: bool
    has_breaking_changes: bool
    changes: List[SchemaChange]
    requires_backfill: bool

    @property
    def breaking_changes(self) -> List[SchemaChange]:
        """Get only breaking changes"""
        return [c for c in self.changes if c.compatibility == CompatibilityLevel.BREAKING]

    @property
    def enhancements(self) -> List[SchemaChange]:
        """Get only enhancements"""
        return [c for c in self.changes if c.compatibility == CompatibilityLevel.ENHANCEMENT]


# =============================================================================
# TYPE COMPATIBILITY MATRIX
# =============================================================================

# Map of (old_type, new_type) → is_compatible
TYPE_COMPATIBILITY_MATRIX = {
    # Exact matches
    ('STRING', 'STRING'): True,
    ('INT', 'INT'): True,
    ('BIGINT', 'BIGINT'): True,
    ('DOUBLE', 'DOUBLE'): True,
    ('TIMESTAMP', 'TIMESTAMP'): True,
    ('BOOLEAN', 'BOOLEAN'): True,

    # Safe widening (no data loss)
    ('INT', 'BIGINT'): True,
    ('INT', 'DOUBLE'): True,
    ('INT', 'STRING'): True,
    ('BIGINT', 'STRING'): True,
    ('DOUBLE', 'STRING'): True,
    ('TIMESTAMP', 'STRING'): True,
    ('BOOLEAN', 'STRING'): True,

    # Unsafe narrowing (data loss or parse errors)
    ('BIGINT', 'INT'): False,
    ('DOUBLE', 'INT'): False,
    ('STRING', 'INT'): False,
    ('STRING', 'BIGINT'): False,
    ('STRING', 'DOUBLE'): False,
    ('STRING', 'TIMESTAMP'): False,
    ('STRING', 'BOOLEAN'): False,
}


def is_type_compatible(old_type: str, new_type: str) -> Tuple[bool, str]:
    """
    Check if type change is backward compatible.

    Args:
        old_type: Current column type (e.g., "INT", "STRING")
        new_type: New column type

    Returns:
        (is_compatible, reason)

    Examples:
        >>> is_type_compatible("INT", "BIGINT")
        (True, "Safe widening: INT → BIGINT")

        >>> is_type_compatible("BIGINT", "INT")
        (False, "Unsafe narrowing: BIGINT → INT (overflow risk)")
    """
    # Normalize types (handle DECIMAL(10,2) → DECIMAL)
    old_base = old_type.split('(')[0].upper()
    new_base = new_type.split('(')[0].upper()

    # Special handling for DECIMAL
    if old_base == 'DECIMAL' and new_base == 'DECIMAL':
        return _check_decimal_compatibility(old_type, new_type)

    # Check matrix
    key = (old_base, new_base)
    is_compatible = TYPE_COMPATIBILITY_MATRIX.get(key, False)

    if is_compatible:
        if old_base == new_base:
            return True, f"No type change"
        else:
            return True, f"Safe widening: {old_base} → {new_base}"
    else:
        return False, f"Unsafe narrowing: {old_base} → {new_base} (data loss or parse error risk)"


def _check_decimal_compatibility(old_type: str, new_type: str) -> Tuple[bool, str]:
    """
    Check DECIMAL precision/scale compatibility.

    Examples:
        DECIMAL(10,2) → DECIMAL(12,2): Compatible (wider precision)
        DECIMAL(12,2) → DECIMAL(10,2): Incompatible (narrower precision)
    """
    import re

    old_match = re.match(r'DECIMAL\((\d+),(\d+)\)', old_type)
    new_match = re.match(r'DECIMAL\((\d+),(\d+)\)', new_type)

    if not old_match or not new_match:
        # Fallback: assume compatible if can't parse
        return True, "DECIMAL without precision info"

    old_precision, old_scale = int(old_match.group(1)), int(old_match.group(2))
    new_precision, new_scale = int(new_match.group(1)), int(new_match.group(2))

    # Compatible if new precision >= old precision and scale unchanged
    if new_precision >= old_precision and new_scale == old_scale:
        return True, f"Safe DECIMAL widening: {old_type} → {new_type}"
    elif new_precision < old_precision:
        return False, f"Unsafe DECIMAL narrowing: {old_type} → {new_type}"
    elif new_scale != old_scale:
        return False, f"DECIMAL scale change: {old_type} → {new_type}"

    return True, f"DECIMAL change: {old_type} → {new_type}"


# =============================================================================
# SCHEMA COMPARISON
# =============================================================================

def validate_backward_compatibility(
    old_schema: Dict,
    new_schema: Dict
) -> ValidationResult:
    """
    Validate that new schema is backward compatible with old schema.

    Args:
        old_schema: Current active schema
        new_schema: Proposed new schema

    Returns:
        ValidationResult with compatibility status and detected changes

    Example:
        >>> old = {"payload_columns": {"user_id": {"type": "INT"}}}
        >>> new = {"payload_columns": {"user_id": {"type": "BIGINT"}, "device": {"type": "STRING"}}}
        >>> result = validate_backward_compatibility(old, new)
        >>> result.is_enhancement  # True (added device, widened user_id)
        >>> result.has_breaking_changes  # False
    """
    changes = []

    # Extract column definitions
    old_columns = _get_all_columns(old_schema)
    new_columns = _get_all_columns(new_schema)

    # 1. Check for removed columns (BREAKING)
    for col_name in old_columns:
        if col_name not in new_columns:
            changes.append(SchemaChange(
                change_type=ChangeType.REMOVE_COLUMN,
                field_name=col_name,
                old_value=old_columns[col_name].get('type', 'STRING'),
                new_value=None,
                compatibility=CompatibilityLevel.BREAKING,
                reason=f"Column '{col_name}' removed (data loss)"
            ))

    # 2. Check for added columns (ENHANCEMENT)
    for col_name in new_columns:
        if col_name not in old_columns:
            new_spec = new_columns[col_name]
            is_required = new_spec.get('required', False) or new_spec.get('cde', False)

            if is_required:
                # Adding required column is BREAKING (historical data lacks it)
                changes.append(SchemaChange(
                    change_type=ChangeType.ADD_COLUMN,
                    field_name=col_name,
                    old_value=None,
                    new_value=new_spec.get('type', 'STRING'),
                    compatibility=CompatibilityLevel.BREAKING,
                    reason=f"Added required column '{col_name}' (historical data lacks it)"
                ))
            else:
                # Adding optional column is ENHANCEMENT (needs backfill)
                changes.append(SchemaChange(
                    change_type=ChangeType.ADD_COLUMN,
                    field_name=col_name,
                    old_value=None,
                    new_value=new_spec.get('type', 'STRING'),
                    compatibility=CompatibilityLevel.ENHANCEMENT,
                    reason=f"Added optional column '{col_name}' (backfill with NULL)"
                ))

    # 3. Check for type changes in existing columns
    for col_name in old_columns:
        if col_name in new_columns:
            old_spec = old_columns[col_name]
            new_spec = new_columns[col_name]

            old_type = old_spec.get('type', 'STRING')
            new_type = new_spec.get('type', 'STRING')

            if old_type != new_type:
                is_compat, reason = is_type_compatible(old_type, new_type)

                changes.append(SchemaChange(
                    change_type=ChangeType.TYPE_CHANGE,
                    field_name=col_name,
                    old_value=old_type,
                    new_value=new_type,
                    compatibility=CompatibilityLevel.ENHANCEMENT if is_compat else CompatibilityLevel.BREAKING,
                    reason=reason
                ))

            # 4. Check for required/nullable changes
            old_required = old_spec.get('required', False) or old_spec.get('cde', False)
            new_required = new_spec.get('required', False) or new_spec.get('cde', False)

            if old_required != new_required:
                if new_required:
                    # Making field required is BREAKING (historical NULLs fail)
                    changes.append(SchemaChange(
                        change_type=ChangeType.REQUIRED_CHANGE,
                        field_name=col_name,
                        old_value="optional",
                        new_value="required",
                        compatibility=CompatibilityLevel.BREAKING,
                        reason=f"Changed '{col_name}' from optional to required (historical NULLs)"
                    ))
                else:
                    # Making field optional is COMPATIBLE (safe relaxation)
                    changes.append(SchemaChange(
                        change_type=ChangeType.REQUIRED_CHANGE,
                        field_name=col_name,
                        old_value="required",
                        new_value="optional",
                        compatibility=CompatibilityLevel.COMPATIBLE,
                        reason=f"Relaxed '{col_name}' from required to optional"
                    ))

    # Build result
    has_breaking = any(c.compatibility == CompatibilityLevel.BREAKING for c in changes)
    has_enhancements = any(c.compatibility == CompatibilityLevel.ENHANCEMENT for c in changes)

    # Backfill needed if:
    # - New columns added (need to add NULL to historical data)
    # - Type widened (need to recast historical data)
    requires_backfill = has_enhancements and any(
        c.change_type in (ChangeType.ADD_COLUMN, ChangeType.TYPE_CHANGE)
        for c in changes
        if c.compatibility == CompatibilityLevel.ENHANCEMENT
    )

    return ValidationResult(
        is_compatible=not has_breaking,
        is_enhancement=has_enhancements,
        has_breaking_changes=has_breaking,
        changes=changes,
        requires_backfill=requires_backfill
    )


def _get_all_columns(schema: Dict) -> Dict:
    """
    Extract all columns from schema (envelope + payload).

    Returns:
        Dict[column_name, column_spec]
    """
    all_columns = {}

    # Envelope columns
    envelope = schema.get('envelope_columns', {})
    all_columns.update(envelope)

    # Payload columns
    payload = schema.get('payload_columns', {})
    all_columns.update(payload)

    return all_columns


# =============================================================================
# VALIDATION HELPERS
# =============================================================================

def format_validation_report(result: ValidationResult) -> str:
    """
    Format validation result as human-readable report.

    Used for error messages when schema fails validation.
    """
    lines = []
    lines.append("=" * 80)
    lines.append("SCHEMA COMPATIBILITY VALIDATION REPORT")
    lines.append("=" * 80)

    if result.is_compatible:
        lines.append("✅ Status: COMPATIBLE")
    else:
        lines.append("❌ Status: BREAKING CHANGES DETECTED")

    lines.append(f"   Enhancement: {result.is_enhancement}")
    lines.append(f"   Requires Backfill: {result.requires_backfill}")
    lines.append("")

    if result.breaking_changes:
        lines.append("🚨 BREAKING CHANGES:")
        for change in result.breaking_changes:
            lines.append(f"   - {change.field_name}: {change.reason}")
        lines.append("")

    if result.enhancements:
        lines.append("✨ ENHANCEMENTS:")
        for change in result.enhancements:
            lines.append(f"   - {change.field_name}: {change.reason}")
        lines.append("")

    compatible_changes = [c for c in result.changes if c.compatibility == CompatibilityLevel.COMPATIBLE]
    if compatible_changes:
        lines.append("ℹ️  COMPATIBLE CHANGES:")
        for change in compatible_changes:
            lines.append(f"   - {change.field_name}: {change.reason}")

    lines.append("=" * 80)
    return "\n".join(lines)


def get_backfill_config(schema: Dict, result: ValidationResult) -> Dict:
    """
    Extract backfill configuration from schema.

    Returns config dict with lookback_days, strategy, etc.
    """
    default_config = {
        'enabled': result.requires_backfill,
        'lookback_days': 7,  # Default: patch last 7 days
        'strategy': 'incremental',
        'chunk_days': 1,  # Process 1 day at a time
        'new_columns': [
            c.field_name for c in result.changes
            if c.change_type == ChangeType.ADD_COLUMN
        ],
        'type_changes': [
            {'field': c.field_name, 'old_type': c.old_value, 'new_type': c.new_value}
            for c in result.changes
            if c.change_type == ChangeType.TYPE_CHANGE
        ]
    }

    # Override with schema-defined config
    backfill_config = schema.get('backfill_config', {})
    default_config.update(backfill_config)

    return default_config

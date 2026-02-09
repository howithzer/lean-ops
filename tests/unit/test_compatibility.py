"""
Unit tests for schema backward compatibility validation.

Tests type compatibility matrix, change detection, and validation logic.
"""

import sys
import os
import pytest

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../modules/compute/lambda/schema_validator'))

from compatibility import (
    is_type_compatible,
    validate_backward_compatibility,
    ChangeType,
    CompatibilityLevel,
    format_validation_report,
    get_backfill_config
)


class TestTypeCompatibility:
    """Test type compatibility matrix."""

    def test_exact_match(self):
        """Exact type match is compatible."""
        is_compat, reason = is_type_compatible('STRING', 'STRING')
        assert is_compat is True
        assert 'No type change' in reason

    def test_safe_widening_int_to_bigint(self):
        """INT → BIGINT is safe widening."""
        is_compat, reason = is_type_compatible('INT', 'BIGINT')
        assert is_compat is True
        assert 'Safe widening' in reason

    def test_safe_widening_int_to_string(self):
        """INT → STRING is safe (can always stringify)."""
        is_compat, reason = is_type_compatible('INT', 'STRING')
        assert is_compat is True

    def test_unsafe_narrowing_bigint_to_int(self):
        """BIGINT → INT is unsafe (overflow risk)."""
        is_compat, reason = is_type_compatible('BIGINT', 'INT')
        assert is_compat is False
        assert 'Unsafe narrowing' in reason
        assert 'overflow' in reason.lower() or 'data loss' in reason.lower()

    def test_unsafe_string_to_int(self):
        """STRING → INT is unsafe (parse failure risk)."""
        is_compat, reason = is_type_compatible('STRING', 'INT')
        assert is_compat is False
        assert 'parse error' in reason.lower() or 'data loss' in reason.lower()

    def test_timestamp_to_string(self):
        """TIMESTAMP → STRING is safe."""
        is_compat, reason = is_type_compatible('TIMESTAMP', 'STRING')
        assert is_compat is True

    def test_string_to_timestamp(self):
        """STRING → TIMESTAMP is unsafe."""
        is_compat, reason = is_type_compatible('STRING', 'TIMESTAMP')
        assert is_compat is False

    def test_decimal_widening(self):
        """DECIMAL(10,2) → DECIMAL(12,2) is safe."""
        is_compat, reason = is_type_compatible('DECIMAL(10,2)', 'DECIMAL(12,2)')
        assert is_compat is True
        assert 'DECIMAL' in reason

    def test_decimal_narrowing(self):
        """DECIMAL(12,2) → DECIMAL(10,2) is unsafe."""
        is_compat, reason = is_type_compatible('DECIMAL(12,2)', 'DECIMAL(10,2)')
        assert is_compat is False
        assert 'narrowing' in reason.lower()


class TestBackwardCompatibility:
    """Test backward compatibility validation."""

    def test_add_optional_column(self):
        """Adding optional column is an enhancement."""
        old_schema = {
            'payload_columns': {
                'user_id': {'type': 'INT', 'required': False}
            }
        }
        new_schema = {
            'payload_columns': {
                'user_id': {'type': 'INT', 'required': False},
                'device_type': {'type': 'STRING', 'required': False}
            }
        }

        result = validate_backward_compatibility(old_schema, new_schema)

        assert result.is_compatible is True
        assert result.is_enhancement is True
        assert result.has_breaking_changes is False
        assert result.requires_backfill is True
        assert len(result.changes) == 1
        assert result.changes[0].change_type == ChangeType.ADD_COLUMN
        assert result.changes[0].field_name == 'device_type'
        assert result.changes[0].compatibility == CompatibilityLevel.ENHANCEMENT

    def test_add_required_column(self):
        """Adding required column is a breaking change."""
        old_schema = {
            'payload_columns': {
                'user_id': {'type': 'INT'}
            }
        }
        new_schema = {
            'payload_columns': {
                'user_id': {'type': 'INT'},
                'mandatory_field': {'type': 'STRING', 'required': True}
            }
        }

        result = validate_backward_compatibility(old_schema, new_schema)

        assert result.is_compatible is False
        assert result.has_breaking_changes is True
        assert len(result.breaking_changes) == 1
        assert 'mandatory_field' in result.breaking_changes[0].field_name

    def test_remove_column(self):
        """Removing column is a breaking change."""
        old_schema = {
            'payload_columns': {
                'user_id': {'type': 'INT'},
                'session_id': {'type': 'STRING'}
            }
        }
        new_schema = {
            'payload_columns': {
                'user_id': {'type': 'INT'}
            }
        }

        result = validate_backward_compatibility(old_schema, new_schema)

        assert result.is_compatible is False
        assert result.has_breaking_changes is True
        assert any(c.field_name == 'session_id' for c in result.breaking_changes)
        assert any(c.change_type == ChangeType.REMOVE_COLUMN for c in result.changes)

    def test_widen_type(self):
        """Widening type is an enhancement."""
        old_schema = {
            'payload_columns': {
                'count': {'type': 'INT'}
            }
        }
        new_schema = {
            'payload_columns': {
                'count': {'type': 'BIGINT'}
            }
        }

        result = validate_backward_compatibility(old_schema, new_schema)

        assert result.is_compatible is True
        assert result.is_enhancement is True
        assert result.requires_backfill is True
        type_change = [c for c in result.changes if c.change_type == ChangeType.TYPE_CHANGE][0]
        assert 'widening' in type_change.reason.lower()

    def test_narrow_type(self):
        """Narrowing type is a breaking change."""
        old_schema = {
            'payload_columns': {
                'count': {'type': 'BIGINT'}
            }
        }
        new_schema = {
            'payload_columns': {
                'count': {'type': 'INT'}
            }
        }

        result = validate_backward_compatibility(old_schema, new_schema)

        assert result.is_compatible is False
        assert result.has_breaking_changes is True
        type_change = [c for c in result.breaking_changes if c.change_type == ChangeType.TYPE_CHANGE][0]
        assert 'narrowing' in type_change.reason.lower()

    def test_make_field_required(self):
        """Changing optional → required is breaking."""
        old_schema = {
            'payload_columns': {
                'email': {'type': 'STRING', 'required': False}
            }
        }
        new_schema = {
            'payload_columns': {
                'email': {'type': 'STRING', 'required': True}
            }
        }

        result = validate_backward_compatibility(old_schema, new_schema)

        assert result.is_compatible is False
        assert result.has_breaking_changes is True

    def test_make_field_optional(self):
        """Changing required → optional is compatible."""
        old_schema = {
            'payload_columns': {
                'email': {'type': 'STRING', 'required': True}
            }
        }
        new_schema = {
            'payload_columns': {
                'email': {'type': 'STRING', 'required': False}
            }
        }

        result = validate_backward_compatibility(old_schema, new_schema)

        assert result.is_compatible is True
        assert result.has_breaking_changes is False

    def test_no_changes(self):
        """Identical schemas are compatible."""
        schema = {
            'payload_columns': {
                'user_id': {'type': 'INT'},
                'name': {'type': 'STRING'}
            }
        }

        result = validate_backward_compatibility(schema, schema)

        assert result.is_compatible is True
        assert result.is_enhancement is False
        assert result.has_breaking_changes is False
        assert len(result.changes) == 0
        assert result.requires_backfill is False

    def test_multiple_changes(self):
        """Multiple changes are all detected."""
        old_schema = {
            'envelope_columns': {
                'message_id': {'type': 'STRING'}
            },
            'payload_columns': {
                'user_id': {'type': 'INT'},
                'count': {'type': 'INT'}
            }
        }
        new_schema = {
            'envelope_columns': {
                'message_id': {'type': 'STRING'}
            },
            'payload_columns': {
                'user_id': {'type': 'BIGINT'},  # Widened
                'count': {'type': 'INT'},
                'new_field': {'type': 'STRING'}  # Added
            }
        }

        result = validate_backward_compatibility(old_schema, new_schema)

        assert result.is_compatible is True
        assert result.is_enhancement is True
        assert len(result.changes) == 2  # Type change + new column
        assert result.requires_backfill is True

    def test_cde_field_change(self):
        """Changing CDE status is detected."""
        old_schema = {
            'payload_columns': {
                'user_id': {'type': 'INT', 'cde': False}
            }
        }
        new_schema = {
            'payload_columns': {
                'user_id': {'type': 'INT', 'cde': True}
            }
        }

        result = validate_backward_compatibility(old_schema, new_schema)

        assert result.is_compatible is False  # Making CDE=true is like making required
        assert result.has_breaking_changes is True


class TestValidationReport:
    """Test validation report formatting."""

    def test_format_breaking_changes(self):
        """Breaking changes are formatted correctly."""
        old_schema = {
            'payload_columns': {
                'count': {'type': 'BIGINT'}
            }
        }
        new_schema = {
            'payload_columns': {
                'count': {'type': 'INT'}
            }
        }

        result = validate_backward_compatibility(old_schema, new_schema)
        report = format_validation_report(result)

        assert '❌' in report or 'BREAKING' in report
        assert 'count' in report
        assert 'narrowing' in report.lower()

    def test_format_enhancements(self):
        """Enhancements are formatted correctly."""
        old_schema = {
            'payload_columns': {
                'user_id': {'type': 'INT'}
            }
        }
        new_schema = {
            'payload_columns': {
                'user_id': {'type': 'INT'},
                'device': {'type': 'STRING'}
            }
        }

        result = validate_backward_compatibility(old_schema, new_schema)
        report = format_validation_report(result)

        assert '✅' in report or 'COMPATIBLE' in report
        assert 'device' in report
        assert 'ENHANCEMENTS' in report or 'Enhancement' in report


class TestBackfillConfig:
    """Test backfill configuration extraction."""

    def test_backfill_config_with_new_columns(self):
        """Backfill config includes new columns."""
        schema = {
            'payload_columns': {
                'user_id': {'type': 'INT'},
                'device': {'type': 'STRING'}
            }
        }
        old_schema = {
            'payload_columns': {
                'user_id': {'type': 'INT'}
            }
        }

        result = validate_backward_compatibility(old_schema, schema)
        config = get_backfill_config(schema, result)

        assert config['enabled'] is True
        assert 'device' in config['new_columns']
        assert config['lookback_days'] == 7  # Default

    def test_backfill_config_custom(self):
        """Custom backfill config is used."""
        schema = {
            'payload_columns': {
                'user_id': {'type': 'INT'},
                'device': {'type': 'STRING'}
            },
            'backfill_config': {
                'lookback_days': 30,
                'strategy': 'full',
                'chunk_days': 7
            }
        }
        old_schema = {
            'payload_columns': {
                'user_id': {'type': 'INT'}
            }
        }

        result = validate_backward_compatibility(old_schema, schema)
        config = get_backfill_config(schema, result)

        assert config['lookback_days'] == 30
        assert config['strategy'] == 'full'
        assert config['chunk_days'] == 7


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

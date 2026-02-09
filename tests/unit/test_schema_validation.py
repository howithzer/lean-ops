"""
Unit tests for schema validation logic.

Tests basic schema structure validation without AWS dependencies.
Uses mocking for S3/DynamoDB/Athena calls.
"""

import sys
import os
import json
import pytest
from unittest.mock import Mock, patch, MagicMock

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../modules/compute/lambda/schema_validator'))

# Mock boto3 before importing handler
sys.modules['boto3'] = MagicMock()

from handler_enhanced import (
    validate_schema_structure,
    extract_topic_from_key,
    load_schema_from_s3,
    get_backfill_config
)
from compatibility import validate_backward_compatibility


class TestSchemaStructureValidation:
    """Test basic schema structure validation."""

    def test_valid_schema(self):
        """Valid schema passes all checks."""
        schema = {
            'table_name': 'events',
            'envelope_columns': {
                'message_id': {'type': 'STRING', 'required': True}
            },
            'payload_columns': {
                'user_id': {'type': 'INT'}
            }
        }

        errors = validate_schema_structure(schema)
        assert len(errors) == 0

    def test_missing_table_name(self):
        """Schema missing table_name fails."""
        schema = {
            'envelope_columns': {},
            'payload_columns': {}
        }

        errors = validate_schema_structure(schema)
        assert len(errors) > 0
        assert any('table_name' in e['field'] for e in errors)

    def test_missing_envelope_columns(self):
        """Schema missing envelope_columns fails."""
        schema = {
            'table_name': 'events',
            'payload_columns': {}
        }

        errors = validate_schema_structure(schema)
        assert len(errors) > 0
        assert any('envelope_columns' in e['field'] for e in errors)

    def test_missing_payload_columns(self):
        """Schema missing payload_columns fails."""
        schema = {
            'table_name': 'events',
            'envelope_columns': {}
        }

        errors = validate_schema_structure(schema)
        assert len(errors) > 0
        assert any('payload_columns' in e['field'] for e in errors)

    def test_invalid_cde_fields(self):
        """CDE fields not in schema are flagged."""
        schema = {
            'table_name': 'events',
            'envelope_columns': {
                'message_id': {'type': 'STRING'}
            },
            'payload_columns': {
                'user_id': {'type': 'INT'}
            },
            'cde_fields': ['nonexistent_field']
        }

        errors = validate_schema_structure(schema)
        assert len(errors) > 0
        assert any('INVALID_CDE' in e['type'] for e in errors)


class TestTopicExtraction:
    """Test topic name extraction from S3 keys."""

    def test_valid_pending_key(self):
        """Extract topic from pending/ key."""
        key = 'schemas/events/pending/schema.json'
        topic = extract_topic_from_key(key)
        assert topic == 'events'

    def test_valid_active_key(self):
        """Extract topic from active/ key."""
        key = 'schemas/orders/active/schema.json'
        topic = extract_topic_from_key(key)
        assert topic == 'orders'

    def test_invalid_key(self):
        """Invalid key returns None."""
        key = 'invalid/path/schema.json'
        topic = extract_topic_from_key(key)
        assert topic is None

    def test_short_key(self):
        """Short key returns None."""
        key = 'schemas/events'
        topic = extract_topic_from_key(key)
        assert topic is None


class TestIntegrationScenarios:
    """Integration tests for common scenarios."""

    def test_first_schema_registration(self):
        """First schema registration (no active schema)."""
        new_schema = {
            'table_name': 'events',
            'envelope_columns': {
                'message_id': {'type': 'STRING', 'required': True}
            },
            'payload_columns': {
                'user_id': {'type': 'INT'}
            }
        }

        # Validate structure
        errors = validate_schema_structure(new_schema)
        assert len(errors) == 0

        # No backward compatibility check needed (first registration)

    def test_schema_enhancement(self):
        """Schema enhancement with new column."""
        old_schema = {
            'table_name': 'events',
            'envelope_columns': {
                'message_id': {'type': 'STRING'}
            },
            'payload_columns': {
                'user_id': {'type': 'INT'}
            }
        }

        new_schema = {
            'table_name': 'events',
            'envelope_columns': {
                'message_id': {'type': 'STRING'}
            },
            'payload_columns': {
                'user_id': {'type': 'INT'},
                'device_type': {'type': 'STRING'}
            }
        }

        # Validate structure
        errors = validate_schema_structure(new_schema)
        assert len(errors) == 0

        # Validate backward compatibility
        result = validate_backward_compatibility(old_schema, new_schema)
        assert result.is_compatible is True
        assert result.is_enhancement is True
        assert result.requires_backfill is True

    def test_breaking_change_rejected(self):
        """Breaking change is rejected."""
        old_schema = {
            'table_name': 'events',
            'envelope_columns': {
                'message_id': {'type': 'STRING'}
            },
            'payload_columns': {
                'user_id': {'type': 'BIGINT'},
                'session_id': {'type': 'STRING'}
            }
        }

        new_schema = {
            'table_name': 'events',
            'envelope_columns': {
                'message_id': {'type': 'STRING'}
            },
            'payload_columns': {
                'user_id': {'type': 'INT'}  # Narrowed BIGINT → INT
            }
            # session_id removed
        }

        # Validate structure
        errors = validate_schema_structure(new_schema)
        assert len(errors) == 0

        # Validate backward compatibility
        result = validate_backward_compatibility(old_schema, new_schema)
        assert result.is_compatible is False
        assert result.has_breaking_changes is True
        assert len(result.breaking_changes) >= 2  # Type narrowing + removed column


class TestSchemaVersioning:
    """Test schema versioning scenarios."""

    def test_version_increment(self):
        """Schema version increments correctly."""
        v1 = {
            'schema_version': '1.0.0',
            'table_name': 'events',
            'envelope_columns': {'message_id': {'type': 'STRING'}},
            'payload_columns': {'user_id': {'type': 'INT'}}
        }

        v2 = {
            'schema_version': '2.0.0',
            'table_name': 'events',
            'envelope_columns': {'message_id': {'type': 'STRING'}},
            'payload_columns': {
                'user_id': {'type': 'INT'},
                'device': {'type': 'STRING'}
            }
        }

        # Both valid
        assert len(validate_schema_structure(v1)) == 0
        assert len(validate_schema_structure(v2)) == 0

        # v2 is enhancement of v1
        result = validate_backward_compatibility(v1, v2)
        assert result.is_compatible is True
        assert result.is_enhancement is True


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

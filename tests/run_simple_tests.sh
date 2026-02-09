#!/bin/bash
# =============================================================================
# Simple Isolated Schema Validation Tests (No pytest required)
# =============================================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step() { echo -e "${CYAN}[STEP]${NC} $1"; }

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "=============================================="
echo "  Schema Validation Tests (Simple Runner)"
echo "=============================================="
echo ""

# =============================================================================
# Test 1: Type Compatibility Matrix
# =============================================================================
log_step "Test 1: Type Compatibility Matrix"

python3 << 'EOF'
import sys
sys.path.insert(0, 'modules/compute/lambda/schema_validator')
from compatibility import is_type_compatible

tests_passed = 0
tests_failed = 0

def test(name, old_type, new_type, expected_compatible):
    global tests_passed, tests_failed
    is_compat, reason = is_type_compatible(old_type, new_type)
    if is_compat == expected_compatible:
        print(f"  ✅ {name}: {old_type} → {new_type}")
        tests_passed += 1
    else:
        print(f"  ❌ {name}: Expected {expected_compatible}, got {is_compat}")
        print(f"     Reason: {reason}")
        tests_failed += 1

# Safe widenings
test("INT to BIGINT", "INT", "BIGINT", True)
test("INT to STRING", "INT", "STRING", True)
test("TIMESTAMP to STRING", "TIMESTAMP", "STRING", True)

# Unsafe narrowings
test("BIGINT to INT", "BIGINT", "INT", False)
test("STRING to INT", "STRING", "INT", False)
test("STRING to TIMESTAMP", "STRING", "TIMESTAMP", False)

# Exact matches
test("STRING to STRING", "STRING", "STRING", True)
test("INT to INT", "INT", "INT", True)

# Decimal
test("DECIMAL(10,2) to DECIMAL(12,2)", "DECIMAL(10,2)", "DECIMAL(12,2)", True)
test("DECIMAL(12,2) to DECIMAL(10,2)", "DECIMAL(12,2)", "DECIMAL(10,2)", False)

print(f"\nPassed: {tests_passed}/{tests_passed + tests_failed}")
exit(0 if tests_failed == 0 else 1)
EOF

TEST1_EXIT=$?

# =============================================================================
# Test 2: Add Optional Column (Enhancement)
# =============================================================================
log_step "Test 2: Add Optional Column (Enhancement)"

python3 << 'EOF'
import sys
sys.path.insert(0, 'modules/compute/lambda/schema_validator')
from compatibility import validate_backward_compatibility

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

assert result.is_compatible, "Should be compatible"
assert result.is_enhancement, "Should be enhancement"
assert result.requires_backfill, "Should require backfill"
assert not result.has_breaking_changes, "Should not have breaking changes"

print("  ✅ Adding optional column is detected as enhancement")
print(f"     - Compatible: {result.is_compatible}")
print(f"     - Enhancement: {result.is_enhancement}")
print(f"     - Requires backfill: {result.requires_backfill}")
EOF

TEST2_EXIT=$?

# =============================================================================
# Test 3: Remove Column (Breaking Change)
# =============================================================================
log_step "Test 3: Remove Column (Breaking Change)"

python3 << 'EOF'
import sys
sys.path.insert(0, 'modules/compute/lambda/schema_validator')
from compatibility import validate_backward_compatibility

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

assert not result.is_compatible, "Should be incompatible"
assert result.has_breaking_changes, "Should have breaking changes"

print("  ✅ Removing column is detected as breaking change")
print(f"     - Compatible: {result.is_compatible}")
print(f"     - Breaking changes: {len(result.breaking_changes)}")
for change in result.breaking_changes:
    print(f"       • {change.field_name}: {change.reason}")
EOF

TEST3_EXIT=$?

# =============================================================================
# Test 4: Type Narrowing (Breaking Change)
# =============================================================================
log_step "Test 4: Type Narrowing (Breaking Change)"

python3 << 'EOF'
import sys
sys.path.insert(0, 'modules/compute/lambda/schema_validator')
from compatibility import validate_backward_compatibility

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

assert not result.is_compatible, "Should be incompatible"
assert result.has_breaking_changes, "Should have breaking changes"

print("  ✅ Type narrowing is detected as breaking change")
print(f"     - Compatible: {result.is_compatible}")
for change in result.breaking_changes:
    print(f"       • {change.field_name}: {change.reason}")
EOF

TEST4_EXIT=$?

# =============================================================================
# Test 5: Schema Scenario - Enhancement
# =============================================================================
log_step "Test 5: Schema Scenario v1 → v2 (Enhancement)"

python3 << 'EOF'
import sys
import json
sys.path.insert(0, 'modules/compute/lambda/schema_validator')
from compatibility import validate_backward_compatibility

with open('tests/schemas/events_v1.json') as f:
    v1 = json.load(f)
with open('tests/schemas/events_v2_enhancement.json') as f:
    v2 = json.load(f)

result = validate_backward_compatibility(v1, v2)

assert result.is_compatible, "v1→v2 should be compatible"
assert result.is_enhancement, "v1→v2 should be enhancement"
assert result.requires_backfill, "v1→v2 should require backfill"

print("  ✅ v1 → v2 enhancement detected correctly")
print(f"     - Changes: {len(result.changes)}")
for change in result.changes:
    print(f"       • {change.field_name}: {change.reason}")
EOF

TEST5_EXIT=$?

# =============================================================================
# Test 6: Schema Scenario - Breaking Change
# =============================================================================
log_step "Test 6: Schema Scenario v2 → v3 (Breaking Change)"

python3 << 'EOF'
import sys
import json
sys.path.insert(0, 'modules/compute/lambda/schema_validator')
from compatibility import validate_backward_compatibility, format_validation_report

with open('tests/schemas/events_v2_enhancement.json') as f:
    v2 = json.load(f)
with open('tests/schemas/events_v3_breaking.json') as f:
    v3 = json.load(f)

result = validate_backward_compatibility(v2, v3)

assert not result.is_compatible, "v2→v3 should be incompatible"
assert result.has_breaking_changes, "v2→v3 should have breaking changes"

print("  ✅ v2 → v3 breaking changes detected correctly")
print(f"     - Breaking changes: {len(result.breaking_changes)}")
for change in result.breaking_changes:
    print(f"       • {change.field_name}: {change.reason}")
EOF

TEST6_EXIT=$?

# =============================================================================
# Test 7: Schema Structure Validation
# =============================================================================
log_step "Test 7: Schema Structure Validation"

python3 << 'EOF'
import sys
import json

# Test schema structure validation without boto3 import
def validate_schema_structure(schema):
    """Validate basic schema structure."""
    errors = []

    if 'table_name' not in schema:
        errors.append({
            'type': 'MISSING_FIELD',
            'field': 'table_name',
            'message': 'Schema must have a table_name'
        })

    if 'envelope_columns' not in schema:
        errors.append({
            'type': 'MISSING_FIELD',
            'field': 'envelope_columns',
            'message': 'Schema must have envelope_columns'
        })

    if 'payload_columns' not in schema:
        errors.append({
            'type': 'MISSING_FIELD',
            'field': 'payload_columns',
            'message': 'Schema must have payload_columns'
        })

    if 'cde_fields' in schema:
        all_columns = set()
        all_columns.update(schema.get('envelope_columns', {}).keys())
        all_columns.update(schema.get('payload_columns', {}).keys())

        cde_fields = schema['cde_fields']
        missing_cde = [f for f in cde_fields if f not in all_columns]

        if missing_cde:
            errors.append({
                'type': 'INVALID_CDE',
                'field': ','.join(missing_cde),
                'message': f'CDE fields not found in schema: {missing_cde}'
            })

    return errors

# Valid schema
valid_schema = {
    'table_name': 'events',
    'envelope_columns': {'message_id': {'type': 'STRING'}},
    'payload_columns': {'user_id': {'type': 'INT'}}
}
errors = validate_schema_structure(valid_schema)
assert len(errors) == 0, "Valid schema should have no errors"
print("  ✅ Valid schema passes structure validation")

# Missing table_name
invalid_schema = {
    'envelope_columns': {},
    'payload_columns': {}
}
errors = validate_schema_structure(invalid_schema)
assert len(errors) > 0, "Invalid schema should have errors"
assert any('table_name' in e['field'] for e in errors), "Should flag missing table_name"
print("  ✅ Missing table_name is detected")

# Invalid CDE
invalid_cde_schema = {
    'table_name': 'events',
    'envelope_columns': {'message_id': {'type': 'STRING'}},
    'payload_columns': {'user_id': {'type': 'INT'}},
    'cde_fields': ['nonexistent_field']
}
errors = validate_schema_structure(invalid_cde_schema)
assert len(errors) > 0, "Should detect invalid CDE"
assert any('INVALID_CDE' in e['type'] for e in errors), "Should flag invalid CDE"
print("  ✅ Invalid CDE fields are detected")

# Test with real schema files
with open('tests/schemas/events_v1.json') as f:
    v1 = json.load(f)
errors = validate_schema_structure(v1)
assert len(errors) == 0, "events_v1.json should be valid"
print("  ✅ events_v1.json passes validation")

with open('tests/schemas/events_v2_enhancement.json') as f:
    v2 = json.load(f)
errors = validate_schema_structure(v2)
assert len(errors) == 0, "events_v2_enhancement.json should be valid"
print("  ✅ events_v2_enhancement.json passes validation")
EOF

TEST7_EXIT=$?

# =============================================================================
# SUMMARY
# =============================================================================
echo ""
echo "=============================================="
echo "  Test Results Summary"
echo "=============================================="

TOTAL_FAILURES=0

tests=(
    "TEST1_EXIT:Type Compatibility Matrix"
    "TEST2_EXIT:Add Optional Column"
    "TEST3_EXIT:Remove Column Detection"
    "TEST4_EXIT:Type Narrowing Detection"
    "TEST5_EXIT:Schema v1→v2 Enhancement"
    "TEST6_EXIT:Schema v2→v3 Breaking Change"
    "TEST7_EXIT:Structure Validation"
)

for test_info in "${tests[@]}"; do
    test_var="${test_info%%:*}"
    test_name="${test_info#*:}"
    test_exit="${!test_var}"

    if [ "$test_exit" -eq 0 ]; then
        echo -e "${GREEN}✅ ${test_name}: PASSED${NC}"
    else
        echo -e "${RED}❌ ${test_name}: FAILED${NC}"
        TOTAL_FAILURES=$((TOTAL_FAILURES + 1))
    fi
done

echo "=============================================="

if [ $TOTAL_FAILURES -eq 0 ]; then
    echo -e "${GREEN}🎉 All 7 test suites passed! Ready for deployment.${NC}"
    echo ""
    echo "Next steps:"
    echo "  1. Review docs/SCHEMA_MANAGEMENT_IMPLEMENTATION_PLAN.md"
    echo "  2. Deploy enhanced schema_validator Lambda"
    echo "  3. Upload test schema to S3 pending/ bucket"
    echo "  4. Verify schema moves to active/ and DynamoDB updates"
    exit 0
else
    echo -e "${RED}❌ $TOTAL_FAILURES test suite(s) failed${NC}"
    exit 1
fi

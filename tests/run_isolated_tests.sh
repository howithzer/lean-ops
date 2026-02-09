#!/bin/bash
# =============================================================================
# Isolated Schema Validation Tests
# =============================================================================
# Runs unit tests for schema validation without AWS dependencies.
# Tests compatibility module, validation logic, and schema scenarios.
# =============================================================================

set -e  # Exit on error

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step() { echo -e "${CYAN}[STEP]${NC} $1"; }

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "=============================================="
echo "  Isolated Schema Validation Tests"
echo "=============================================="
echo ""
log_info "Project root: $PROJECT_ROOT"
echo ""

# =============================================================================
# STEP 1: Check Dependencies
# =============================================================================
log_step "1/5 Checking dependencies..."

if ! command -v python3 &> /dev/null; then
    log_error "python3 not found. Please install Python 3.11+"
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
log_info "Python version: $PYTHON_VERSION"

# Check if pytest is installed
if ! python3 -m pytest --version &> /dev/null; then
    log_warn "pytest not found - attempting to install with --user flag..."
    python3 -m pip install pytest --user --quiet 2>/dev/null || {
        log_error "Failed to install pytest. Please install manually:"
        log_error "  python3 -m pip install pytest --user"
        log_error "Or create a virtual environment:"
        log_error "  python3 -m venv venv && source venv/bin/activate && pip install pytest"
        exit 1
    }
fi

log_info "✅ Dependencies OK"
echo ""

# =============================================================================
# STEP 2: Validate Test Files Exist
# =============================================================================
log_step "2/5 Validating test files..."

REQUIRED_FILES=(
    "modules/compute/lambda/schema_validator/compatibility.py"
    "modules/compute/lambda/schema_validator/handler_enhanced.py"
    "tests/unit/test_compatibility.py"
    "tests/unit/test_schema_validation.py"
    "tests/schemas/events_v1.json"
    "tests/schemas/events_v2_enhancement.json"
    "tests/schemas/events_v3_breaking.json"
)

MISSING_FILES=0
for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        log_error "Missing: $file"
        MISSING_FILES=$((MISSING_FILES + 1))
    else
        log_info "Found: $file"
    fi
done

if [ $MISSING_FILES -gt 0 ]; then
    log_error "$MISSING_FILES required files missing!"
    exit 1
fi

log_info "✅ All required files found"
echo ""

# =============================================================================
# STEP 3: Run Unit Tests - Compatibility Module
# =============================================================================
log_step "3/5 Running compatibility module tests..."
echo ""

python3 -m pytest tests/unit/test_compatibility.py -v --tb=short

COMPAT_EXIT=$?
if [ $COMPAT_EXIT -eq 0 ]; then
    log_info "✅ Compatibility tests passed"
else
    log_error "❌ Compatibility tests failed"
fi
echo ""

# =============================================================================
# STEP 4: Run Unit Tests - Schema Validation
# =============================================================================
log_step "4/5 Running schema validation tests..."
echo ""

python3 -m pytest tests/unit/test_schema_validation.py -v --tb=short

VALIDATION_EXIT=$?
if [ $VALIDATION_EXIT -eq 0 ]; then
    log_info "✅ Schema validation tests passed"
else
    log_error "❌ Schema validation tests failed"
fi
echo ""

# =============================================================================
# STEP 5: Run Schema Scenario Tests
# =============================================================================
log_step "5/5 Running schema scenario tests..."
echo ""

# Test Scenario 1: Valid enhancement
log_info "Testing scenario: v1 → v2 (enhancement)"
python3 << 'EOF'
import sys
import json
sys.path.insert(0, 'modules/compute/lambda/schema_validator')
from compatibility import validate_backward_compatibility, format_validation_report

with open('tests/schemas/events_v1.json') as f:
    v1 = json.load(f)
with open('tests/schemas/events_v2_enhancement.json') as f:
    v2 = json.load(f)

result = validate_backward_compatibility(v1, v2)
print("\n" + format_validation_report(result))

assert result.is_compatible, "v1→v2 should be compatible"
assert result.is_enhancement, "v1→v2 should be an enhancement"
assert result.requires_backfill, "v1→v2 should require backfill"
print("\n✅ Scenario 1 passed: Enhancement detected correctly")
EOF

SCENARIO1_EXIT=$?

# Test Scenario 2: Breaking change
log_info ""
log_info "Testing scenario: v2 → v3 (breaking change)"
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
print("\n" + format_validation_report(result))

assert not result.is_compatible, "v2→v3 should be incompatible"
assert result.has_breaking_changes, "v2→v3 should have breaking changes"
print("\n✅ Scenario 2 passed: Breaking changes detected correctly")
EOF

SCENARIO2_EXIT=$?

# =============================================================================
# SUMMARY
# =============================================================================
echo ""
echo "=============================================="
echo "  Test Results Summary"
echo "=============================================="

TOTAL_FAILURES=0

if [ $COMPAT_EXIT -eq 0 ]; then
    echo -e "${GREEN}✅ Compatibility Module Tests: PASSED${NC}"
else
    echo -e "${RED}❌ Compatibility Module Tests: FAILED${NC}"
    TOTAL_FAILURES=$((TOTAL_FAILURES + 1))
fi

if [ $VALIDATION_EXIT -eq 0 ]; then
    echo -e "${GREEN}✅ Schema Validation Tests: PASSED${NC}"
else
    echo -e "${RED}❌ Schema Validation Tests: FAILED${NC}"
    TOTAL_FAILURES=$((TOTAL_FAILURES + 1))
fi

if [ $SCENARIO1_EXIT -eq 0 ]; then
    echo -e "${GREEN}✅ Enhancement Scenario: PASSED${NC}"
else
    echo -e "${RED}❌ Enhancement Scenario: FAILED${NC}"
    TOTAL_FAILURES=$((TOTAL_FAILURES + 1))
fi

if [ $SCENARIO2_EXIT -eq 0 ]; then
    echo -e "${GREEN}✅ Breaking Change Scenario: PASSED${NC}"
else
    echo -e "${RED}❌ Breaking Change Scenario: FAILED${NC}"
    TOTAL_FAILURES=$((TOTAL_FAILURES + 1))
fi

echo "=============================================="

if [ $TOTAL_FAILURES -eq 0 ]; then
    echo -e "${GREEN}🎉 All tests passed! Ready for deployment.${NC}"
    echo ""
    echo "Next steps:"
    echo "  1. Review the deployment guide in SCHEMA_MANAGEMENT_IMPLEMENTATION_PLAN.md"
    echo "  2. Deploy Lambda functions and Terraform infrastructure"
    echo "  3. Run E2E tests in your environment"
    exit 0
else
    echo -e "${RED}❌ $TOTAL_FAILURES test suite(s) failed${NC}"
    echo ""
    echo "Please fix the failing tests before deploying."
    exit 1
fi

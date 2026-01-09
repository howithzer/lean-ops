#!/bin/bash
# =============================================================================
# Lean-Ops Test Runner
# =============================================================================
# Run all test scenarios using the sample-data-generator
#
# Usage:
#   ./run_tests.sh [test_number]
#   ./run_tests.sh           # Run all tests
#   ./run_tests.sh 01        # Run test 01 only
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GENERATOR_DIR="/Users/sagarm/Documents/Projects/Huntington_Exercises/Testing_Framework_EEH_2.0/sample-data-generator"
CONFIGS_DIR="$SCRIPT_DIR/configs"
OUTPUT_DIR="$SCRIPT_DIR/output"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "============================================"
echo "  Lean-Ops Test Runner"
echo "============================================"
echo ""

# Check generator exists
if [ ! -d "$GENERATOR_DIR" ]; then
    echo -e "${RED}ERROR: Generator not found at $GENERATOR_DIR${NC}"
    exit 1
fi

# Activate venv
if [ -d "$GENERATOR_DIR/.venv" ]; then
    source "$GENERATOR_DIR/.venv/bin/activate"
    echo -e "${GREEN}✓ Virtual environment activated${NC}"
else
    echo -e "${YELLOW}⚠ No virtual environment found, using system Python${NC}"
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Function to run a test
run_test() {
    local test_name=$1
    local config_file="$CONFIGS_DIR/$test_name.json"
    
    if [ ! -f "$config_file" ]; then
        echo -e "${RED}✗ Config not found: $config_file${NC}"
        return 1
    fi
    
    echo ""
    echo -e "${YELLOW}Running: $test_name${NC}"
    echo "----------------------------------------"
    
    # Run generator
    cd "$GENERATOR_DIR"
    python -m data_injector.main --config "$config_file"
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ $test_name completed${NC}"
    else
        echo -e "${RED}✗ $test_name failed${NC}"
        return 1
    fi
}

# Run tests
if [ -n "$1" ]; then
    # Run specific test
    run_test "test_$1*" 2>/dev/null || run_test "test_0$1*" 2>/dev/null || {
        echo -e "${RED}Test $1 not found${NC}"
        exit 1
    }
else
    # Run all tests
    for config in "$CONFIGS_DIR"/test_*.json; do
        test_name=$(basename "$config" .json)
        run_test "$test_name"
    done
fi

echo ""
echo "============================================"
echo -e "${GREEN}  All tests completed!${NC}"
echo "============================================"
echo ""
echo "Output files: $OUTPUT_DIR"

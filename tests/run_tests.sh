#!/bin/bash
# ==============================================================================
# TEST RUNNER SCRIPT
# ==============================================================================
# What does this script do? (ELI5)
# --------------------------------
# Think of it like a robot that runs through a checklist:
# 1. Finds all test config files in tests/configs/
# 2. For each one, runs the data injector with that config
# 3. Reports which tests passed or failed
#
# Why do we need this?
# Instead of manually running each test, we automate it.
# One command runs ALL tests.
#
# Usage:
#   ./tests/run_tests.sh           # Run ALL tests
#   ./tests/run_tests.sh 01        # Run test_01 only
#   ./tests/run_tests.sh 05        # Run test_05 only
#
# Prerequisites:
#   - sample-data-generator must be set up (../Testing_Framework_EEH_2.0)
#   - Python virtual environment with data_injector installed
# ==============================================================================

# Exit immediately if any command fails
set -e

# ==============================================================================
# CONFIGURATION
# ==============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GENERATOR_DIR="/Users/sagarm/Documents/Projects/Huntington_Exercises/Testing_Framework_EEH_2.0/sample-data-generator"
CONFIGS_DIR="$SCRIPT_DIR/configs"
OUTPUT_DIR="$SCRIPT_DIR/output"

# Colors for pretty output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color (reset)

echo "============================================"
echo "  Lean-Ops Test Runner"
echo "============================================"
echo ""

# ==============================================================================
# STEP 1: Check that the data generator exists
# ==============================================================================
if [ ! -d "$GENERATOR_DIR" ]; then
    echo -e "${RED}ERROR: Data generator not found at $GENERATOR_DIR${NC}"
    echo "Please clone/setup the Testing_Framework_EEH_2.0 repo first."
    exit 1
fi

# ==============================================================================
# STEP 2: Activate Python virtual environment (if it exists)
# ==============================================================================
if [ -d "$GENERATOR_DIR/.venv" ]; then
    source "$GENERATOR_DIR/.venv/bin/activate"
    echo -e "${GREEN}✓ Virtual environment activated${NC}"
else
    echo -e "${YELLOW}⚠ No virtual environment found, using system Python${NC}"
fi

# ==============================================================================
# STEP 3: Create output directory for test results
# ==============================================================================
mkdir -p "$OUTPUT_DIR"

# ==============================================================================
# FUNCTION: Run a single test
# ==============================================================================
# This function:
# 1. Checks if the config file exists
# 2. Runs the data injector with that config
# 3. Reports success or failure
# ==============================================================================
run_test() {
    local test_name=$1
    local config_file="$CONFIGS_DIR/$test_name.json"
    
    # Check if config exists
    if [ ! -f "$config_file" ]; then
        echo -e "${RED}✗ Config not found: $config_file${NC}"
        return 1
    fi
    
    echo ""
    echo -e "${YELLOW}Running: $test_name${NC}"
    echo "----------------------------------------"
    
    # Change to generator directory and run
    cd "$GENERATOR_DIR"
    python -m data_injector.main --config "$config_file"
    
    # Check exit code
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ $test_name completed successfully${NC}"
    else
        echo -e "${RED}✗ $test_name FAILED${NC}"
        return 1
    fi
}

# ==============================================================================
# MAIN: Run tests based on arguments
# ==============================================================================
if [ -n "$1" ]; then
    # User specified a test number (e.g., "01" or "1")
    # Try to find a matching config file
    run_test "test_$1*" 2>/dev/null || run_test "test_0$1*" 2>/dev/null || {
        echo -e "${RED}Test $1 not found${NC}"
        echo "Available tests:"
        ls "$CONFIGS_DIR"/*.json 2>/dev/null | xargs -n1 basename
        exit 1
    }
else
    # No argument = run ALL tests
    for config in "$CONFIGS_DIR"/test_*.json; do
        test_name=$(basename "$config" .json)
        run_test "$test_name"
    done
fi

# ==============================================================================
# SUMMARY
# ==============================================================================
echo ""
echo "============================================"
echo -e "${GREEN}  All tests completed!${NC}"
echo "============================================"
echo ""
echo "Output files: $OUTPUT_DIR"
echo ""
echo "Next steps:"
echo "  1. Check Athena for data in iceberg_raw_db.events_staging"
echo "  2. Trigger Step Function to process: ./tests/e2e/production_emulation.sh trigger"

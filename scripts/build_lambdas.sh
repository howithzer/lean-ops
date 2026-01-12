#!/bin/bash
# =============================================================================
# Build Lambda packages with common library included
# =============================================================================
# This script prepares Lambda deployment packages by copying each handler
# along with the shared common library into the .build directory.
#
# Usage: ./scripts/build_lambdas.sh
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LAMBDA_DIR="$PROJECT_ROOT/modules/compute/lambda"
BUILD_DIR="$PROJECT_ROOT/modules/compute/.build"

# List of Lambdas that need the common library
LAMBDAS=(
    "sqs_processor"
    "dlq_processor"
    "firehose_transform"
    "get_all_checkpoints"
    "circuit_breaker"
    "check_schema"
)

echo "Building Lambda packages..."
echo "Source: $LAMBDA_DIR"
echo "Output: $BUILD_DIR"
echo ""

for lambda in "${LAMBDAS[@]}"; do
    echo "Building $lambda..."
    
    # Clean and create build directory
    rm -rf "$BUILD_DIR/$lambda"
    mkdir -p "$BUILD_DIR/$lambda"
    
    # Copy Lambda handler(s)
    if [ -d "$LAMBDA_DIR/$lambda" ]; then
        cp -r "$LAMBDA_DIR/$lambda/"* "$BUILD_DIR/$lambda/"
    else
        echo "  Warning: $LAMBDA_DIR/$lambda not found, skipping"
        continue
    fi
    
    # Copy common library
    if [ -d "$LAMBDA_DIR/common" ]; then
        cp -r "$LAMBDA_DIR/common" "$BUILD_DIR/$lambda/"
    else
        echo "  Warning: common library not found"
    fi
    
    echo "  → $BUILD_DIR/$lambda/"
done

echo ""
echo "Build complete! Run 'terraform plan' to verify."

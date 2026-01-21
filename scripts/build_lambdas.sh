#!/bin/bash
# ==============================================================================
# BUILD LAMBDAS SCRIPT
# ==============================================================================
# What does this script do? (ELI5)
# ---------------------------------
# Think of it like packing boxes for shipping:
# 1. For each Lambda, create a folder (.build/lambda_name/)
# 2. Copy the Lambda code into the folder
# 3. Copy the shared "common" library (utilities everyone uses)
# 4. Now each folder is ready to be zipped and deployed by Terraform
#
# Why bundle the common library?
# ------------------------------
# Lambda functions can't import from other folders. We need to copy the
# common utilities INTO each Lambda folder so they can access them.
#
# Usage:
#   ./scripts/build_lambdas.sh
#
# After running:
#   1. Run 'terraform plan' to see changes
#   2. Run 'terraform apply' to deploy
# ==============================================================================

# Exit immediately if any command fails
set -e

# Figure out where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LAMBDA_DIR="$PROJECT_ROOT/modules/compute/lambda"
BUILD_DIR="$PROJECT_ROOT/modules/compute/.build"

# List of Lambda functions to build
# Each one will get its own folder with code + common library
LAMBDAS=(
    "sqs_processor"          # Reads SQS, sends to Firehose
    "dlq_processor"          # Archives failed messages
    "firehose_transform"     # Adds routing metadata for Iceberg tables
    "get_all_checkpoints"    # Gets checkpoint info for Step Functions
    "circuit_breaker"        # Disables SQS triggers if error rate too high
    "check_schema"           # Checks if schema file exists in S3
    "update_checkpoint"      # Updates checkpoint after processing
)

echo "========================================"
echo "Building Lambda Packages"
echo "========================================"
echo "Source: $LAMBDA_DIR"
echo "Output: $BUILD_DIR"
echo ""

# Loop through each Lambda and build it
for lambda in "${LAMBDAS[@]}"; do
    echo "📦 Building: $lambda"
    
    # Step 1: Clean up old build and create fresh folder
    rm -rf "$BUILD_DIR/$lambda"
    mkdir -p "$BUILD_DIR/$lambda"
    
    # Step 2: Copy the Lambda's code
    if [ -d "$LAMBDA_DIR/$lambda" ]; then
        cp -r "$LAMBDA_DIR/$lambda/"* "$BUILD_DIR/$lambda/"
    else
        echo "   ⚠️  Warning: $LAMBDA_DIR/$lambda not found, skipping"
        continue
    fi
    
    # Step 3: Copy the shared common library
    if [ -d "$LAMBDA_DIR/common" ]; then
        cp -r "$LAMBDA_DIR/common" "$BUILD_DIR/$lambda/"
    else
        echo "   ⚠️  Warning: common library not found"
    fi
    
    echo "   ✅ Done → $BUILD_DIR/$lambda/"
done

echo ""
echo "========================================"
echo "Build Complete!"
echo "========================================"
echo "Next steps:"
echo "  1. terraform plan  (preview changes)"
echo "  2. terraform apply (deploy)"
echo ""

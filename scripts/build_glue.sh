#!/bin/bash
# =============================================================================
# Build Glue Job Package
# =============================================================================
# Creates the deployment artifacts for the Curated Processor Glue job:
# 1. curated_processor.py - Main script
# 2. glue_libs.zip - utils/ package as extra Python files
#
# Usage:
#   ./scripts/build_glue.sh
#   ./scripts/build_glue.sh --upload  # Also upload to S3
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
GLUE_DIR="$PROJECT_ROOT/scripts/glue"
BUILD_DIR="$PROJECT_ROOT/build/glue"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# =============================================================================
# Create build directory
# =============================================================================
log_info "Creating build directory: $BUILD_DIR"
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# =============================================================================
# Copy main script
# =============================================================================
log_info "Copying main script..."
cp "$GLUE_DIR/curated_processor.py" "$BUILD_DIR/"

# =============================================================================
# Create glue_libs.zip with utils package
# =============================================================================
log_info "Creating glue_libs.zip with utils package..."
cd "$GLUE_DIR"

# Create the zip (Glue expects the package structure inside)
zip -r "$BUILD_DIR/glue_libs.zip" utils/ -x "*.pyc" -x "*__pycache__*" -x "*.DS_Store"

cd "$PROJECT_ROOT"

# =============================================================================
# Show build artifacts
# =============================================================================
log_info "Build complete! Artifacts:"
ls -lh "$BUILD_DIR/"

# =============================================================================
# Upload to S3 (optional)
# =============================================================================
if [[ "${1:-}" == "--upload" ]]; then
    # Get bucket from tfvars
    BUCKET=$(grep 'iceberg_bucket' "$PROJECT_ROOT/environments/dev.tfvars" | sed 's/.*"\(.*\)".*/\1/' || echo "lean-ops-development-iceberg")
    
    log_info "Uploading to s3://$BUCKET/glue-scripts/..."
    
    AWS_PROFILE="${AWS_PROFILE:-terraform-firehose}" aws s3 cp "$BUILD_DIR/curated_processor.py" "s3://$BUCKET/glue-scripts/curated_processor.py"
    AWS_PROFILE="${AWS_PROFILE:-terraform-firehose}" aws s3 cp "$BUILD_DIR/glue_libs.zip" "s3://$BUCKET/glue-scripts/glue_libs.zip"
    
    log_info "Upload complete!"
    log_info "  - s3://$BUCKET/glue-scripts/curated_processor.py"
    log_info "  - s3://$BUCKET/glue-scripts/glue_libs.zip"
fi

log_info "Done!"

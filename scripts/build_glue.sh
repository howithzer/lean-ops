#!/bin/bash
# ==============================================================================
# BUILD GLUE SCRIPT
# ==============================================================================
# What does this script do? (ELI5)
# ---------------------------------
# Think of it like packing a toolbox for a job site:
# 1. Copy the main Glue scripts (standardized_processor.py, curated_processor.py)
# 2. Zip up the utilities folder (utils/) as glue_libs.zip
# 3. Optionally upload everything to S3
#
# Why zip the utils folder?
# -------------------------
# AWS Glue needs extra Python files bundled as a ZIP. The --extra-py-files
# flag in Glue tells it "unzip this and add to the Python path."
#
# Usage:
#   ./scripts/build_glue.sh           # Just build locally
#   ./scripts/build_glue.sh --upload  # Build AND upload to S3
#
# After running:
#   The Glue jobs will use these files from S3 when executing
# ==============================================================================

# Exit on error, undefined variable, or pipe failure
set -euo pipefail

# Figure out where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
GLUE_DIR="$PROJECT_ROOT/scripts/glue"
BUILD_DIR="$PROJECT_ROOT/build/glue"

# Color codes for pretty output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Helper functions for logging
log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# ==============================================================================
# STEP 1: Create build directory
# ==============================================================================
log_info "Creating build directory: $BUILD_DIR"
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# ==============================================================================
# STEP 2: Copy main scripts
# ==============================================================================
log_info "Copying Glue scripts..."

# Copy the two main processor scripts
cp "$GLUE_DIR/standardized_processor.py" "$BUILD_DIR/"
cp "$GLUE_DIR/curated_processor.py" "$BUILD_DIR/"

log_info "  ✅ standardized_processor.py (RAW → Standardized)"
log_info "  ✅ curated_processor.py (Standardized → Curated)"

# ==============================================================================
# STEP 3: Create glue_libs.zip with utils package
# ==============================================================================
log_info "Creating glue_libs.zip with utils package..."

cd "$GLUE_DIR"

# Create the zip file
# -r = recursive
# -x = exclude patterns (no compiled Python files, no cache, no Mac files)
zip -r "$BUILD_DIR/glue_libs.zip" utils/ -x "*.pyc" -x "*__pycache__*" -x "*.DS_Store"

cd "$PROJECT_ROOT"

log_info "  ✅ glue_libs.zip (contains utils/flatten.py, utils/schema_evolution.py, etc.)"

# ==============================================================================
# STEP 4: Show what we built
# ==============================================================================
log_info "Build complete! Artifacts:"
ls -lh "$BUILD_DIR/"

# ==============================================================================
# STEP 5: Upload to S3 (optional)
# ==============================================================================
if [[ "${1:-}" == "--upload" ]]; then
    # Get the bucket name from the tfvars file
    BUCKET=$(grep 'iceberg_bucket' "$PROJECT_ROOT/environments/dev.tfvars" | sed 's/.*"\(.*\)".*/\1/' || echo "lean-ops-development-iceberg")
    
    log_info "Uploading to s3://$BUCKET/glue-scripts/..."
    
    # Upload each file
    AWS_PROFILE="${AWS_PROFILE:-terraform-firehose}" aws s3 cp "$BUILD_DIR/standardized_processor.py" "s3://$BUCKET/glue-scripts/standardized_processor.py"
    AWS_PROFILE="${AWS_PROFILE:-terraform-firehose}" aws s3 cp "$BUILD_DIR/curated_processor.py" "s3://$BUCKET/glue-scripts/curated_processor.py"
    AWS_PROFILE="${AWS_PROFILE:-terraform-firehose}" aws s3 cp "$BUILD_DIR/glue_libs.zip" "s3://$BUCKET/glue-scripts/glue_libs.zip"
    
    log_info "Upload complete!"
    log_info "  📤 s3://$BUCKET/glue-scripts/standardized_processor.py"
    log_info "  📤 s3://$BUCKET/glue-scripts/curated_processor.py"
    log_info "  📤 s3://$BUCKET/glue-scripts/glue_libs.zip"
fi

log_info "Done! 🎉"

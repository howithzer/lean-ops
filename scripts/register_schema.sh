#!/bin/bash
# =============================================================================
# REGISTER SCHEMA - Phase 2 Schema Management
# =============================================================================
# Registers a schema for a topic by:
# 1. Creating S3 folder structure (pending/active/failed/archive)
# 2. Uploading schema to pending/
# 3. Validating JSON syntax
# 4. Moving to active/ on success (or failed/ with error.json on failure)
# 5. Setting DynamoDB processing_enabled=true
#
# Usage:
#   ./scripts/register_schema.sh <topic> [schema_file]
#   ./scripts/register_schema.sh events                    # Uses schemas/events.json
#   ./scripts/register_schema.sh events ./custom_schema.json
# =============================================================================

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# AWS Configuration (match E2E test settings)
export AWS_PROFILE="${AWS_PROFILE:-terraform-firehose}"

# Configuration
BUCKET="${SCHEMA_BUCKET:-lean-ops-development-iceberg}"
TABLE_NAME="${SCHEMA_REGISTRY_TABLE:-lean-ops-dev-schema-registry}"

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step() { echo -e "${CYAN}[STEP]${NC} $1"; }

# =============================================================================
# PARSE ARGUMENTS
# =============================================================================
TOPIC="${1:-}"
SCHEMA_FILE="${2:-}"

if [ -z "$TOPIC" ]; then
    echo "Usage: $0 <topic> [schema_file]"
    echo ""
    echo "Examples:"
    echo "  $0 events                     # Uses schemas/events.json"
    echo "  $0 events ./custom_schema.json"
    exit 1
fi

# Default schema file
if [ -z "$SCHEMA_FILE" ]; then
    SCHEMA_FILE="$PROJECT_ROOT/schemas/${TOPIC}.json"
fi

if [ ! -f "$SCHEMA_FILE" ]; then
    log_error "Schema file not found: $SCHEMA_FILE"
    exit 1
fi

log_info "Registering schema for topic: $TOPIC"
log_info "Schema file: $SCHEMA_FILE"
log_info "S3 bucket: $BUCKET"

# =============================================================================
# STEP 1: Create S3 folder structure
# =============================================================================
log_step "1/5 Creating S3 folder structure..."

S3_PREFIX="schemas/${TOPIC}"

# Create placeholder files for each folder (S3 doesn't have real folders)
for folder in pending active failed archive; do
    aws s3api put-object \
        --bucket "$BUCKET" \
        --key "${S3_PREFIX}/${folder}/.keep" \
        --body /dev/null \
        2>/dev/null || true
done

log_info "Created folders: pending/, active/, failed/, archive/"

# =============================================================================
# STEP 2: Upload to pending/
# =============================================================================
log_step "2/5 Uploading schema to pending/..."

PENDING_KEY="${S3_PREFIX}/pending/schema.json"
aws s3 cp "$SCHEMA_FILE" "s3://${BUCKET}/${PENDING_KEY}"

log_info "Uploaded to s3://${BUCKET}/${PENDING_KEY}"

# =============================================================================
# STEP 3: Validate JSON syntax
# =============================================================================
log_step "3/5 Validating schema..."

VALIDATION_ERRORS=""

# Check JSON syntax
if ! jq empty "$SCHEMA_FILE" 2>/dev/null; then
    VALIDATION_ERRORS="Invalid JSON syntax"
fi

# Check required fields
if [ -z "$VALIDATION_ERRORS" ]; then
    HAS_TABLE_NAME=$(jq -r '.table_name // empty' "$SCHEMA_FILE")
    HAS_ENVELOPE=$(jq -r '.envelope_columns // empty' "$SCHEMA_FILE")
    HAS_PAYLOAD=$(jq -r '.payload_columns // empty' "$SCHEMA_FILE")
    
    if [ -z "$HAS_TABLE_NAME" ]; then
        VALIDATION_ERRORS="Missing required field: table_name"
    elif [ -z "$HAS_ENVELOPE" ]; then
        VALIDATION_ERRORS="Missing required field: envelope_columns"
    elif [ -z "$HAS_PAYLOAD" ]; then
        VALIDATION_ERRORS="Missing required field: payload_columns"
    fi
fi

# =============================================================================
# STEP 4: Move to active/ or failed/
# =============================================================================
log_step "4/5 Processing validation result..."

TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)

if [ -z "$VALIDATION_ERRORS" ]; then
    # SUCCESS: Move to active/
    ACTIVE_KEY="${S3_PREFIX}/active/schema.json"
    
    aws s3 mv "s3://${BUCKET}/${PENDING_KEY}" "s3://${BUCKET}/${ACTIVE_KEY}"
    
    log_info "Schema moved to active/"
    
    # Archive previous version (if exists)
    # S3 versioning handles this automatically
    
else
    # FAILURE: Move to failed/ with error.json
    FAILED_KEY="${S3_PREFIX}/failed/schema.json"
    ERROR_KEY="${S3_PREFIX}/failed/error.json"
    
    aws s3 mv "s3://${BUCKET}/${PENDING_KEY}" "s3://${BUCKET}/${FAILED_KEY}"
    
    # Write error.json
    ERROR_JSON=$(cat <<EOF
{
  "status": "FAILED",
  "topic": "${TOPIC}",
  "timestamp": "${TIMESTAMP}",
  "errors": [
    {
      "type": "VALIDATION_ERROR",
      "message": "${VALIDATION_ERRORS}"
    }
  ],
  "action_required": "Fix schema and re-upload to pending/"
}
EOF
)
    echo "$ERROR_JSON" | aws s3 cp - "s3://${BUCKET}/${ERROR_KEY}"
    
    log_error "Schema validation failed: $VALIDATION_ERRORS"
    log_error "Error details: s3://${BUCKET}/${ERROR_KEY}"
    
    # Update DynamoDB with error status
    aws dynamodb put-item \
        --table-name "$TABLE_NAME" \
        --item "{
            \"topic_name\": {\"S\": \"${TOPIC}\"},
            \"processing_enabled\": {\"BOOL\": false},
            \"status\": {\"S\": \"ERROR\"},
            \"last_error\": {\"S\": \"${VALIDATION_ERRORS}\"},
            \"updated_at\": {\"S\": \"${TIMESTAMP}\"}
        }"
    
    exit 1
fi

# =============================================================================
# STEP 5: Update DynamoDB flag
# =============================================================================
log_step "5/5 Updating DynamoDB processing flag..."

aws dynamodb put-item \
    --table-name "$TABLE_NAME" \
    --item "{
        \"topic_name\": {\"S\": \"${TOPIC}\"},
        \"processing_enabled\": {\"BOOL\": true},
        \"status\": {\"S\": \"READY\"},
        \"schema_path\": {\"S\": \"s3://${BUCKET}/${S3_PREFIX}/active/schema.json\"},
        \"updated_at\": {\"S\": \"${TIMESTAMP}\"}
    }"

log_info "DynamoDB: processing_enabled=true, status=READY"

# =============================================================================
# SUMMARY
# =============================================================================
echo ""
echo "=========================================="
echo -e "${GREEN}✅ SCHEMA REGISTERED SUCCESSFULLY${NC}"
echo "=========================================="
echo "Topic:      $TOPIC"
echo "Schema:     s3://${BUCKET}/${S3_PREFIX}/active/schema.json"
echo "DynamoDB:   processing_enabled=true"
echo ""
echo "Data processing is now ENABLED for this topic."
echo ""

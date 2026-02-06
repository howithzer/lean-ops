#!/bin/bash
# =============================================================================
# UNREGISTER SCHEMA - Phase 2 Schema Management
# =============================================================================
# Disables processing for a topic by:
# 1. Setting DynamoDB processing_enabled=false
# 2. Optionally removing schema from active/
#
# Usage:
#   ./scripts/unregister_schema.sh <topic> [--remove-schema]
#   ./scripts/unregister_schema.sh events           # Just disable flag
#   ./scripts/unregister_schema.sh events --remove-schema  # Also remove schema
# =============================================================================

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# AWS Configuration (match E2E test settings)
export AWS_PROFILE="${AWS_PROFILE:-terraform-firehose}"

# Configuration
BUCKET="${SCHEMA_BUCKET:-lean-ops-development-iceberg}"
TABLE_NAME="${SCHEMA_REGISTRY_TABLE:-lean-ops-dev-schema-registry}"

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }

# =============================================================================
# PARSE ARGUMENTS
# =============================================================================
TOPIC="${1:-}"
REMOVE_SCHEMA="${2:-}"

if [ -z "$TOPIC" ]; then
    echo "Usage: $0 <topic> [--remove-schema]"
    echo ""
    echo "Examples:"
    echo "  $0 events                     # Disable processing only"
    echo "  $0 events --remove-schema     # Also remove schema file"
    exit 1
fi

log_info "Unregistering schema for topic: $TOPIC"

# =============================================================================
# STEP 1: Update DynamoDB flag
# =============================================================================
log_info "Setting DynamoDB processing_enabled=false..."

TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)

aws dynamodb put-item \
    --table-name "$TABLE_NAME" \
    --item "{
        \"topic_name\": {\"S\": \"${TOPIC}\"},
        \"processing_enabled\": {\"BOOL\": false},
        \"status\": {\"S\": \"DISABLED\"},
        \"updated_at\": {\"S\": \"${TIMESTAMP}\"}
    }"

log_info "DynamoDB: processing_enabled=false, status=DISABLED"

# =============================================================================
# STEP 2: Optionally remove schema
# =============================================================================
if [ "$REMOVE_SCHEMA" = "--remove-schema" ]; then
    log_info "Removing schema files..."
    
    S3_PREFIX="schemas/${TOPIC}"
    
    # Remove active schema
    aws s3 rm "s3://${BUCKET}/${S3_PREFIX}/active/schema.json" 2>/dev/null || true
    
    # Remove pending schema (if any)
    aws s3 rm "s3://${BUCKET}/${S3_PREFIX}/pending/schema.json" 2>/dev/null || true
    
    log_info "Schema files removed"
fi

# =============================================================================
# SUMMARY
# =============================================================================
echo ""
echo "=========================================="
echo -e "${GREEN}✅ SCHEMA UNREGISTERED${NC}"
echo "=========================================="
echo "Topic:      $TOPIC"
echo "DynamoDB:   processing_enabled=false"
echo ""
echo "Data processing is now DISABLED for this topic."
echo ""

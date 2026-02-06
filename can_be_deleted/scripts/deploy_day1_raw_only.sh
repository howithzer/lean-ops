#!/bin/bash
#==============================================================================
# CLEAN DAY 1 DEPLOYMENT - Phase 1 RAW Layer Only
#==============================================================================
# This script emulates production Day 1 deployment:
# - Deploy infrastructure (Terraform)
# - NO schemas deployed
# - Step Function runs but skips processing (schema gate)
# - RAW ingestion works independently
#==============================================================================

set -e

echo "=================================="
echo "DAY 1 DEPLOYMENT - RAW LAYER ONLY"
echo "=================================="

# Configuration
ENVIRONMENT="dev"
AWS_PROFILE="terraform-firehose"
BUCKET="lean-ops-development-iceberg"

echo ""
echo "Step 1: Clean up existing schemas from previous runs"
echo "-----------------------------------------------------"
echo "This ensures we start fresh with NO schemas (Day 1 production scenario)"

aws s3 rm "s3://${BUCKET}/schemas/" --recursive --profile "$AWS_PROFILE" 2>/dev/null || echo "No existing schemas to remove"

echo "✅ Schemas removed (or none existed)"

echo ""
echo "Step 2: Clean up existing data (optional - for clean test)"
echo "-----------------------------------------------------------"
read -p "Do you want to delete existing RAW data? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Removing existing RAW data..."
    aws s3 rm "s3://${BUCKET}/iceberg_raw_db/" --recursive --profile "$AWS_PROFILE" 2>/dev/null || echo "No RAW data to remove"
    aws s3 rm "s3://${BUCKET}/iceberg_standardized_db/" --recursive --profile "$AWS_PROFILE" 2>/dev/null || echo "No Standardized data to remove"
    aws s3 rm "s3://${BUCKET}/iceberg_curated_db/" --recursive --profile "$AWS_PROFILE" 2>/dev/null || echo "No Curated data to remove"
    echo "✅ Existing data removed"
else
    echo "⏩ Skipping data cleanup"
fi

echo ""
echo "Step 3: Deploy infrastructure via Terraform"
echo "--------------------------------------------"
echo "This creates:"
echo "  - RAW database and tables (with partitions)"
echo "  - Standardized database (empty - no tables)"
echo "  - Curated database (empty - no tables)"
echo "  - Step Function (will run but skip due to no schemas)"
echo "  - Firehose, Lambda, SQS"
echo ""

AWS_PROFILE="$AWS_PROFILE" terraform apply -var-file="environments/${ENVIRONMENT}.tfvars" -auto-approve

echo ""
echo "✅ Infrastructure deployed!"

echo ""
echo "Step 4: Verify deployment"
echo "-------------------------"

# Check RAW tables exist
echo "Checking RAW tables..."
RAW_TABLES=$(aws glue get-tables \
    --database-name iceberg_raw_db \
    --profile "$AWS_PROFILE" \
    --query "TableList[].Name" \
    --output text)

if [ -z "$RAW_TABLES" ]; then
    echo "❌ ERROR: No RAW tables found!"
    exit 1
else
    echo "✅ RAW tables created: $RAW_TABLES"
fi

# Check schemas DO NOT exist
echo ""
echo "Checking schemas DO NOT exist..."
SCHEMA_COUNT=$(aws s3 ls "s3://${BUCKET}/schemas/" --profile "$AWS_PROFILE" 2>/dev/null | wc -l || echo "0")

if [ "$SCHEMA_COUNT" -eq 0 ]; then
    echo "✅ No schemas exist (correct for Day 1)"
else
    echo "⚠️  WARNING: Schemas found! Expected none for Day 1 test"
    aws s3 ls "s3://${BUCKET}/schemas/" --profile "$AWS_PROFILE"
fi

# Check Step Function exists
echo ""
echo "Checking Step Function..."
SFN_ARN=$(aws stepfunctions list-state-machines \
    --query "stateMachines[?contains(name, 'lean-ops-${ENVIRONMENT}')].stateMachineArn" \
    --output text \
    --profile "$AWS_PROFILE")

if [ -z "$SFN_ARN" ]; then
    echo "❌ ERROR: Step Function not found!"
    exit 1
else
    echo "✅ Step Function exists: $(basename $SFN_ARN)"
fi

echo ""
echo "======================================"
echo "DAY 1 DEPLOYMENT COMPLETE!"
echo "======================================"
echo ""
echo "What happens next:"
echo "  1. ✅ RAW ingestion works (SQS → Lambda → Firehose → RAW tables)"
echo "  2. ✅ Step Function runs every 15 minutes"
echo "  3. ✅ Schema gate BLOCKS processing (no schemas exist)"
echo "  4. ✅ RAW data accumulates safely"
echo ""
echo "To test RAW ingestion:"
echo "  ./tests/e2e/production_emulation.sh day1"
echo ""
echo "To deploy schemas and enable Standardized processing:"
echo "  ./tests/e2e/production_emulation.sh schema"
echo ""
echo "To run Step Function manually and verify schema gate:"
echo "  aws stepfunctions start-execution \\"
echo "    --state-machine-arn \"$SFN_ARN\" \\"
echo "    --input '{\"topic_name\":\"events\"}' \\"
echo "    --profile \"$AWS_PROFILE\""
echo ""

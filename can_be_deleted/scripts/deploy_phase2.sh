#!/bin/bash
#==============================================================================
# DEPLOY PHASE 2: Standardized Layer Deduplication
#==============================================================================
# This script:
# 1. Deploys Terraform changes (Schema partitioning)
# 2. Uploads updated Glue scripts (Two-stage dedup logic)
# 3. Clears data for a fresh Day 1 test
#==============================================================================

set -e

# Configuration
ENVIRONMENT="dev"
AWS_PROFILE="terraform-firehose"
BUCKET="lean-ops-development-iceberg"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=================================="
echo "DEPLOY PHASE 2: Standardized Dedup"
echo "=================================="

echo ""
echo "Step 1: Terraform Apply (Infrastructure)"
echo "----------------------------------------"
echo "Applying changes to: modules/catalog/main.tf (Partitioning)"

cd "$ROOT_DIR"
AWS_PROFILE="$AWS_PROFILE" terraform apply -var-file="environments/${ENVIRONMENT}.tfvars" -auto-approve

echo "✅ Terraform apply complete!"

echo ""
echo "Step 2: Upload Glue Scripts"
echo "---------------------------"
echo "Uploading Python scripts to s3://${BUCKET}/glue-scripts/"

aws s3 cp "$ROOT_DIR/scripts/glue/standardized_processor.py" "s3://${BUCKET}/glue-scripts/" --profile "$AWS_PROFILE"
aws s3 cp "$ROOT_DIR/scripts/glue/curated_processor.py" "s3://${BUCKET}/glue-scripts/" --profile "$AWS_PROFILE"
aws s3 cp "$ROOT_DIR/scripts/glue/utils/schema_evolution.py" "s3://${BUCKET}/glue-scripts/utils/" --profile "$AWS_PROFILE"

# Zip and upload utils
cd "$ROOT_DIR/scripts/glue"
zip -r glue_libs.zip utils/
aws s3 cp glue_libs.zip "s3://${BUCKET}/glue-scripts/" --profile "$AWS_PROFILE"
rm glue_libs.zip
cd "$ROOT_DIR"

echo "✅ Glue scripts uploaded!"

echo ""
echo "Step 3: Cleanup for Fresh Test (Optional)"
echo "----------------------------------------"
read -p "Do you want to clear existing data for a fresh start? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Clearing S3 data..."
    aws s3 rm "s3://${BUCKET}/iceberg_raw_db/" --recursive --profile "$AWS_PROFILE" >/dev/null
    aws s3 rm "s3://${BUCKET}/iceberg_standardized_db/" --recursive --profile "$AWS_PROFILE" >/dev/null
    aws s3 rm "s3://${BUCKET}/iceberg_curated_db/" --recursive --profile "$AWS_PROFILE" >/dev/null
    aws s3 rm "s3://${BUCKET}/schemas/" --recursive --profile "$AWS_PROFILE" >/dev/null
    
    echo "Clearing DynamoDB checkpoints..."
    aws dynamodb scan --table-name "lean-ops-dev-checkpoints" --profile "$AWS_PROFILE" \
        --query "Items[*].[pipeline_id, checkpoint_type]" --output text | \
    while read pipeline_id checkpoint_type; do
        aws dynamodb delete-item --table-name "lean-ops-dev-checkpoints" --profile "$AWS_PROFILE" \
            --key "{\"pipeline_id\": {\"S\": \"$pipeline_id\"}, \"checkpoint_type\": {\"S\": \"$checkpoint_type\"}}"
    done
    
    echo "✅ Data cleared!"
else
    echo "⏩ Skipping data cleanup"
fi

echo ""
echo "======================================"
echo "DEPLOYMENT COMPLETE!"
echo "======================================"
echo "Ready to run tests:"
echo "  ./tests/e2e/production_emulation.sh full"
echo ""

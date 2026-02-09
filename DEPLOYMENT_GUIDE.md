# Schema Management Deployment Guide

**Status**: ✅ All isolated tests passed
**Date**: February 8, 2026

---

## Test Results Summary

```
✅ Type Compatibility Matrix: PASSED (10/10 type conversions)
✅ Add Optional Column: PASSED
✅ Remove Column Detection: PASSED
✅ Type Narrowing Detection: PASSED
✅ Schema v1→v2 Enhancement: PASSED
✅ Schema v2→v3 Breaking Change: PASSED
✅ Structure Validation: PASSED
```

All core validation logic is working correctly!

---

## Files Saved

### Core Code
- ✅ `/modules/compute/lambda/schema_validator/compatibility.py` - Backward compatibility validator
- ✅ `/modules/compute/lambda/schema_validator/handler_enhanced.py` - Enhanced schema validator Lambda
- ✅ `/modules/compute/lambda/schema_validator/handler.py` - Original handler (preserved)

### Test Files
- ✅ `/tests/unit/test_compatibility.py` - Unit tests (pytest)
- ✅ `/tests/unit/test_schema_validation.py` - Validation tests (pytest)
- ✅ `/tests/run_simple_tests.sh` - Simple test runner (no dependencies)
- ✅ `/tests/schemas/events_v1.json` - Base schema
- ✅ `/tests/schemas/events_v2_enhancement.json` - Enhancement scenario
- ✅ `/tests/schemas/events_v3_breaking.json` - Breaking change scenario

### Documentation
- ✅ `/docs/SCHEMA_MANAGEMENT_IMPLEMENTATION_PLAN.md` - Complete implementation guide

---

## Deployment Steps

### Step 1: Build Lambda Package

```bash
cd /Users/sagarm/Documents/Projects/Huntington_Exercises/lean-ops

# Create deployment package
cd modules/compute/lambda/schema_validator
zip -r schema_validator_enhanced.zip handler_enhanced.py compatibility.py

# Verify package
unzip -l schema_validator_enhanced.zip
```

**Expected output:**
```
  Length      Date    Time    Name
---------  ---------- -----   ----
    14143  02-08-2026 18:21   compatibility.py
    22921  02-08-2026 18:23   handler_enhanced.py
```

### Step 2: Upload to S3

```bash
export ICEBERG_BUCKET="your-iceberg-bucket-name"  # e.g., lean-ops-development-iceberg

# Upload Lambda package
aws s3 cp schema_validator_enhanced.zip \
  s3://$ICEBERG_BUCKET/lambda-packages/schema_validator_enhanced.zip

# Verify upload
aws s3 ls s3://$ICEBERG_BUCKET/lambda-packages/
```

### Step 3: Update Lambda Function

**Option A: Via AWS CLI**

```bash
# Update function code
aws lambda update-function-code \
  --function-name lean-ops-dev-schema-validator \
  --s3-bucket $ICEBERG_BUCKET \
  --s3-key lambda-packages/schema_validator_enhanced.zip

# Update handler to use enhanced version
aws lambda update-function-configuration \
  --function-name lean-ops-dev-schema-validator \
  --handler handler_enhanced.lambda_handler

# Add environment variable for backfill (if not exists)
aws lambda update-function-configuration \
  --function-name lean-ops-dev-schema-validator \
  --environment "Variables={
    SCHEMA_BUCKET=$ICEBERG_BUCKET,
    SCHEMA_REGISTRY_TABLE=lean-ops-dev-schema-registry,
    ATHENA_OUTPUT_BUCKET=$ICEBERG_BUCKET,
    ICEBERG_BUCKET=$ICEBERG_BUCKET,
    BACKFILL_STATE_MACHINE_ARN=arn:aws:states:region:account:stateMachine:backfill
  }"
```

**Option B: Via Terraform**

Update `modules/compute/main.tf`:

```hcl
resource "aws_lambda_function" "schema_validator" {
  function_name = "${var.project_name}-${var.environment}-schema-validator"

  # Update to use new package
  s3_bucket = var.iceberg_bucket
  s3_key    = "lambda-packages/schema_validator_enhanced.zip"

  # Update handler
  handler = "handler_enhanced.lambda_handler"

  environment {
    variables = {
      SCHEMA_BUCKET              = var.schema_bucket
      SCHEMA_REGISTRY_TABLE      = "${var.project_name}-${var.environment}-schema-registry"
      ATHENA_OUTPUT_BUCKET       = var.iceberg_bucket
      ICEBERG_BUCKET             = var.iceberg_bucket
      BACKFILL_STATE_MACHINE_ARN = aws_sfn_state_machine.backfill.arn  # Add when backfill is implemented
    }
  }
}
```

Then deploy:

```bash
terraform plan
terraform apply
```

### Step 4: Test Schema Upload (Isolated Test)

```bash
# Test 1: Upload valid schema to pending/
aws s3 cp tests/schemas/events_v1.json \
  s3://$ICEBERG_BUCKET/schemas/test_events/pending/schema.json

# Wait 30 seconds for EventBridge + Lambda execution
sleep 30

# Verify: Schema moved to active/
aws s3 ls s3://$ICEBERG_BUCKET/schemas/test_events/active/
# Expected: schema.json

# Verify: DynamoDB updated
aws dynamodb get-item \
  --table-name lean-ops-dev-schema-registry \
  --key '{"topic_name":{"S":"test_events"}}' \
  | jq '.Item.processing_enabled.BOOL, .Item.status.S'
# Expected: true, "READY"

# Check Lambda logs
aws logs tail /aws/lambda/lean-ops-dev-schema-validator --since 5m --follow
```

**Expected log output:**
```
Processing schema for topic: test_events
✅ Schema validated and activated
Updated DynamoDB: processing_enabled=true, status=READY
```

### Step 5: Test Breaking Change Detection

```bash
# Upload breaking change schema
aws s3 cp tests/schemas/events_v3_breaking.json \
  s3://$ICEBERG_BUCKET/schemas/test_events/pending/schema.json

# Wait 30 seconds
sleep 30

# Verify: Schema moved to failed/
aws s3 ls s3://$ICEBERG_BUCKET/schemas/test_events/failed/
# Expected: schema.json, error.json

# Check error.json
aws s3 cp s3://$ICEBERG_BUCKET/schemas/test_events/failed/error.json - | jq .
```

**Expected error.json:**
```json
{
  "status": "FAILED",
  "topic": "test_events",
  "timestamp": "2026-02-08T...",
  "errors": [
    {
      "type": "BREAKING_CHANGE",
      "field": "user_id",
      "message": "Unsafe narrowing: BIGINT → INT (data loss or parse error risk)"
    },
    {
      "type": "BREAKING_CHANGE",
      "field": "device_type",
      "message": "Column 'device_type' removed (data loss)"
    }
  ],
  "compatibility_report": "...detailed report...",
  "action_required": "Fix breaking changes and re-upload to pending/"
}
```

### Step 6: Test Schema Enhancement

```bash
# First, register v1
aws s3 cp tests/schemas/events_v1.json \
  s3://$ICEBERG_BUCKET/schemas/test_events2/pending/schema.json

# Wait for processing
sleep 30

# Then, upload v2 (enhancement)
aws s3 cp tests/schemas/events_v2_enhancement.json \
  s3://$ICEBERG_BUCKET/schemas/test_events2/pending/schema.json

# Wait for processing
sleep 30

# Verify: v1 archived
aws s3 ls s3://$ICEBERG_BUCKET/schemas/test_events2/archive/
# Expected: v1.0.0_YYYYMMDD_HHMMSS.json

# Verify: v2 in active/
aws s3 cp s3://$ICEBERG_BUCKET/schemas/test_events2/active/schema.json - | jq .schema_version
# Expected: "2.0.0"

# Check DynamoDB status
aws dynamodb get-item \
  --table-name lean-ops-dev-schema-registry \
  --key '{"topic_name":{"S":"test_events2"}}' \
  | jq '.Item.status.S'
# Expected: "BACKFILL_PENDING" (until backfill is implemented)
# Or "READY" if BACKFILL_STATE_MACHINE_ARN not set
```

### Step 7: Cleanup Test Data

```bash
# Remove test topics
aws s3 rm s3://$ICEBERG_BUCKET/schemas/test_events --recursive
aws s3 rm s3://$ICEBERG_BUCKET/schemas/test_events2 --recursive

# Remove DynamoDB entries
aws dynamodb delete-item \
  --table-name lean-ops-dev-schema-registry \
  --key '{"topic_name":{"S":"test_events"}}'

aws dynamodb delete-item \
  --table-name lean-ops-dev-schema-registry \
  --key '{"topic_name":{"S":"test_events2"}}'
```

---

## Rollback Plan

If issues occur, rollback to original handler:

```bash
# Option 1: Via CLI
aws lambda update-function-configuration \
  --function-name lean-ops-dev-schema-validator \
  --handler handler.lambda_handler

# Option 2: Via Terraform
# Revert to original handler in main.tf
# handler = "handler.lambda_handler"
terraform apply
```

The original `handler.py` is preserved and will continue working.

---

## Monitoring

### CloudWatch Logs

```bash
# Watch Lambda logs in real-time
aws logs tail /aws/lambda/lean-ops-dev-schema-validator --follow

# Search for errors
aws logs filter-pattern /aws/lambda/lean-ops-dev-schema-validator \
  --filter-pattern "ERROR" \
  --start-time $(date -u -d '1 hour ago' +%s)000
```

### CloudWatch Metrics

The enhanced Lambda publishes these metrics:
- `LeanOps/SchemaManagement/SchemaValidationSuccessRate`
- `LeanOps/SchemaManagement/BreakingChangesDetected`

**To query:**
```bash
aws cloudwatch get-metric-statistics \
  --namespace LeanOps/SchemaManagement \
  --metric-name BreakingChangesDetected \
  --start-time $(date -u -d '1 hour ago' --iso-8601) \
  --end-time $(date -u --iso-8601) \
  --period 3600 \
  --statistics Sum
```

---

## Next Phase: Drift Detection & Catch-Up

After successful deployment and testing of schema validation:

1. **Add drift detection** to `standardized_processor.py` (see Implementation Plan)
2. **Create drift_log table** in Glue catalog
3. **Implement catch-up orchestrator** Lambda
4. **Create curated_catchup Glue job**
5. **Run E2E drift scenario tests**

Refer to `/docs/SCHEMA_MANAGEMENT_IMPLEMENTATION_PLAN.md` for detailed steps.

---

## Troubleshooting

### Issue: Lambda timeout

**Solution**: Increase timeout in Lambda configuration:
```bash
aws lambda update-function-configuration \
  --function-name lean-ops-dev-schema-validator \
  --timeout 300  # 5 minutes
```

### Issue: Athena DDL fails

**Symptoms**: Tables not created, error in logs: "FAILED: SemanticException..."

**Solution**: Check Glue catalog permissions:
```bash
# Verify Lambda role has glue:CreateTable permission
aws iam get-role-policy \
  --role-name lean-ops-dev-schema-validator-role \
  --policy-name lambda-glue-policy
```

### Issue: Schema stays in pending/

**Symptoms**: Schema not moving to active/ or failed/

**Solution**: Check EventBridge rule is enabled:
```bash
aws events describe-rule \
  --name lean-ops-dev-schema-upload-trigger

# Enable if disabled
aws events enable-rule \
  --name lean-ops-dev-schema-upload-trigger
```

### Issue: Breaking changes not detected

**Symptoms**: Breaking change schema moves to active/ instead of failed/

**Solution**: Verify active schema exists before uploading new schema:
```bash
aws s3 ls s3://$ICEBERG_BUCKET/schemas/your_topic/active/
# If empty, upload v1 first, then try v2
```

---

## Success Criteria

✅ **Deployment successful if:**
1. Valid schema uploaded to pending/ → moves to active/
2. DynamoDB `processing_enabled=true`, `status=READY`
3. Breaking change schema → moves to failed/ with error.json
4. Enhancement schema → archives old version, moves new to active/
5. CloudWatch logs show no errors
6. Lambda metrics show successful validations

---

## Support

For issues or questions:
1. Check CloudWatch logs first
2. Review `/docs/SCHEMA_MANAGEMENT_IMPLEMENTATION_PLAN.md`
3. Run local tests: `./tests/run_simple_tests.sh`
4. Check architectural review: `/docs/LEAN_OPS_ARCHITECTURE_REVIEW.md` (in lean-claude directory)

---

**Ready for deployment!** 🚀

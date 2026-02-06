# Lean-Ops: Lessons Learned & Next Wave

## Date: 2026-01-10

---

## Lessons Learned

### Critical Findings

| Issue | Impact | Fix |
|-------|--------|-----|
| Missing `ReportBatchItemFailures` | Failed messages deleted, not retried | Added to ESM |
| No `scaling_config` | Uncontrolled Lambda concurrency | Added max=50 |
| 300s visibility timeout | Slow DLQ delivery (15 min) | Reduce Lambda timeout |
| No DLQ processor | Messages expire, data loss | Wave 2 item |

### Testing Observations

1. **Happy path works at scale**: 5,000 records at 1,171/sec with 0 failures
2. **Multi-table routing works**: Firehose transform correctly routes by topic_name
3. **Error handling now works**: After Wave 1 fix, messages properly retry → DLQ

---

## Wave 2 Implementation Plan

### Priority 1: Error Classification

Update `sqs_processor/handler.py`:
```python
def classify_error(error):
    msg = str(error).lower()
    if 'json' in msg or 'decode' in msg:
        return 'DROP'  # Malformed - don't retry
    if 'throttl' in msg or 'provision' in msg:
        return 'RETRY'  # Transient
    return 'UNKNOWN'  # Let SQS retry → DLQ
```

### Priority 2: DLQ Processor Lambda

```
DLQ → Lambda → s3://bucket/dlq-archive/{topic}/{date}/{message_id}.json
             → DynamoDB error_tracker
```

### Priority 3: Reduce Visibility Timeout

```hcl
# modules/compute/main.tf
timeout = 30  # was 60

# OR explicitly set visibility in messaging module
visibility_timeout_seconds = 90
```

### Priority 4: Circuit Breaker

```
CloudWatch Alarm (ErrorRate > 50%)
    → SNS
    → Lambda: disable ESM
```

---

## Wave 3 Items

- DLQ age alarm (7 days)
- BisectBatchOnFunctionError
- Water balance monitoring (RAW = Curated = Semantic)
- Exponential backoff with jitter

---

## Production Readiness Checklist

- [ ] Reduce Lambda timeout to 30s
- [ ] Add DLQ Processor Lambda
- [ ] Implement error classification
- [ ] Add circuit breaker
- [ ] Harden IAM policies (least privilege)
- [ ] Add VPC deployment
- [ ] Enable KMS encryption

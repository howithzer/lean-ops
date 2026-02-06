# Lean-Ops: Next Release Plan (v1.1.0)

## Timeline: Wave 2 - Production Hardening

---

## Priority 1: DLQ Processing (1-2 days)

### DLQ Processor Lambda
```
DLQ → Lambda → s3://bucket/dlq-archive/{topic}/{YYYY-MM-DD}/{message_id}.json
             → DynamoDB error_tracker
```

**Files to create:**
- `modules/compute/lambda/dlq_processor/handler.py`
- Update `modules/messaging/main.tf` (DLQ event source)

---

## Priority 2: Error Classification (1 day)

Update `sqs_processor/handler.py`:

```python
def classify_error(error):
    msg = str(error).lower()
    if 'json' in msg or 'decode' in msg:
        return 'DROP'    # Log and return success
    if 'throttl' in msg:
        return 'RETRY'   # Return in batchItemFailures
    return 'UNKNOWN'     # Let SQS retry → DLQ
```

---

## Priority 3: Performance Tuning (0.5 days)

- Reduce Lambda timeout: 60s → 30s
- Visibility timeout: 300s → 90s
- Add `bisect_batch_on_function_error = true`

---

## Priority 4: Circuit Breaker (1 day)

```
CloudWatch Alarm (ErrorRate > 50%, 5 min)
    → SNS Topic
    → Lambda: disable Event Source Mapping
    
Recovery:
    → Lambda: re-enable ESM with BatchSize=1
    → Gradual ramp-up
```

---

## Priority 5: Observability (0.5 days)

- DLQ age alarm (7 days threshold)
- Water balance metrics (RAW ≈ Curated ≈ Semantic)
- Error rate dashboard

---

## Acceptance Criteria

- [ ] Malformed messages → S3 archive (not lost)
- [ ] Error classification logs to DynamoDB
- [ ] Circuit breaker tested with simulated outage
- [ ] DLQ age alarm fires at 7 days
- [ ] All IAM policies least-privilege

---

## Estimated Effort: 4-5 days

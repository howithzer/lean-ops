# Implementation Plan: Unified Dual-Output Job

## The Problem with Parallel Jobs

| Issue | Impact |
|-------|--------|
| **Checkpoint sync** | Both checkpoints are the same — why track separately? |
| **Partial failure** | Curation succeeds, Semantic fails → data inconsistency |
| **Duplicate work** | Both jobs read same data, dedup same records |
| **Drift detection** | Both need to detect drift — wasteful |

## Solution: Single Unified Job, Dual Output

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         UNIFIED GLUE JOB                                    │
│                                                                             │
│   ┌──────────────┐                                                          │
│   │ Read RAW     │ ─────────────────────────────────────────────────────┐   │
│   │ (once)       │                                                       │   │
│   └──────┬───────┘                                                       │   │
│          │                                                               │   │
│          ▼                                                               │   │
│   ┌──────────────┐                                                       │   │
│   │ Dedup        │  FIFO (message_id) + LIFO (idempotency_key)           │   │
│   │ (once)       │                                                       │   │
│   └──────┬───────┘                                                       │   │
│          │                                                               │   │
│          ▼                                                               │   │
│   ┌──────────────┐                                                       │   │
│   │ Detect Drift │  Compare actual fields vs schema                      │   │
│   │ (once)       │  → new_fields, missing_fields                         │   │
│   └──────┬───────┘                                                       │   │
│          │                                                               │   │
│          ├──────────────────────────────────┐                            │   │
│          │                                   │                            │   │
│          ▼                                   ▼                            │   │
│   ┌──────────────────────┐         ┌──────────────────────┐              │   │
│   │ CURATED OUTPUT       │         │ SEMANTIC OUTPUT      │              │   │
│   │ ──────────────────── │         │ ──────────────────── │              │   │
│   │ • Flatten all fields │         │ • Schema fields only │              │   │
│   │ • ALL STRING         │         │ • TYPED columns      │              │   │
│   │ • Auto-add columns   │         │ • CDE validation     │              │   │
│   │ • No validation      │         │ • PII masking        │              │   │
│   └──────────┬───────────┘         └──────────┬───────────┘              │   │
│              │                                │                           │   │
│              ▼                                ▼                           │   │
│   ┌──────────────────────┐         ┌──────────────────────┐              │   │
│   │ curated_db.{topic}   │         │ semantic_db.{topic}  │              │   │
│   └──────────────────────┘         └──────────────────────┘              │   │
│                                                                          │   │
│   ┌──────────────────────────────────────────────────────────────────┐   │   │
│   │ SINGLE CHECKPOINT (one atomic update)                            │   │   │
│   └──────────────────────────────────────────────────────────────────┘   │   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Failure Handling

### Transactional Write Strategy

```python
def main():
    # Read and process once
    raw_df = read_incremental()
    deduped = dedup(raw_df)
    drift = detect_drift(deduped)
    
    # Prepare both outputs
    curated_df = prepare_curated(deduped, drift)   # Flatten, STRING
    semantic_df = prepare_semantic(deduped)        # Schema, TYPED
    
    try:
        # Write to CURATED (auto-evolve first if needed)
        if drift.new_fields:
            apply_ddl(drift.new_fields)
        curated_df.writeTo("curated_db.{topic}").append()
        
        # Write to SEMANTIC
        semantic_df.writeTo("semantic_db.{topic}").append()
        
        # SINGLE checkpoint update (only if BOTH succeed)
        update_checkpoint(current_snapshot)
        
    except Exception as e:
        # If either fails, checkpoint NOT updated
        # Next run will reprocess the same data
        raise e
```

### What If Curated Succeeds but Semantic Fails?

| Scenario | Old (Parallel) | New (Unified) |
|----------|----------------|---------------|
| Curated ✅, Semantic ❌ | Curated checkpoint updated, data inconsistent | Checkpoint NOT updated, both retry |
| Both retry next run | Only Semantic retries | Both retry (idempotent via dedup) |

**Key insight**: Since we dedup on `idempotency_key`, reprocessing is idempotent. No harm in writing to Curated twice.

---

## Tiered Checkpoints (Middle Ground)

> **Concern**: If Semantic fails due to type mismatch (e.g., STRING→INT), data scientists shouldn't wait for analysts' schema fix.

### Solution: Independent Checkpoints

```python
def main():
    # Get BOTH checkpoints
    curated_chkpt = get_checkpoint("curated")   # e.g., 103
    semantic_chkpt = get_checkpoint("semantic")  # e.g., 100
    
    # Determine what to read (minimum = earliest checkpoint)
    read_from = min(curated_chkpt, semantic_chkpt)  # Read from 100
    
    raw_df = read_from_snapshot(snapshot_after=read_from)
    deduped = dedup(raw_df)
    
    # CURATED: Only write if new data for curated
    if current_snapshot > curated_chkpt:
        curated_df = flatten_to_strings(deduped)
        curated_df.writeTo("curated_db.{topic}").append()
        update_curated_checkpoint(current_snapshot)
    
    # === CURATED OUTPUT (always happens first) ===
    try:
        curated_df = flatten_to_strings(deduped)
        curated_df.writeTo("curated_db.{topic}").append()
        update_curated_checkpoint(current_snapshot)  # ✅ Curated checkpoint
        curated_success = True
    except Exception as e:
        log_error("CURATED_FAILED", e)
        raise  # Critical failure - stop job
    
    # === SEMANTIC OUTPUT (independent try/catch) ===
    try:
        semantic_df = parse_with_schema(deduped, schema)
        semantic_df.writeTo("semantic_db.{topic}").append()
        update_semantic_checkpoint(current_snapshot)  # ✅ Semantic checkpoint
        semantic_success = True
    except Exception as e:
        log_error("SEMANTIC_FAILED", e)
        alert("Semantic failed, Curated succeeded")
        semantic_success = False
    
    return "FULL_SUCCESS" if semantic_success else "CURATED_ONLY"
```

### Checkpoint Schema

| Checkpoint | Updated When | Purpose |
|------------|--------------|---------|
| `curated_checkpoint` | Curated write succeeds | Track Curated layer progress |
| `semantic_checkpoint` | Semantic write succeeds | Track Semantic layer progress |

### Failure Matrix

| Scenario | Curated Chkpt | Semantic Chkpt | Data Scientists | Analysts | Next Run |
|----------|---------------|----------------|-----------------|----------|----------|
| Both ✅ | Updated | Updated | ✅ Fresh data | ✅ Fresh data | Both advance |
| Curated ✅, Semantic ❌ | Updated | **NOT** | ✅ Fresh data | ⏳ Stale | Semantic retries |
| Both ❌ | NOT | NOT | ⏳ Stale | ⏳ Stale | Both retry |

### Step Functions Outcomes

```
                    ┌──────────────────────┐
                    │   RunUnifiedJob      │
                    └──────────┬───────────┘
                               │
           ┌───────────────────┼───────────────────┐
           ▼                   ▼                   ▼
    ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
    │ FULL_SUCCESS│     │CURATED_ONLY │     │CURATED_FAILED│
    │  (succeed)  │     │(succeed+alert)│     │  (fail)     │
    └─────────────┘     └─────────────┘     └─────────────┘
```

**Result**: Data scientists always get their data when Curated succeeds!

---

## Simplified Step Functions

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         STEP FUNCTIONS                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌──────────────────┐                                                      │
│   │ GetCheckpoint    │  (single checkpoint for both layers)                 │
│   └────────┬─────────┘                                                      │
│            ▼                                                                │
│   ┌──────────────────┐                                                      │
│   │ GetRawSnapshot   │                                                      │
│   └────────┬─────────┘                                                      │
│            ▼                                                                │
│   ┌──────────────────┐                                                      │
│   │ CheckNewData     │ ─────────▶ NoNewData (exit)                          │
│   └────────┬─────────┘                                                      │
│            ▼                                                                │
│   ┌──────────────────┐                                                      │
│   │ CheckSchemaExists│ ─────────▶ NoSchema (exit)                           │
│   └────────┬─────────┘                                                      │
│            ▼                                                                │
│   ┌──────────────────┐                                                      │
│   │ RunUnifiedJob    │  ← Single job writes to both tables                  │
│   └────────┬─────────┘                                                      │
│            ▼                                                                │
│   ┌──────────────────┐                                                      │
│   │ UpdateCheckpoint │  ← Single checkpoint                                 │
│   └────────┬─────────┘                                                      │
│            ▼                                                                │
│        FullSuccess                                                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Code Structure: Unified Job

```python
# unified_job.py

def main():
    # 1. Read incremental from RAW
    raw_df = read_from_snapshot(last_checkpoint)
    
    # 2. Dedup (shared)
    deduped = apply_network_dedup(raw_df)
    deduped = apply_app_correction_dedup(deduped)
    
    # 3. Detect drift (shared)
    drift_result = detect_drift(deduped, schema)
    
    # 4. CURATED output: Flatten all fields, STRING types
    curated_df = flatten_to_strings(deduped)
    
    if drift_result.new_fields:
        apply_curated_ddl(drift_result.new_fields)
    
    curated_df.writeTo(curated_table).append()
    
    # 5. SEMANTIC output: Schema fields, typed, validated
    semantic_df = parse_with_schema(deduped, schema)
    semantic_df = apply_pii_masking(semantic_df, schema)
    valid_df, errors_df = validate_cdes(semantic_df, schema)
    
    write_errors(errors_df)
    valid_df.writeTo(semantic_table).append()
    
    # 6. Log drift for visibility
    if drift_result.has_drift:
        log_drift(drift_result)
    
    # 7. Single checkpoint update
    update_checkpoint(current_snapshot)
```

---

## Benefits Summary

| Aspect | Parallel Jobs | Unified Job |
|--------|---------------|-------------|
| **Read RAW** | 2x | 1x ✅ |
| **Dedup** | 2x | 1x ✅ |
| **Drift detection** | 2x | 1x ✅ |
| **Checkpoints** | 2 | 1 ✅ |
| **Failure handling** | Complex | Simple ✅ |
| **Consistency** | Risk of drift | Atomic ✅ |

---

## Answers to Your Questions

1. **Same checkpoint?** → Yes, single checkpoint for the unified job
2. **Curation ✅, Semantic ❌?** → Both retry (checkpoint not updated until both succeed)
3. **Read once, write twice?** → Yes, exactly what unified job does
4. **Schema drift detection once?** → Yes, shared logic before branching to outputs

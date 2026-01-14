# Iceberg Optimizations for Lean-Ops

Based on the LakeOps article, here's an analysis of applicable optimizations for the lean-ops data pipeline.

---

## ✅ Already Implemented

| Optimization | Status | Location |
|-------------|--------|----------|
| MERGE deduplication | ✅ | `curated_processor.py` - LIFO on idempotency_key |
| Incremental processing | ✅ | Checkpoint-based reads from RAW |
| IcebergSparkSessionExtensions | ✅ | Glue job `--conf` |

---

## 🎯 High Priority: Add Now

### 1. Snapshot Expiration (Critical)

**Problem:** Each Firehose commit creates a snapshot. With high-volume writes, snapshot chains grow fast.

**Add to:** `modules/orchestration/main.tf` as a scheduled Lambda or Step Function task

```python
# Add to curated_processor.py AFTER main processing
def expire_old_snapshots():
    """Expire snapshots older than 7 days, keep last 50."""
    expire_sql = f"""
    CALL glue_catalog.system.expire_snapshots(
      table => '{CURATED_TABLE}',
      older_than => TIMESTAMP '{(datetime.utcnow() - timedelta(days=7)).isoformat()}',
      retain_last => 50
    )
    """
    spark.sql(expire_sql)
```

### 2. Orphan File Cleanup (Storage Cost)

**Problem:** Failed writes, aborted jobs leave orphan files. Storage costs creep up.

**Add to:** Weekly maintenance Lambda or Glue job

```python
def remove_orphan_files():
    """Remove orphan files older than 48 hours."""
    cleanup_sql = f"""
    CALL glue_catalog.system.remove_orphan_files(
      table => '{CURATED_TABLE}',
      older_than => TIMESTAMP '{(datetime.utcnow() - timedelta(hours=48)).isoformat()}'
    )
    """
    spark.sql(cleanup_sql)
```

### 3. Manifest Rewrite (Planning Performance)

**Problem:** Many small commits = many manifests = slow query planning.

**Add to:** After snapshot expiration

```python
def rewrite_manifests():
    """Consolidate manifests to reduce planning overhead."""
    spark.sql(f"CALL glue_catalog.system.rewrite_manifests(table => '{CURATED_TABLE}')")
```

---

## 📊 Medium Priority: Add for Production

### 4. Hot Window Compaction

**Problem:** Only compact recent data (hot window), not cold history.

**Current:** We don't have explicit compaction.

**Recommendation:** Add targeted compaction for last 14 days only:

```python
def compact_hot_window():
    """Compact only recent data where fragmentation matters."""
    compact_sql = f"""
    CALL glue_catalog.system.rewrite_data_files(
      table => '{CURATED_TABLE}',
      where => 'ingestion_ts >= {int((datetime.utcnow() - timedelta(days=14)).timestamp())}',
      options => map(
        'min-input-files', '5',
        'target-file-size-bytes', '268435456',
        'max-file-group-size-bytes', '8589934592',
        'max-concurrent-file-group-rewrites', '3'
      )
    )
    """
    spark.sql(compact_sql)
```

### 5. Table Health Monitoring Lambda

**Add:** A Lambda that queries Iceberg metadata and stores health metrics:

```python
def get_table_health():
    """Collect table health metrics."""
    return {
        'snapshot_count': spark.sql(f"SELECT COUNT(*) FROM {TABLE}.snapshots").first()[0],
        'manifest_count': spark.sql(f"SELECT COUNT(*) FROM {TABLE}.manifests").first()[0],
        'data_file_count': spark.sql(f"SELECT COUNT(*) FROM {TABLE}.files WHERE content = 0").first()[0],
        'small_file_ratio': calculate_small_file_ratio()
    }
```

---

## 📐 Architecture Recommendations

### 6. Maintenance Orchestration

Add a new Step Function state machine for maintenance:

```
┌─────────────────────────────────────────────────────────┐
│        iceberg-maintenance-orchestrator                 │
├─────────────────────────────────────────────────────────┤
│  1. GetTableHealth (Lambda)                             │
│     ↓                                                   │
│  2. CheckSnapshotThreshold (Choice)                     │
│     → Yes: ExpireSnapshots (Glue/Athena)                │
│     ↓                                                   │
│  3. CheckManifestThreshold (Choice)                     │
│     → Yes: RewriteManifests (Glue)                      │
│     ↓                                                   │
│  4. CheckSmallFileRatio (Choice)                        │
│     → Yes: CompactHotWindow (Glue)                      │
│     ↓                                                   │
│  5. RemoveOrphanFiles (Glue/Athena)                     │
│     ↓                                                   │
│  6. LogMetrics → Success                                │
└─────────────────────────────────────────────────────────┘
```

**Schedule:** Daily or after every N curation runs.

---

## 🔧 Table Properties to Set

Add these to the Curated table DDL in `modules/catalog/main.tf`:

```sql
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet',
  'write.target-file-size-bytes' = '268435456',  -- 256MB
  'write.distribution-mode' = 'range',
  'commit.manifest.target-size-bytes' = '8388608',  -- 8MB
  'commit.manifest.min-count-to-merge' = '50',
  'history.expire.max-snapshot-age-ms' = '604800000'  -- 7 days
)
```

---

## 📋 Implementation Checklist

- [ ] Add snapshot expiration to Glue job
- [ ] Add orphan file cleanup (weekly schedule)
- [ ] Add manifest rewrite after expiration
- [ ] Set table properties for auto-optimization
- [ ] Create maintenance Step Function
- [ ] Add table health monitoring Lambda
- [ ] Add CloudWatch metrics for Iceberg health

---

## ⚠️ Notes

1. **Athena Limitations:** Many `CALL system.*` procedures require Spark, not Athena
2. **Glue 4.0 Compatibility:** Test each procedure in Glue before production
3. **POC First:** Test on dev tables before applying to production

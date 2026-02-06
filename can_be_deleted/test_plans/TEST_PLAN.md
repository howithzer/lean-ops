# Lean-Ops: Test Plan

## Test Categories

| Category | Description | Status |
|----------|-------------|--------|
| Unit Tests | Common library functions | ✅ Ready |
| Integration Tests | Lambda handlers with mocked AWS | 🔄 In Progress |
| End-to-End Tests | Full pipeline with live AWS | 📋 Planned |

---

## Unit Tests

### `tests/test_common.py`

Tests the common library modules:

| Module | Test Count | Coverage |
|--------|------------|----------|
| `topic_utils` | 7 tests | Topic extraction from ARNs |
| `error_classification` | 11 tests | DROP/RETRY classification |
| `aws_clients` | 4 tests | Client caching and cleanup |
| `checkpoint_utils` | 2 tests | CheckpointData structure |

### Run Tests

```bash
# Install dependencies
pip install pytest moto boto3

# Run all unit tests
pytest tests/test_common.py -v

# Run with coverage
pytest tests/test_common.py -v --cov=modules/compute/lambda/common
```

---

## Integration Tests (Planned)

### Test Scenarios

| Test | Input | Expected Output |
|------|-------|-----------------|
| SQS Processor Happy Path | Valid SQS event | Firehose batch sent |
| SQS Processor Malformed JSON | Invalid JSON body | DROP, no failure |
| SQS Processor Throttling | Firehose throttle error | RETRY, batchItemFailures |
| DLQ Processor | DLQ message | S3 archive + DynamoDB log |
| GetAllCheckpoints | Topic name | Checkpoint data returned |

### Mock Setup

```python
from moto import mock_dynamodb, mock_s3, mock_firehose

@mock_dynamodb
@mock_s3
def test_dlq_processor_archives_to_s3():
    # Setup mock S3 bucket
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket='test-bucket')
    
    # Setup mock DynamoDB table
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    dynamodb.create_table(...)
    
    # Test handler
    from dlq_processor.handler import lambda_handler
    result = lambda_handler(event, context)
    assert result['processed'] == 1
```

---

## End-to-End Tests

### E2E Test Suite

| Test | Description | Data Generator Config |
|------|-------------|----------------------|
| 01: Happy Path | 500 records, no duplicates | `test_01_happy_path.json` |
| 02: Network Duplicates | 30% duplicate messageId | `test_02_network_duplicates.json` |
| 03: Business Corrections | 20% corrections on idempotency_key | `test_03_business_corrections.json` |
| 04: Schema Drift | 30% with new columns | `test_04_schema_drift.json` |
| 05: Error Handling | 10% malformed JSON | `test_05_error_handling.json` |

### Validation Queries

```sql
-- RAW count
SELECT COUNT(*) FROM iceberg_raw_db.events_staging;

-- DLQ archived messages
SELECT COUNT(*) FROM aws_s3.access_bucket('lean-ops-dev-iceberg', 'dlq-archive/events/');

-- Error tracker
SELECT * FROM lean_ops_error_tracker WHERE topic_name = 'events';
```

---

## Error Classification Matrix

| Error Type | Action | Test Case |
|------------|--------|-----------|
| `JSONDecodeError` | DROP | Malformed JSON body |
| `Expecting value` | DROP | Empty payload |
| `KeyError` | DROP | Missing required field |
| `ProvisionedThroughputExceededException` | RETRY | Firehose throttle |
| `Connection timed out` | RETRY | Network timeout |
| `Unknown error` | RETRY | Unclassified error |

---

## CI/CD Integration

### GitHub Actions Test Job

```yaml
test:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - name: Setup Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    - name: Install dependencies
      run: pip install pytest moto boto3
    - name: Run tests
      run: pytest tests/ -v --junitxml=test-results.xml
    - name: Upload results
      uses: actions/upload-artifact@v3
      with:
        name: test-results
        path: test-results.xml
```

---

## Test Data Generator

The data generator is located at:
```
/Users/sagarm/Documents/Projects/Huntington_Exercises/Testing_Framework_EEH_2.0/sample-data-generator
```

### Generate Test Data

```bash
cd /Users/sagarm/Documents/Projects/Huntington_Exercises/Testing_Framework_EEH_2.0/sample-data-generator
source .venv/bin/activate

# Test 01: Happy Path
python -m data_injector.main --config ../../lean-ops/tests/configs/test_01_happy_path.json
```

"""
==============================================================================
UNIT TESTS FOR COMMON LIBRARY
==============================================================================
What are these tests for? (ELI5)
--------------------------------
These tests make sure our shared utilities work correctly BEFORE we deploy.

Think of it like testing your car's brakes in the driveway before driving on
the highway. If something is broken, we catch it here.

What gets tested:
- topic_utils: Extracting topic names from AWS ARNs
- error_classification: Deciding if errors should RETRY or DROP
- aws_clients: Making sure boto3 clients are cached (not recreated every time)
- checkpoint_utils: Data structures for tracking progress

How to run:
    cd /path/to/lean-ops
    source .venv/bin/activate
    pytest tests/test_common.py -v

Requirements:
    pip install pytest moto boto3
==============================================================================
"""
import pytest
import json
import sys
import os

# ==============================================================================
# PATH SETUP
# ==============================================================================
# We need to tell Python where to find the 'common' library.
# It lives in modules/compute/lambda/common/, so we add that to the Python path.
# ==============================================================================
common_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
lambda_path = os.path.join(common_path, 'modules', 'compute', 'lambda')
if lambda_path not in sys.path:
    sys.path.insert(0, lambda_path)


# ==============================================================================
# TESTS: topic_utils
# ==============================================================================
# What does topic_utils do?
# -------------------------
# It extracts the "topic name" from an AWS ARN (Amazon Resource Name).
# 
# Example:
#   Input:  "arn:aws:sqs:us-east-1:123456789012:lean-ops-dev-events-queue"
#   Output: "events"
#
# Why do we need this?
# The SQS Processor Lambda needs to know which topic a message came from
# so it can route it to the correct Iceberg table.
# ==============================================================================

class TestTopicUtils:
    """Tests for extracting topic names from AWS ARNs."""
    
    def test_extract_topic_from_queue_arn(self):
        """
        Given: An SQS queue ARN like "lean-ops-dev-events-queue"
        Expect: Extract "events" as the topic name
        """
        from common.topic_utils import extract_topic_from_arn
        
        arn = "arn:aws:sqs:us-east-1:123456789012:lean-ops-dev-events-queue"
        assert extract_topic_from_arn(arn) == "events"
    
    def test_extract_topic_from_prod_arn(self):
        """Test with a production-style ARN (different region/account)."""
        from common.topic_utils import extract_topic_from_arn
        
        arn = "arn:aws:sqs:us-west-2:999999999999:lean-ops-prod-orders-queue"
        assert extract_topic_from_arn(arn) == "orders"
    
    def test_extract_topic_from_payment_arn(self):
        """Test with the payments topic."""
        from common.topic_utils import extract_topic_from_arn
        
        arn = "arn:aws:sqs:us-east-1:123456789012:lean-ops-dev-payments-queue"
        assert extract_topic_from_arn(arn) == "payments"
    
    def test_extract_topic_from_empty_arn(self):
        """
        Edge case: Empty string should return "unknown" (not crash).
        """
        from common.topic_utils import extract_topic_from_arn
        
        assert extract_topic_from_arn("") == "unknown"
    
    def test_extract_topic_from_none_arn(self):
        """
        Edge case: None should return "unknown" (not crash).
        """
        from common.topic_utils import extract_topic_from_arn
        
        assert extract_topic_from_arn(None) == "unknown"
    
    def test_extract_topic_from_dlq_attributes(self):
        """
        DLQ messages have a 'DeadLetterQueueSourceArn' attribute.
        We should be able to extract the original topic from there.
        """
        from common.topic_utils import extract_topic_from_dlq_attributes
        
        attributes = {
            'DeadLetterQueueSourceArn': 'arn:aws:sqs:us-east-1:123:lean-ops-dev-payments-queue'
        }
        assert extract_topic_from_dlq_attributes(attributes) == "payments"
    
    def test_extract_topic_from_missing_dlq_attribute(self):
        """
        Edge case: DLQ message missing the source ARN.
        """
        from common.topic_utils import extract_topic_from_dlq_attributes
        
        assert extract_topic_from_dlq_attributes({}) == "unknown"


# ==============================================================================
# TESTS: error_classification
# ==============================================================================
# What does error_classification do?
# ----------------------------------
# It decides what to do when something goes wrong:
#   - DROP: The message is bad (malformed JSON). Don't retry, just log it.
#   - RETRY: Temporary problem (timeout, throttle). Try again later.
#
# Why is this important?
# If we retry bad data forever, we waste resources.
# If we drop transient errors, we lose valid data.
# ==============================================================================

class TestErrorClassification:
    """Tests for error classification logic."""
    
    def test_classify_json_decode_error(self):
        """
        Invalid JSON should be DROPPED (not retried).
        Retrying won't fix broken data.
        """
        from common.error_classification import classify_error, ErrorAction
        
        error = json.JSONDecodeError("Expecting value", "", 0)
        result = classify_error(error)
        
        assert result.action == ErrorAction.DROP
        assert result.error_type == "MALFORMED_JSON"
    
    def test_classify_json_expecting_property(self):
        """
        Another type of JSON error (missing property name).
        Also should be DROPPED.
        """
        from common.error_classification import classify_error, ErrorAction
        
        error = Exception("Expecting property name enclosed in double quotes")
        result = classify_error(error)
        
        assert result.action == ErrorAction.DROP
        assert result.error_type == "MALFORMED_JSON"
    
    def test_classify_key_error(self):
        """
        KeyError means a required field is missing.
        This is a data problem, not a transient error. DROP it.
        """
        from common.error_classification import classify_error, ErrorAction
        
        error = KeyError("missing_field")
        result = classify_error(error)
        
        assert result.action == ErrorAction.DROP
        assert result.error_type == "INVALID_CONTRACT"
    
    def test_classify_throttle_error(self):
        """
        Throttling means we're hitting rate limits.
        This is temporary - RETRY after backoff.
        """
        from common.error_classification import classify_error, ErrorAction
        
        error = Exception("ProvisionedThroughputExceededException: Rate exceeded")
        result = classify_error(error)
        
        assert result.action == ErrorAction.RETRY
        assert result.error_type == "THROTTLE"
    
    def test_classify_capacity_error(self):
        """
        Capacity exceeded = temporary. RETRY.
        """
        from common.error_classification import classify_error, ErrorAction
        
        error = Exception("Insufficient capacity for the delivery stream")
        result = classify_error(error)
        
        assert result.action == ErrorAction.RETRY
        assert result.error_type == "THROTTLE"
    
    def test_classify_timeout_error(self):
        """
        Timeout = network hiccup. RETRY.
        """
        from common.error_classification import classify_error, ErrorAction
        
        error = Exception("Connection timed out after 30 seconds")
        result = classify_error(error)
        
        assert result.action == ErrorAction.RETRY
        assert result.error_type == "TIMEOUT"
    
    def test_classify_connection_error(self):
        """
        Connection failed = network problem. RETRY.
        """
        from common.error_classification import classify_error, ErrorAction
        
        error = Exception("Could not connect to endpoint")
        result = classify_error(error)
        
        assert result.action == ErrorAction.RETRY
        assert result.error_type == "TIMEOUT"
    
    def test_classify_unknown_error(self):
        """
        Unknown errors default to RETRY (safer than dropping).
        """
        from common.error_classification import classify_error, ErrorAction
        
        error = Exception("Something unexpected happened")
        result = classify_error(error)
        
        assert result.action == ErrorAction.RETRY
        assert result.error_type == "UNKNOWN"
    
    def test_classify_dlq_error_valid_json(self):
        """
        DLQ message with valid JSON = processing error (not parse error).
        """
        from common.error_classification import classify_dlq_error
        
        body = '{"message": "test"}'
        assert classify_dlq_error(body) == "PROCESSING_ERROR"
    
    def test_classify_dlq_error_invalid_json(self):
        """
        DLQ message with invalid JSON = malformed data.
        """
        from common.error_classification import classify_dlq_error
        
        body = "not valid json"
        assert classify_dlq_error(body) == "MALFORMED_JSON"
    
    def test_classify_dlq_error_empty_body(self):
        """
        DLQ message with empty body = malformed.
        """
        from common.error_classification import classify_dlq_error
        
        body = ""
        assert classify_dlq_error(body) == "MALFORMED_JSON"


# ==============================================================================
# TESTS: aws_clients
# ==============================================================================
# What does aws_clients do?
# -------------------------
# Creates boto3 clients/resources with CACHING.
# 
# Why cache clients?
# Creating a new boto3 client takes ~50-100ms.
# If we process 1000 messages and create a new client each time,
# that's 50-100 SECONDS of overhead!
#
# With caching, we create ONE client and reuse it.
# ==============================================================================

class TestAwsClients:
    """Tests for AWS client caching."""
    
    def test_client_caching(self, monkeypatch):
        """
        Test: Calling get_s3_client() twice returns the SAME object.
        """
        monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
        from common.aws_clients import get_s3_client, clear_client_cache
        
        clear_client_cache()
        
        client1 = get_s3_client()
        client2 = get_s3_client()
        
        # Same object = caching works
        assert client1 is client2
    
    def test_firehose_client_caching(self, monkeypatch):
        """
        Same test for Firehose client.
        """
        monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
        from common.aws_clients import get_firehose_client, clear_client_cache
        
        clear_client_cache()
        
        client1 = get_firehose_client()
        client2 = get_firehose_client()
        
        assert client1 is client2
    
    def test_dynamodb_resource_caching(self, monkeypatch):
        """
        Same test for DynamoDB resource.
        """
        monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
        from common.aws_clients import get_dynamodb_resource, clear_client_cache
        
        clear_client_cache()
        
        resource1 = get_dynamodb_resource()
        resource2 = get_dynamodb_resource()
        
        assert resource1 is resource2
    
    def test_clear_cache(self, monkeypatch):
        """
        Test: clear_client_cache() forces new instances to be created.
        """
        monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
        from common.aws_clients import get_s3_client, clear_client_cache
        
        client1 = get_s3_client()
        clear_client_cache()
        client2 = get_s3_client()
        
        # Different objects after clearing
        assert client1 is not client2


# ==============================================================================
# TESTS: checkpoint_utils
# ==============================================================================
# What does checkpoint_utils do?
# ------------------------------
# Provides data structures for tracking pipeline progress.
# 
# CheckpointData holds:
# - raw_snapshot: Last processed snapshot in RAW
# - standardized_checkpoint: Last processed snapshot in Standardized
# - semantic_checkpoint: Last processed snapshot in Semantic (future)
# - schema_exists: Whether the schema file exists (gate check)
# ==============================================================================

class TestCheckpointUtils:
    """Tests for checkpoint data structures."""
    
    def test_checkpoint_data_structure(self):
        """
        Test: CheckpointData dataclass holds all expected fields.
        """
        from common.checkpoint_utils import CheckpointData
        
        data = CheckpointData(
            raw_snapshot="snap-001",
            standardized_checkpoint="snap-000",
            semantic_checkpoint="snap-000",
            schema_exists=True
        )
        
        assert data.raw_snapshot == "snap-001"
        assert data.standardized_checkpoint == "snap-000"
        assert data.semantic_checkpoint == "snap-000"
        assert data.schema_exists is True
    
    def test_checkpoint_data_with_none(self):
        """
        Test: CheckpointData works with None values (first run scenario).
        """
        from common.checkpoint_utils import CheckpointData
        
        data = CheckpointData(
            raw_snapshot=None,
            standardized_checkpoint=None,
            semantic_checkpoint=None,
            schema_exists=False
        )
        
        assert data.raw_snapshot is None
        assert data.schema_exists is False


# ==============================================================================
# RUN TESTS
# ==============================================================================
# To run from command line:
#   pytest tests/test_common.py -v
#
# To run this file directly:
#   python tests/test_common.py
# ==============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])

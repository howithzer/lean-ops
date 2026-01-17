"""
Unit tests for common library.

Run with: pytest tests/test_common.py -v

Requires:
  - pytest
  - moto (for AWS mocking)
"""
import pytest
import json
import sys
import os

# Add common library to path BEFORE imports
common_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
lambda_path = os.path.join(common_path, 'modules', 'compute', 'lambda')
if lambda_path not in sys.path:
    sys.path.insert(0, lambda_path)


# =============================================================================
# TESTS: topic_utils
# =============================================================================

class TestTopicUtils:
    """Tests for topic_utils module."""
    
    def test_extract_topic_from_queue_arn(self):
        """Test extracting topic from SQS queue ARN."""
        from common.topic_utils import extract_topic_from_arn
        
        arn = "arn:aws:sqs:us-east-1:123456789012:lean-ops-dev-events-queue"
        assert extract_topic_from_arn(arn) == "events"
    
    def test_extract_topic_from_prod_arn(self):
        """Test extracting topic from production ARN."""
        from common.topic_utils import extract_topic_from_arn
        
        arn = "arn:aws:sqs:us-west-2:999999999999:lean-ops-prod-orders-queue"
        assert extract_topic_from_arn(arn) == "orders"
    
    def test_extract_topic_from_payment_arn(self):
        """Test extracting payment topic."""
        from common.topic_utils import extract_topic_from_arn
        
        arn = "arn:aws:sqs:us-east-1:123456789012:lean-ops-dev-payments-queue"
        assert extract_topic_from_arn(arn) == "payments"
    
    def test_extract_topic_from_empty_arn(self):
        """Test handling empty ARN."""
        from common.topic_utils import extract_topic_from_arn
        
        assert extract_topic_from_arn("") == "unknown"
    
    def test_extract_topic_from_none_arn(self):
        """Test handling None ARN."""
        from common.topic_utils import extract_topic_from_arn
        
        assert extract_topic_from_arn(None) == "unknown"
    
    def test_extract_topic_from_dlq_attributes(self):
        """Test extracting topic from DLQ message attributes."""
        from common.topic_utils import extract_topic_from_dlq_attributes
        
        attributes = {
            'DeadLetterQueueSourceArn': 'arn:aws:sqs:us-east-1:123:lean-ops-dev-payments-queue'
        }
        assert extract_topic_from_dlq_attributes(attributes) == "payments"
    
    def test_extract_topic_from_missing_dlq_attribute(self):
        """Test handling missing DLQ source ARN."""
        from common.topic_utils import extract_topic_from_dlq_attributes
        
        assert extract_topic_from_dlq_attributes({}) == "unknown"


# =============================================================================
# TESTS: error_classification
# =============================================================================

class TestErrorClassification:
    """Tests for error_classification module."""
    
    def test_classify_json_decode_error(self):
        """Test JSON decode errors are classified as DROP."""
        from common.error_classification import classify_error, ErrorAction
        
        error = json.JSONDecodeError("Expecting value", "", 0)
        result = classify_error(error)
        
        assert result.action == ErrorAction.DROP
        assert result.error_type == "MALFORMED_JSON"
    
    def test_classify_json_expecting_property(self):
        """Test 'expecting property' errors are classified as DROP."""
        from common.error_classification import classify_error, ErrorAction
        
        error = Exception("Expecting property name enclosed in double quotes")
        result = classify_error(error)
        
        assert result.action == ErrorAction.DROP
        assert result.error_type == "MALFORMED_JSON"
    
    def test_classify_key_error(self):
        """Test KeyError is classified as DROP."""
        from common.error_classification import classify_error, ErrorAction
        
        error = KeyError("missing_field")
        result = classify_error(error)
        
        assert result.action == ErrorAction.DROP
        assert result.error_type == "INVALID_CONTRACT"
    
    def test_classify_throttle_error(self):
        """Test throttle errors are classified as RETRY."""
        from common.error_classification import classify_error, ErrorAction
        
        error = Exception("ProvisionedThroughputExceededException: Rate exceeded")
        result = classify_error(error)
        
        assert result.action == ErrorAction.RETRY
        assert result.error_type == "THROTTLE"
    
    def test_classify_capacity_error(self):
        """Test capacity errors are classified as RETRY."""
        from common.error_classification import classify_error, ErrorAction
        
        error = Exception("Insufficient capacity for the delivery stream")
        result = classify_error(error)
        
        assert result.action == ErrorAction.RETRY
        assert result.error_type == "THROTTLE"
    
    def test_classify_timeout_error(self):
        """Test timeout errors are classified as RETRY."""
        from common.error_classification import classify_error, ErrorAction
        
        error = Exception("Connection timed out after 30 seconds")
        result = classify_error(error)
        
        assert result.action == ErrorAction.RETRY
        assert result.error_type == "TIMEOUT"
    
    def test_classify_connection_error(self):
        """Test connection errors are classified as RETRY."""
        from common.error_classification import classify_error, ErrorAction
        
        error = Exception("Could not connect to endpoint")
        result = classify_error(error)
        
        assert result.action == ErrorAction.RETRY
        assert result.error_type == "TIMEOUT"
    
    def test_classify_unknown_error(self):
        """Test unknown errors default to RETRY."""
        from common.error_classification import classify_error, ErrorAction
        
        error = Exception("Something unexpected happened")
        result = classify_error(error)
        
        assert result.action == ErrorAction.RETRY
        assert result.error_type == "UNKNOWN"
    
    def test_classify_dlq_error_valid_json(self):
        """Test DLQ error classification for valid JSON."""
        from common.error_classification import classify_dlq_error
        
        body = '{"message": "test"}'
        assert classify_dlq_error(body) == "PROCESSING_ERROR"
    
    def test_classify_dlq_error_invalid_json(self):
        """Test DLQ error classification for invalid JSON."""
        from common.error_classification import classify_dlq_error
        
        body = "not valid json"
        assert classify_dlq_error(body) == "MALFORMED_JSON"
    
    def test_classify_dlq_error_empty_body(self):
        """Test DLQ error classification for empty body."""
        from common.error_classification import classify_dlq_error
        
        body = ""
        assert classify_dlq_error(body) == "MALFORMED_JSON"


# =============================================================================
# TESTS: aws_clients
# =============================================================================

class TestAwsClients:
    """Tests for aws_clients module."""
    
    def test_client_caching(self, monkeypatch):
        """Test that clients are cached (same instance returned)."""
        monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
        from common.aws_clients import get_s3_client, clear_client_cache
        
        # Clear cache first
        clear_client_cache()
        
        client1 = get_s3_client()
        client2 = get_s3_client()
        
        assert client1 is client2
    
    def test_firehose_client_caching(self, monkeypatch):
        """Test Firehose client caching."""
        monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
        from common.aws_clients import get_firehose_client, clear_client_cache
        
        clear_client_cache()
        
        client1 = get_firehose_client()
        client2 = get_firehose_client()
        
        assert client1 is client2
    
    def test_dynamodb_resource_caching(self, monkeypatch):
        """Test DynamoDB resource caching."""
        monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
        from common.aws_clients import get_dynamodb_resource, clear_client_cache
        
        clear_client_cache()
        
        resource1 = get_dynamodb_resource()
        resource2 = get_dynamodb_resource()
        
        assert resource1 is resource2
    
    def test_clear_cache(self, monkeypatch):
        """Test cache clearing creates new instances."""
        monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
        from common.aws_clients import get_s3_client, clear_client_cache
        
        client1 = get_s3_client()
        clear_client_cache()
        client2 = get_s3_client()
        
        # After clearing, should be a new instance
        assert client1 is not client2


# =============================================================================
# TESTS: checkpoint_utils (basic structure tests)
# =============================================================================

class TestCheckpointUtils:
    """Tests for checkpoint_utils module."""
    
    def test_checkpoint_data_structure(self):
        """Test CheckpointData dataclass structure."""
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
        """Test CheckpointData with None values."""
        from common.checkpoint_utils import CheckpointData
        
        data = CheckpointData(
            raw_snapshot=None,
            standardized_checkpoint=None,
            semantic_checkpoint=None,
            schema_exists=False
        )
        
        assert data.raw_snapshot is None
        assert data.schema_exists is False


# =============================================================================
# RUN TESTS
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])

"""
Unit tests for common library.

Run with: pytest tests/test_common.py -v
"""
import pytest
import json


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
    
    def test_extract_topic_from_empty_arn(self):
        """Test handling empty ARN."""
        from common.topic_utils import extract_topic_from_arn
        
        assert extract_topic_from_arn("") == "unknown"
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


class TestErrorClassification:
    """Tests for error_classification module."""
    
    def test_classify_json_decode_error(self):
        """Test JSON decode errors are classified as DROP."""
        from common.error_classification import classify_error, ErrorAction
        
        error = json.JSONDecodeError("Expecting value", "", 0)
        result = classify_error(error)
        
        assert result.action == ErrorAction.DROP
        assert result.error_type == "MALFORMED_JSON"
    
    def test_classify_throttle_error(self):
        """Test throttle errors are classified as RETRY."""
        from common.error_classification import classify_error, ErrorAction
        
        error = Exception("ProvisionedThroughputExceededException: Rate exceeded")
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


class TestAwsClients:
    """Tests for aws_clients module."""
    
    def test_client_caching(self):
        """Test that clients are cached (same instance returned)."""
        from common.aws_clients import get_s3_client, clear_client_cache
        
        # Clear cache first
        clear_client_cache()
        
        client1 = get_s3_client()
        client2 = get_s3_client()
        
        assert client1 is client2
    
    def test_clear_cache(self):
        """Test cache clearing."""
        from common.aws_clients import get_s3_client, clear_client_cache
        
        client1 = get_s3_client()
        clear_client_cache()
        client2 = get_s3_client()
        
        # After clearing, should be a new instance
        # (In real boto3, this creates a new client)
        assert client1 is not client2


# Conftest for path setup
@pytest.fixture(autouse=True)
def setup_path():
    """Add common library to path."""
    import sys
    import os
    
    common_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    lambda_path = os.path.join(common_path, 'modules', 'compute', 'lambda')
    
    if lambda_path not in sys.path:
        sys.path.insert(0, lambda_path)

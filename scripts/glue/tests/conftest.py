"""
PyTest fixtures and configuration for Glue job tests.
"""

import os
import sys
import pytest

# Add the glue scripts directory to the path so we can import utils
GLUE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, GLUE_DIR)


@pytest.fixture
def clean_env():
    """
    Fixture that provides a clean environment for testing.
    Saves and restores environment variables.
    """
    original_env = os.environ.copy()
    yield os.environ
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def sample_json_payloads():
    """
    Sample JSON payloads for testing flatten functionality.
    """
    return [
        # Simple flat payload
        '{"userId": "123", "eventType": "click"}',
        
        # Nested payload
        '{"user": {"id": "456", "name": "John"}, "event": "login"}',
        
        # Deeply nested payload
        '{"level1": {"level2": {"level3": {"level4": {"level5": "deep"}}}}}',
        
        # Payload with arrays
        '{"items": [1, 2, 3], "tags": ["a", "b", "c"]}',
        
        # Payload with special characters in keys
        '{"user-id": "789", "event.type": "purchase"}',
        
        # Empty payload
        '{}',
        
        # Payload with nulls
        '{"userId": null, "eventType": "view"}',
    ]


@pytest.fixture
def sample_config_env(clean_env):
    """
    Set up environment variables for Config tests.
    """
    clean_env["TOPIC_NAME"] = "test_topic"
    clean_env["RAW_DATABASE"] = "test_raw_db"
    clean_env["STANDARDIZED_DATABASE"] = "test_standardized_db"
    clean_env["ENABLE_DEEP_FLATTEN"] = "true"
    clean_env["MAX_FLATTEN_DEPTH"] = "3"
    return clean_env


# Optional: Spark session fixture (requires pyspark)
@pytest.fixture(scope="session")
def spark_session():
    """
    Create a local Spark session for testing.
    Skipped if pyspark is not installed.
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .master("local[2]") \
            .appName("glue-tests") \
            .config("spark.driver.memory", "1g") \
            .getOrCreate()
        yield spark
        spark.stop()
    except ImportError:
        pytest.skip("pyspark not installed")

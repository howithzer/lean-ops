"""
AWS client factories with lazy initialization.

Using @lru_cache ensures clients are created once per Lambda container,
and allows easy mocking in tests.
"""
import boto3
from functools import lru_cache


@lru_cache(maxsize=1)
def get_firehose_client():
    """Get or create Firehose client (cached per container)."""
    return boto3.client('firehose')


@lru_cache(maxsize=1)
def get_s3_client():
    """Get or create S3 client (cached per container)."""
    return boto3.client('s3')


@lru_cache(maxsize=1)
def get_dynamodb_resource():
    """Get or create DynamoDB resource (cached per container)."""
    return boto3.resource('dynamodb')


@lru_cache(maxsize=1)
def get_dynamodb_client():
    """Get or create DynamoDB client (cached per container)."""
    return boto3.client('dynamodb')


@lru_cache(maxsize=1)
def get_glue_client():
    """Get or create Glue client (cached per container)."""
    return boto3.client('glue')


@lru_cache(maxsize=1)
def get_athena_client():
    """Get or create Athena client (cached per container)."""
    return boto3.client('athena')


def clear_client_cache():
    """Clear all cached clients. Useful for testing."""
    get_firehose_client.cache_clear()
    get_s3_client.cache_clear()
    get_dynamodb_resource.cache_clear()
    get_dynamodb_client.cache_clear()
    get_glue_client.cache_clear()
    get_athena_client.cache_clear()

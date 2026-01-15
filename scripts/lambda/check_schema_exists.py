"""
Lambda: check_schema_exists
===========================
Checks if a schema file exists in S3 for a given topic.
Used as Schema Gate in the Step Function to skip topics
that haven't been onboarded yet.
"""

import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')


def lambda_handler(event, context):
    """
    Check if schema file exists for a topic.
    
    Input:
        {
            "bucket": "lean-ops-development-iceberg",
            "key": "schemas/events.json"
        }
    
    Output:
        {
            "exists": true/false,
            "bucket": "...",
            "key": "..."
        }
    """
    bucket = event['bucket']
    key = event['key']
    
    logger.info(f"Checking schema existence: s3://{bucket}/{key}")
    
    try:
        s3.head_object(Bucket=bucket, Key=key)
        logger.info(f"Schema found: s3://{bucket}/{key}")
        return {
            "exists": True,
            "bucket": bucket,
            "key": key
        }
    except s3.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == '404':
            logger.info(f"Schema NOT found: s3://{bucket}/{key}")
            return {
                "exists": False,
                "bucket": bucket,
                "key": key
            }
        # Re-raise unexpected errors
        logger.error(f"Error checking schema: {e}")
        raise

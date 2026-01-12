"""
Topic extraction utilities.

Extracts topic names from AWS ARNs (SQS queues, DLQ sources).
"""


def extract_topic_from_arn(arn: str) -> str:
    """
    Extract topic name from an AWS ARN.
    
    Supports:
    - SQS queue ARN: arn:aws:sqs:region:account:lean-ops-{env}-{topic}-queue
    - DLQ source attribute: Same pattern
    
    Args:
        arn: AWS ARN string
    
    Returns:
        Topic name (e.g., 'events', 'orders', 'payments')
        Returns 'unknown' if topic cannot be extracted.
    
    Examples:
        >>> extract_topic_from_arn("arn:aws:sqs:us-east-1:123:lean-ops-dev-events-queue")
        'events'
        >>> extract_topic_from_arn("arn:aws:sqs:us-east-1:123:lean-ops-prod-orders-queue")
        'orders'
    """
    if not arn:
        return 'unknown'
    
    # Get the resource name (last part of ARN)
    resource_name = arn.split(':')[-1]
    
    # Remove -queue suffix if present
    resource_name = resource_name.replace('-queue', '')
    
    # Split by hyphen: lean-ops-{env}-{topic}
    parts = resource_name.split('-')
    
    # Topic is the last part (after env)
    if len(parts) >= 3:
        return parts[-1]
    
    return 'unknown'


def extract_topic_from_dlq_attributes(attributes: dict) -> str:
    """
    Extract original topic from DLQ message attributes.
    
    When a message goes to DLQ, SQS adds the source queue ARN in attributes.
    
    Args:
        attributes: SQS message attributes dict
    
    Returns:
        Topic name or 'unknown'
    """
    source_arn = attributes.get('DeadLetterQueueSourceArn', '')
    return extract_topic_from_arn(source_arn)

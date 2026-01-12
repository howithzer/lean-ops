"""
Error classification for fail-aware processing.

Classifies errors into DROP (don't retry) or RETRY (let SQS retry → DLQ).
"""
from enum import Enum
from dataclasses import dataclass


class ErrorAction(Enum):
    """Action to take for a given error."""
    DROP = 'DROP'    # Log and return success (malformed data, won't fix)
    RETRY = 'RETRY'  # Return in batchItemFailures (transient, may succeed)


@dataclass
class ErrorClassification:
    """Result of error classification."""
    action: ErrorAction
    error_type: str
    message: str


def classify_error(error: Exception) -> ErrorClassification:
    """
    Classify an error to determine handling strategy.
    
    DROP: Malformed data or invalid schema - retrying won't help.
    RETRY: Transient errors (throttling, timeout) - will likely succeed.
    
    Args:
        error: The exception that occurred
    
    Returns:
        ErrorClassification with action, type, and message
    
    Examples:
        >>> classify_error(json.JSONDecodeError("Expecting value", "", 0))
        ErrorClassification(action=DROP, error_type='MALFORMED_JSON', ...)
        
        >>> classify_error(Exception("ProvisionedThroughputExceededException"))
        ErrorClassification(action=RETRY, error_type='THROTTLE', ...)
    """
    error_msg = str(error).lower()
    
    # DROP: Malformed data - retrying won't help
    if any(x in error_msg for x in ['json', 'decode', 'expecting value', 'expecting property']):
        return ErrorClassification(ErrorAction.DROP, 'MALFORMED_JSON', str(error))
    
    if any(x in error_msg for x in ['keyerror', 'missing required', 'invalid schema']):
        return ErrorClassification(ErrorAction.DROP, 'INVALID_CONTRACT', str(error))
    
    # RETRY: Transient errors - will likely succeed on retry
    if any(x in error_msg for x in ['throttl', 'provision', 'capacity']):
        return ErrorClassification(ErrorAction.RETRY, 'THROTTLE', str(error))
    
    if any(x in error_msg for x in ['timeout', 'timed out', 'connection']):
        return ErrorClassification(ErrorAction.RETRY, 'TIMEOUT', str(error))
    
    # Default: RETRY - let SQS retry → eventually DLQ
    return ErrorClassification(ErrorAction.RETRY, 'UNKNOWN', str(error))


def classify_dlq_error(body: str) -> str:
    """
    Classify why a message ended up in DLQ.
    
    Args:
        body: Original message body (may or may not be valid JSON)
    
    Returns:
        Error type string
    """
    import json
    
    try:
        json.loads(body)
        # If it parsed, it was likely a processing error
        return 'PROCESSING_ERROR'
    except json.JSONDecodeError:
        return 'MALFORMED_JSON'
    except Exception:
        return 'UNKNOWN_ERROR'

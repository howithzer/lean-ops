import os
import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table_name = os.environ.get('PIPELINE_REGISTRY_TABLE')

def lambda_handler(event, context):
    logger.info("Fetching active topics from %s", table_name)
    
    table = dynamodb.Table(table_name)
    topics = []
    
    try:
        # We need all topics where flow_control_flag == 'ENABLED'
        # A Scan with a FilterExpression is used since we have low table velocity
        # If the table grew to 10k+ items, a GSI would be preferable
        response = table.scan(
            FilterExpression="flow_control_flag = :active",
            ExpressionAttributeValues={":active": "ENABLED"}
        )
        
        items = response.get('Items', [])
        for item in items:
            topics.append(item.get('topic_name'))
            
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression="flow_control_flag = :active",
                ExpressionAttributeValues={":active": "ENABLED"},
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items = response.get('Items', [])
            for item in items:
                topics.append(item.get('topic_name'))
                
    except Exception as e:
        logger.error("Error scanning DynamoDB: %s", e)
        raise e
        
    logger.info("Found %d active topics for pipeline processing", len(topics))
    
    # Step Functions map state requires an array payload 
    return {
        "topics": topics
    }

"""
Firehose Transform Lambda: Dynamic Iceberg Table Routing

This Lambda is invoked by Firehose to transform records and add routing metadata.
It extracts topic_name from records and sets the destination Iceberg table dynamically.

Key feature: Uses otfMetadata to route records to different Iceberg tables
based on the topic_name field in each record.

Reference: AWS Firehose Developer Guide - "Route incoming records to different Iceberg tables"
"""

import json
import base64

# Configuration
DATABASE_NAME = 'iceberg_raw_db'
TABLE_SUFFIX = '_staging'


def lambda_handler(firehose_records_input, context):
    """
    Transform Firehose records and add dynamic Iceberg routing metadata.
    
    Each record's topic_name determines which Iceberg table it goes to:
        topic_name: "events" → iceberg_raw_db.events_staging
        topic_name: "orders" → iceberg_raw_db.orders_staging
        topic_name: "payments" → iceberg_raw_db.payments_staging
    """
    print(f"Received {len(firehose_records_input.get('records', []))} records from Firehose")
    
    firehose_records_output = {'records': []}
    
    # Track routing for logging
    routing_counts = {}
    
    for record in firehose_records_input.get('records', []):
        try:
            # Decode the record data
            payload_bytes = base64.b64decode(record['data'])
            data = json.loads(payload_bytes.decode('utf-8'))
            
            # Extract topic_name for routing
            topic_name = data.get('topic_name', 'default')
            routing_counts[topic_name] = routing_counts.get(topic_name, 0) + 1
            
            # Determine destination table
            destination_table = f"{topic_name}{TABLE_SUFFIX}"
            
            # Build output record with routing metadata
            output_record = {
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': record['data'],  # Keep original Base64 data
                'metadata': {
                    'otfMetadata': {
                        'destinationDatabaseName': DATABASE_NAME,
                        'destinationTableName': destination_table
                    }
                }
            }
            
            firehose_records_output['records'].append(output_record)
            
        except Exception as e:
            print(f"Error processing record {record.get('recordId')}: {e}")
            
            # Return failed record for retry
            firehose_records_output['records'].append({
                'recordId': record['recordId'],
                'result': 'ProcessingFailed',
                'data': record['data']
            })
    
    print(f"Routed records: {routing_counts}")
    
    return firehose_records_output

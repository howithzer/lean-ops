"""
Lean-Ops: Firehose Transform Lambda

This Lambda is invoked by Firehose to transform records and add routing metadata.
It extracts topic_name from each record and sets the destination Iceberg table dynamically.

Key feature: Uses otfMetadata to route records to different Iceberg tables
based on the topic_name field in each record.

Architecture:
    SHARED FIREHOSE
         │
         ▼
    THIS LAMBDA ──→ Adds destinationTableName
         │
         ├──→ Iceberg: events_staging
         ├──→ Iceberg: orders_staging
         └──→ Iceberg: payments_staging

Reference: AWS Firehose Developer Guide - "Route incoming records to different Iceberg tables"
"""

import json
import base64
import os

# Configuration
DATABASE_NAME = os.environ.get('DATABASE_NAME', 'iceberg_raw_db')
TABLE_SUFFIX = os.environ.get('TABLE_SUFFIX', '_staging')


def lambda_handler(firehose_records_input, context):
    """
    Transform Firehose records and add dynamic Iceberg routing metadata.
    
    Each record's topic_name determines which Iceberg table it goes to:
        topic_name: "events"   → iceberg_raw_db.events_staging
        topic_name: "orders"   → iceberg_raw_db.orders_staging
        topic_name: "payments" → iceberg_raw_db.payments_staging
    """
    records_input = firehose_records_input.get('records', [])
    print(f"[TRANSFORM] Received {len(records_input)} records")
    
    firehose_records_output = {'records': []}
    
    # Track routing for logging
    routing_counts = {}
    errors = 0
    
    for record in records_input:
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
            print(f"[TRANSFORM] Error processing record {record.get('recordId')}: {e}")
            errors += 1
            
            # Return failed record for retry
            firehose_records_output['records'].append({
                'recordId': record['recordId'],
                'result': 'ProcessingFailed',
                'data': record['data']
            })
    
    print(f"[TRANSFORM] Routing: {routing_counts}, Errors: {errors}")
    
    return firehose_records_output

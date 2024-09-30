import json
import boto3
import os
from botocore.exceptions import ClientError
from datetime import datetime
import logging
from collections import defaultdict

# Initialize AWS clients
sqs_client = boto3.client('sqs')
s3_client = boto3.client('s3')
comprehend = boto3.client('comprehend')
dynamodb = boto3.resource('dynamodb')

# Get environment variables
QUEUE_URL = os.environ['QUEUE_URL']
CLASSIFIER_ARN = os.environ['CLASSIFIER_ARN']
DATA_ACCESS_ROLE_ARN = os.environ['DATA_ACCESS_ROLE_ARN']
OUTPUT_BUCKET = os.environ['OUTPUT_BUCKET']
DYNAMODB_TABLE_NAME = os.environ['DYNAMODB_TABLE_NAME']

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Dictionary to store existing Comprehend jobs
existing_jobs = {}

def process_messages(messages, table):
    logger.info(f"Entering process_messages with {len(messages)} messages")
    if not messages:
        logger.info("No messages to process")
        return

    logger.info(f"Processing {len(messages)} messages")

    # Group messages by prefix
    prefix_groups = defaultdict(list)
    for message in messages:
        try:
            if isinstance(message, dict):
                message_body = json.loads(message['body'])
                s3_record = message_body['Records'][0]
                bucket_name = s3_record['s3']['bucket']['name']
                object_key = s3_record['s3']['object']['key']
                prefix = os.path.dirname(object_key)
            else:
                logger.error(f"Invalid message: {message}")
                continue
        except (ValueError, TypeError):
            logger.error(f"Invalid JSON message: {message['body']}")
            continue

        prefix_groups[prefix].append((message, bucket_name, object_key))

    logger.info(f"Grouped messages into {len(prefix_groups)} prefix groups")

    # Process each prefix group
    for prefix, group in prefix_groups.items():
        logger.info(f"Processing prefix group: {prefix}")
        process_prefix_group(prefix, group, table)

def process_prefix_group(prefix, group, table):
    bucket_name = group[0][1]  # All items in the group have the same bucket
    s3_uri = f's3://{bucket_name}/{prefix}/'

    if prefix in existing_jobs:
        job_name = existing_jobs[prefix]
        logger.info(f'Using existing Comprehend Classification job {job_name} for prefix {s3_uri}')
    else:
        job_name = f'document-classification-{datetime.now().strftime("%Y%m%d-%H%M%S")}'
        logger.info(f'Starting Comprehend Classification job {job_name} for prefix {s3_uri}')

        try:
            response = comprehend.start_document_classification_job(
                JobName=job_name,
                DocumentClassifierArn=CLASSIFIER_ARN,
                InputDataConfig={
                    'S3Uri': s3_uri,
                    'InputFormat': 'ONE_DOC_PER_FILE',
                    'DocumentReaderConfig': {
                        'DocumentReadAction': 'TEXTRACT_DETECT_DOCUMENT_TEXT',
                        'DocumentReadMode': 'FORCE_DOCUMENT_READ_ACTION'
                    }
                },
                OutputDataConfig={
                    'S3Uri': f's3://{OUTPUT_BUCKET}/comprehend/doc-class-output/'
                },
                DataAccessRoleArn=DATA_ACCESS_ROLE_ARN
            )

            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                existing_jobs[prefix] = job_name
            else:
                logger.error(f"Failed to start Comprehend job for prefix {s3_uri}. Response: {response}")
                raise Exception("Comprehend job creation failed")

        except Exception as e:
            logger.error(f"Error processing prefix {s3_uri}: {str(e)}")
            raise

    # Write to DynamoDB and delete SQS messages for all files in this group
    for message, _, object_key in group:
        table.put_item(
            Item={
                'bucket': bucket_name,
                'filename': os.path.basename(object_key),
                'prefix': prefix,
                'key': object_key,
                'job_name': job_name
            }
        )
        logger.info(f"Added item to DynamoDB for object: {object_key}")

        sqs_client.delete_message(
            QueueUrl=QUEUE_URL,
            ReceiptHandle=message['receiptHandle']
        )
        logger.info(f"Deleted message from SQS for object: {object_key}")

def lambda_handler(event, context):
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    logger.info(f"DynamoDB table: {DYNAMODB_TABLE_NAME}")

    try:
        # Log the incoming event
        logger.info(f"Received event: {json.dumps(event)}")

        # Check if there are any records in the event
        if 'Records' not in event or not event['Records']:
            logger.warning("No records found in the event")
            return {
                'statusCode': 200,
                'body': 'No records to process'
            }

        # Process the SQS messages
        messages = [record for record in event['Records']]
        process_messages(messages, table)

        return {
            'statusCode': 200,
            'body': f'Processed {len(event["Records"])} messages successfully.'
        }
    except ClientError as e:
        logger.error(f"AWS ClientError: {e}")
        return {
            'statusCode': 500,
            'body': f'Error processing messages: AWS ClientError. {str(e)}'
        }
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': f'Error processing messages: Unexpected error. {str(e)}'
        }
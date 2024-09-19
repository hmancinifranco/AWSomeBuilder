import json
import boto3
import os
from botocore.exceptions import ClientError
from datetime import datetime

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

def process_message(message_body, table):
    # Get information from the message
    bucket = message_body['bucket']
    key = message_body['key']

    # Extract filename and prefix from the key
    filename = os.path.basename(key)
    prefix = os.path.dirname(key)

    # Start Comprehend Classification job
    job_name = f'document-classification-{datetime.now().strftime("%Y%m%d-%H%M%S")}'
    print(f'Starting Comprehend Classification job {job_name} with model {CLASSIFIER_ARN}')

    # Build an S3 path for the file to be processed
    s3_uri = os.path.join('s3://', bucket, key)

    comprehend.start_document_classification_job(
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

    # Write to DynamoDB table with put_item
    table.put_item(
        Item={
            'bucket': bucket,
            'filename': filename,
            'prefix': prefix,
            'key': key
        }
    )

    print(f"Processed message: {message_body}")

def lambda_handler(event, context):
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)

    try:
        if 'Records' in event:  # This is an SQS trigger event
            for record in event['Records']:
                message_body = json.loads(record['body'])
                process_message(message_body, table)
                
                # Delete the processed message from the queue
                sqs_client.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=record['receiptHandle']
                )
            
            return {
                'statusCode': 200,
                'body': f'Processed {len(event["Records"])} messages successfully.'
            }
        else:  # This is a manual invocation or test event
            # Receive a single message from the queue
            message = sqs_client.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=5
            )

            if 'Messages' in message:
                message_body = json.loads(message['Messages'][0]['Body'])
                receipt_handle = message['Messages'][0]['ReceiptHandle']

                process_message(message_body, table)

                # Delete the processed message from the queue
                sqs_client.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=receipt_handle
                )

                return {
                    'statusCode': 200,
                    'body': f'Document processed successfully. Message: {message_body}'
                }
            else:
                print("No messages in the queue")
                return {
                    'statusCode': 200,
                    'body': 'No messages in the queue.'
                }

    except ClientError as e:
        print(f"AWS ClientError: {e}")
        return {
            'statusCode': 500,
            'body': f'Error processing messages: AWS ClientError. {str(e)}'
        }
    except Exception as e:
        print(f"Unexpected error: {e}")
        return {
            'statusCode': 500,
            'body': f'Error processing messages: Unexpected error. {str(e)}'
        }

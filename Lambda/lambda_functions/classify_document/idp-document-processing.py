import json
import boto3
import uuid
import os
from collections import deque

# Initialize AWS clients
sqs_client = boto3.client('sqs')
s3_client = boto3.client('s3')
comprehend = boto3.client('comprehend')
sns_client = boto3.client('sns')

# Get environment variables
QUEUE_URL = os.environ['QUEUE_URL']
CLASSIFIER_ARN = os.environ['CLASSIFIER_ARN']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
DATA_ACCESS_ROLE_ARN = os.environ['DATA_ACCESS_ROLE_ARN']
OUTPUT_BUCKET = os.environ['OUTPUT_BUCKET']

def lambda_handler(event, context):
    document_paths = deque()

    # Receive messages in batches of 10
    while True:
        messages = sqs_client.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=10
        )

        if 'Messages' in messages:
            for message in messages['Messages']:
                message_body = json.loads(message['Body'])

                # Get information from the message
                bucket = message_body['bucket']
                key = message_body['key']
                document_paths.append(f's3://{bucket}/{key}')

                # Delete message from the original queue
                sqs_client.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=message['ReceiptHandle']
                )
        else:
            break

    try:
        # Start Comprehend Classification job
        job_name = f'doc-classification-job-{uuid.uuid4()}'
        print(f'Starting Comprehend Classification job {job_name} with model {CLASSIFIER_ARN}')

        comprehend.start_document_classification_job(
            JobName=job_name,
            DocumentClassifierArn=CLASSIFIER_ARN,
            InputDataConfig={
                'S3Uri': ','.join(document_paths),
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

        return {
            'statusCode': 200,
            'body': 'Documents processed successfully'
        }

    except Exception as e:
        print(f"Error: {e}")
        print("Job creation failed. Messages written to SNS.")
        # Format and publish SNS messages
        for document_path in document_paths:
            sns_message = {
                'bucket': document_path.split('/')[2],
                'key': '/'.join(document_path.split('/')[3:])
            }
            sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=json.dumps(sns_message)
            )

    return {
        'statusCode': 200,
        'body': 'No messages in the queue'
    }

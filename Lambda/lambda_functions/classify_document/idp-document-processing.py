import json
import boto3
import uuid
import os
import base64

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

def get_document_text(bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    document_content = response['Body'].read()
    return base64.b64encode(document_content).decode('utf-8')

def format_sns_message(classification_result, bucket, key):
    return {
        'bucket': bucket,
        'input_key': key,
        'job_name': classification_result['JobName'],
        'score': classification_result['Labels'][0]['Score']
    }

def lambda_handler(event, context):
    # Get messages from SQS queue
    messages = sqs_client.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=10  # Adjust this value as per your requirements
    )

    if 'Messages' in messages:
        for message in messages['Messages']:
            message_body = json.loads(message['Body'])

            # Get information from the message
            bucket = message_body['bucket']
            key = message_body['key']

            try:
                # Start Comprehend Classification job
                job_name = f'doc-classification-job-{uuid.uuid4()}'
                print(f'Starting Comprehend Classification job {job_name} with model {CLASSIFIER_ARN}')

                response = comprehend.start_document_classification_job(
                    JobName=job_name,
                    DocumentClassifierArn=CLASSIFIER_ARN,
                    InputDataConfig={
                        'S3Uri': f's3://{bucket}/{key}',
                        'InputFormat': 'ONE_DOC_PER_FILE',
                        'DocumentReaderConfig': {
                            'DocumentReadAction': 'TEXTRACT_DETECT_DOCUMENT_TEXT',
                            'DocumentReadMode': 'FORCE_DOCUMENT_READ_ACTION'
                        }
                    },
                    OutputDataConfig={
                        'S3Uri': f's3://{bucket}/comprehend/doc-class-output/'
                    },
                    DataAccessRoleArn=DATA_ACCESS_ROLE_ARN
                )

                # Format and publish SNS message
                sns_message = format_sns_message(response, bucket, key)
                sns_client.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Message=json.dumps(sns_message)
                )

                # Delete message from the original queue
                sqs_client.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=message['ReceiptHandle']
                )

            except Exception as e:
                print(f"Error: {e}")

        return {
            'statusCode': 200,
            'body': 'Documents processed successfully'
        }

    else:
        return {
            'statusCode': 200,
            'body': 'No messages in the queue'
        }

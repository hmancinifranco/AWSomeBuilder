import json
import boto3
import os

# Initialize AWS clients
sqs_client = boto3.client('sqs')
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Get the SQS queue URL from the environment variable
    queue_url = os.environ['QUEUE_URL']

    # Process each record in the event
    for record in event['Records']:
        # Get S3 bucket and object key information
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        # Ignore folder creation events (keys ending with '/')
        if key.endswith('/'):
            print(f"Ignoring folder creation event: {key}")
            continue

        # Create message body for SQS
        message_body = {
            'bucket': bucket,
            'key': key
        }

        # Send message to SQS queue
        try:
            response = sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(message_body)
            )
            print(f"Message sent to queue for file: {key}. MessageId: {response['MessageId']}")
        except Exception as e:
            print(f"Error sending message to queue for file: {key}. Error: {str(e)}")

    return {
        'statusCode': 200,
        'body': 'Processing complete'
    }

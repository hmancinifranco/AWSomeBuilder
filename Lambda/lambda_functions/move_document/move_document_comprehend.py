import json
import boto3
import os

# Initialize AWS clients
sqs_client = boto3.client('sqs')
s3_client = boto3.client('s3')

# Get environment variables
QUEUE_URL = os.environ['QUEUE_URL']
DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']

def lambda_handler(event, context):
    # Get message from SQS queue
    message = sqs_client.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=1
    )

    if 'Messages' in message:
        message_body = json.loads(message['Messages'][0]['Body'])

        # Get information from the message
        source_bucket = message_body['bucket']
        source_key = message_body['key']
        classification = message_body['classification']

        try:
            # Copy the object to the destination bucket with the classification as the prefix
            destination_key = f"accounting_docs/{source_key}/{classification}"
            copy_source = {
                'Bucket': source_bucket,
                'Key': source_key
            }
            s3_client.copy_object(
                CopySource=copy_source,
                Bucket=DESTINATION_BUCKET,
                Key=destination_key
            )

            print(f"Successfully copied {source_key} from {source_bucket} to {DESTINATION_BUCKET}/{destination_key}")

            # Delete message from the original queue
            sqs_client.delete_message(
                QueueUrl=QUEUE_URL,
                ReceiptHandle=message['Messages'][0]['ReceiptHandle']
            )

            return {
                'statusCode': 200,
                'body': 'Document moved successfully'
            }

        except Exception as e:
            print(f"Error: {e}")
            return {
                'statusCode': 500,
                'body': 'Error moving document'
            }

    else:
        return {
            'statusCode': 200,
            'body': 'No messages in the queue'
        }

import json
import boto3
import os

# Initialize AWS clients
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

# Get environment variables
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']

def process_sns_message(message):
    message_body = json.loads(message['Message'])
    return message_body['bucket'], message_body['key'], message_body['classification']

def move_document(source_bucket, source_key, classification):
    destination_key = f"{classification.lower()}/{source_key}"
    s3_client.copy_object(
        CopySource={'Bucket': source_bucket, 'Key': source_key},
        Bucket=DESTINATION_BUCKET,
        Key=destination_key
    )
    return destination_key

def lambda_handler(event, context):
    try:
        # Process SNS message
        for record in event['Records']:
            if record['EventSource'] == 'aws:sns':
                source_bucket, source_key, classification = process_sns_message(record['Sns'])
                
                # Move document
                destination_key = move_document(source_bucket, source_key, classification)
                
                # Delete original document (optional)
                # s3_client.delete_object(Bucket=source_bucket, Key=source_key)
                
                # Publish success message to SNS (optional)
                success_message = {
                    'status': 'success',
                    'source_bucket': source_bucket,
                    'source_key': source_key,
                    'destination_bucket': DESTINATION_BUCKET,
                    'destination_key': destination_key,
                    'classification': classification
                }
                sns_client.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Message=json.dumps(success_message)
                )

        return {
            'statusCode': 200,
            'body': 'Documents processed successfully'
        }

    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': 'Error processing documents'
        }

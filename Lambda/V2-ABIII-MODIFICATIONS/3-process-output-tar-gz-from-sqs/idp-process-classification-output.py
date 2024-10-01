import json
import boto3
import tarfile
import io
import logging
import os

# Initialize AWS clients
sqs_client = boto3.client('sqs')
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

# Get environment variables
QUEUE_URL = os.environ['QUEUE_URL']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # Get the message from the SQS queue
    for record in event['Records']:
        message_body = json.loads(record['body'])
        
        # Log the complete message body
        logger.info(f"Received message: {message_body}")
        
        # Check if the message body has the expected format
        if 'Records' in message_body and message_body['Records']:
            s3_record = message_body['Records'][0]
            if 's3' in s3_record:
                bucket_name = s3_record['s3']['bucket']['name']
                object_key = s3_record['s3']['object']['key']

                # Check if the object key matches the pattern for the output.tar.gz file
                if object_key.endswith('output.tar.gz'):
                    try:
                        logger.info(f"Processing object: {object_key}")

                        # Download the output.tar.gz file
                        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
                        tar_file = tarfile.open(fileobj=io.BytesIO(response['Body'].read()), mode='r:gz')

                        # Process each file in the tar archive
                        for member in tar_file.getmembers():
                            if member.isfile() and member.name.endswith('.out'):
                                file_obj = tar_file.extractfile(member)
                                content = file_obj.read().decode('utf-8')
                                data = json.loads(content)

                                # Check if the classification confidence score is greater than 0.8
                                for classification in data['Classes']:
                                    if classification['Score'] > 0.8:
                                        # Construct the message
                                        message = f"File: {data['File']}, Classification: {classification['Name']}, Score: {classification['Score']}"

                                        # Send the message to the SNS topic
                                        sns_client.publish(
                                            TopicArn=SNS_TOPIC_ARN,
                                            Message=message
                                        )
                                        logger.info(f"Message sent to SNS: {message}")

                    except Exception as e:
                        logger.error(f"Error: {e}")

                else:
                    logger.info(f"Skipping object {object_key} as it is not the output.tar.gz file")
            else:
                logger.error("Invalid message format: 's3' key not found in message body")
        else:
            logger.error("Invalid message format: 'Records' key not found or empty in message body")

    return {
        'statusCode': 200,
        'body': 'Documents processed successfully'
    }
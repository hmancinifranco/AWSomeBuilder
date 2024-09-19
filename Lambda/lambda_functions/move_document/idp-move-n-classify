import json
import boto3
import os
from botocore.exceptions import ClientError
import logging

# Initialize AWS clients
sqs_client = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

# Get environment variables
QUEUE_URL = os.environ['QUEUE_URL']
SOURCE_TABLE_NAME = os.environ['SOURCE_TABLE_NAME']
DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']
SOURCE_BUCKET = os.environ['SOURCE_BUCKET']

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    for record in event['Records']:
        message_body = json.loads(record['body'])
        logger.info(f"Processing message: {message_body}")

        # Get information from the message
        file_name = message_body['file_name']
        classification = message_body['classification']
        score = message_body.get('score')  # Use .get() in case 'score' is not present

        try:
            # Get item from DynamoDB table
            table = dynamodb.Table(SOURCE_TABLE_NAME)
            response = table.get_item(
                Key={
                    'bucket': SOURCE_BUCKET,
                    'filename': file_name
                }
            )

            if 'Item' in response:
                item = response['Item']
                source_key = item['key']  # Assuming 'key' is the full S3 key
                source_prefix = item['prefix']

                # Remove 'landing/' from the beginning of the source_prefix
                destination_prefix = source_prefix[8:] if source_prefix.startswith('landing/') else source_prefix

                try:
                    # Check if the source file exists
                    s3_client.head_object(Bucket=SOURCE_BUCKET, Key=source_key)
                except ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        logger.info(f"File {source_key} does not exist in {SOURCE_BUCKET}")
                        raise

                # Construct the destination key without 'landing/' prefix
                destination_key = f"classified/{destination_prefix}/{classification}/{file_name}"

                # Copy the file
                s3_client.copy_object(
                    Bucket=DESTINATION_BUCKET,
                    Key=destination_key,
                    CopySource={
                        'Bucket': SOURCE_BUCKET,
                        'Key': source_key
                    }
                )
                logger.info(f"File {source_key} copied to {DESTINATION_BUCKET}/{destination_key} with classification {classification} and score {score}")

                # Delete the item from DynamoDB table
                table.delete_item(
                    Key={
                        'bucket': SOURCE_BUCKET,
                        'filename': file_name
                    }
                )
                logger.info(f"Item deleted from DynamoDB table for {file_name}")

                # Delete the message from the SQS queue
                sqs_client.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=record['receiptHandle']
                )
                logger.info(f"Message deleted from SQS queue for {file_name}")

            else:
                logger.warning(f"No item found in DynamoDB for file: {file_name}")

        except ClientError as e:
            logger.error(f"AWS Error: {e}")
            # Don't delete the SQS message if there was an error
            return {
                'statusCode': 500,
                'body': f'Error processing file: {str(e)}'
            }
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            # Don't delete the SQS message if there was an error
            return {
                'statusCode': 500,
                'body': f'Unexpected error: {str(e)}'
            }

    return {
        'statusCode': 200,
        'body': 'File processed successfully'
    }
import json
import boto3
import tarfile
import io
import os

# Initialize AWS clients
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

# Get environment variables
OUTPUT_BUCKET = os.environ['OUTPUT_BUCKET']
QUEUE_URL = os.environ['QUEUE_URL']

def lambda_handler(event, context):
    # Get the bucket name and key from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']

    # Check if the object key matches the pattern for the output.tar.gz file
    if object_key.endswith('output.tar.gz'):
        try:
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
                            message_body = {
                                'file_name': data['File'],
                                'classification': classification['Name'],
                                'score': classification['Score']
                            }

                            # Send the message to the SQS queue
                            sqs_client.send_message(
                                QueueUrl=QUEUE_URL,
                                MessageBody=json.dumps(message_body)
                            )
                            print(f"Message sent to SQS for {data['File']},{classification['Name'],{classification['Score']}}")

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

    else:
        print(f"Skipping object {object_key} as it is not the output.tar.gz file")
        return {
            'statusCode': 200,
            'body': 'Not the output.tar.gz file'
        }
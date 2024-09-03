import json
import boto3
import time
from datetime import datetime

# Initialize AWS clients
sqs_client = boto3.client('sqs')
comprehend = boto3.client('comprehend')

# Get environment variables
QUEUE_URL = os.environ['DESTINATION_QUEUE_URL']
DATA_ACCESS_ROLE_ARN = os.environ['DATA_ACCESS_ROLE_ARN']

def lambda_handler(event, context):
    # Get the message from the SNS topic
    sns_message = json.loads(event['Records'][0]['Sns']['Message'])

    # Extract the job name from the SNS message
    job_name = sns_message['job_name']

    # Wait for the job to complete
    max_time = time.time() + 3 * 60 * 60  # 3 hours
    while time.time() < max_time:
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        describe_job = comprehend.describe_document_classification_job(JobId=job_name)

        print(f"{current_time} : Custom document classifier Job: {status}")

        if status == "COMPLETED":
            # Get the necessary data from the job output
            bucket = sns_message['bucket']
            key = sns_message['input_key']
            classification = describe_job["DocumentClassificationJobProperties"]["Labels"][0]["Name"]
            score = describe_job["DocumentClassificationJobProperties"]["Labels"][0]["Score"]

            # Send the data to the SQS queue
            queue_message = {
                'bucket': bucket,
                'key': key,
                'classification': classification,
                'score': score
            }
            sqs_client.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=json.dumps(queue_message)
            )

            print(f"Message sent to the queue: {queue_message}")
            break
        elif status == "FAILED":
            print("Classification job failed")
            print(describe_job)
            break

        time.sleep(10)

    return {
        'statusCode': 200,
        'body': 'Processing complete'
    }

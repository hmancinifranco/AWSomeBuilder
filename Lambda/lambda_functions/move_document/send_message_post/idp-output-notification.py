import json
import boto3
import os

# Inicializar cliente de AWS
sqs_client = boto3.client('sqs')
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Obtener información del objeto cargado en S3
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Obtener la URL de la cola de SQS desde la variable de entorno
    queue_url = os.environ['QUEUE_URL']

    #descomprime el output.tar.gz, lee resultado y mueve, origen y destino del resultado.

    # Enviar mensaje a la cola de SQS
    message_body = {
        'bucket': bucket,
        'key': key
    }
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message_body)
    )

    return {
        'statusCode': 200,
        'body': f'Mensaje enviado a la cola final: {response["MessageId"]}'
    }


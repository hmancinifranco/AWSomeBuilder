import json
import boto3
import tarfile
import os
from boto3.dynamodb.conditions import Key

# Inicializar clientes de AWS
sqs_client = boto3.client('sqs')
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Obtener variables de entorno
RESULT_QUEUE_URL = os.environ['RESULT_QUEUE_URL']
TEMP_RESULT_TABLE_NAME = os.environ['TEMP_RESULT_TABLE_NAME']

def lambda_handler(event, context):
    # Obtener mensaje de la cola de SQS
    message = sqs_client.receive_message(
        QueueUrl=RESULT_QUEUE_URL,
        MaxNumberOfMessages=1
    )

    if 'Messages' in message:
        message_body = json.loads(message['Messages'][0]['Body'])

        # Obtener información del mensaje
        job_id = message_body['job_id']
        output_bucket = message_body['output_bucket']
        output_prefix = message_body['output_prefix']

        try:
            # Descargar el archivo de resultados
            output_key = f"{output_prefix}/{job_id}/output/output.tar.gz"
            output_file = s3_client.get_object(Bucket=output_bucket, Key=output_key)
            output_data = output_file['Body'].read()

            # Procesar el archivo de resultados
            temp_result_table = dynamodb.Table(TEMP_RESULT_TABLE_NAME)
            with tarfile.open(fileobj=output_data, mode="r:gz") as tar:
                result_file = tar.extractfile("output.json")
                result_data = json.load(result_file)

                # Escribir los resultados en la tabla temporal de DynamoDB
                for document in result_data:
                    source_bucket = document['InputDocumentInfo']['S3Bucket']
                    source_key = document['InputDocumentInfo']['S3ObjectKey']
                    document_type = document['Classes'][0]['Name']

                    temp_result_table.put_item(
                        Item={
                            'source_bucket': source_bucket,
                            'source_key': source_key,
                            'document_type': document_type
                        }
                    )

            # Eliminar mensaje de la cola
            sqs_client.delete_message(
                QueueUrl=RESULT_QUEUE_URL,
                ReceiptHandle=message['Messages'][0]['ReceiptHandle']
            )

            return {
                'statusCode': 200,
                'body': 'Resultados procesados correctamente'
            }

        except Exception as e:
            print(f"Error: {e}")
            return {
                'statusCode': 500,
                'body': 'Error al procesar los resultados'
            }

    else:
        return {
            'statusCode': 200,
            'body': 'No hay mensajes en la cola'
        }
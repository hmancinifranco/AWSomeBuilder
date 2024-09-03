import json
import boto3
import uuid
import time
from datetime import datetime

# Inicializar clientes de AWS
sqs_client = boto3.client('sqs')
s3_client = boto3.client('s3')
comprehend_client = boto3.client('comprehend')
textract_client = boto3.client('textract')
dynamodb = boto3.resource('dynamodb')

# Definir variables de entorno
QUEUE_URL = 'https://sqs.us-east-2.amazonaws.com/533267341537/idp-document-processing-queue'
DESTINATION_BUCKET = 'idp-processed-documents'
CLASSIFIER_ARN = 'arn:aws:comprehend:us-east-2:533267341537:document-classifier/Sample-Doc-Classifier-IDP/version/Sample-Doc-Classifier-IDP-v1'
ADAPTER_ID = '04e53b5c0c33'
DATA_ACCESS_ROLE_ARN = 'arn:aws:iam::533267341537:role/idp-document-processing-role'
JOBS_TABLE = dynamodb.Table('idp-classification-jobs')

def lambda_handler(event, context):
    # Procesar mensajes de la cola
    for record in event['Records']:
        message_body = json.loads(record['body'])
        bucket = message_body['bucket']
        key = message_body['key']

        try:
            # Verificar si ya existe un trabajo para este archivo
            job_record = JOBS_TABLE.get_item(Key={'object_key': key})

            if 'Item' not in job_record or job_record['Item']['status'] != 'IN_PROGRESS':
                # Descargar el documento del bucket de origen
                document = s3_client.get_object(Bucket=bucket, Key=key)
                document_bytes = document['Body'].read()

                # Iniciar un trabajo de clasificación de documentos en Comprehend
                job_name = f'idp-classification-job-{uuid.uuid4()}'
                response = comprehend_client.start_document_classification_job(
                    JobName=job_name,
                    DocumentClassifierArn=CLASSIFIER_ARN,
                    DataAccessRoleArn=DATA_ACCESS_ROLE_ARN,
                    InputDataConfig={
                        'S3Uri': f's3://{bucket}/{key}',
                        'InputFormat': 'ONE_DOC_PER_FILE',
                        'DocumentReaderConfig': {
                            'DocumentReadAction': 'TEXTRACT_DETECT_DOCUMENT_TEXT',
                            'DocumentReadMode': 'FORCE_DOCUMENT_READ_ACTION'
                        }
                    },
                    OutputDataConfig={
                        'S3Uri': f's3://{DESTINATION_BUCKET}/output/{job_name}'
                    }
                )

                # Crear o actualizar el registro en la tabla de DynamoDB
                JOBS_TABLE.put_item(
                    Item={
                        'object_key': key,
                        'job_id': response['JobId'],
                        'status': 'IN_PROGRESS'
                    }
                )

                # Esperar a que se complete el trabajo de clasificación
                max_time = time.time() + 3 * 60 * 60  # 3 horas
                while time.time() < max_time:
                    now = datetime.now()
                    current_time = now.strftime("%H:%M:%S")
                    describe_job = comprehend_client.describe_document_classification_job(
                        JobId=response['JobId']
                    )
                    status = describe_job["DocumentClassificationJobProperties"]["JobStatus"]

                    print(f"{current_time} : Custom document classifier Job: {status}")

                    if status == "COMPLETED" or status == "FAILED":
                        if status == "COMPLETED":
                            print(f'Output generated - {describe_job["DocumentClassificationJobProperties"]["OutputDataConfig"]["S3Uri"]}')
                            # Resto del código para procesar los resultados
                        else:
                            print("Classification job failed")
                            print(describe_job)
                        break

                    time.sleep(10)

                # Extraer año y empresa con Textract
                entities_response = textract_client.analyze_document(
                    Document={'Bytes': document_bytes},
                    FeatureTypes=['QUERIES'],
                    QueriesConfig={'Queries': [ADAPTER_ID]}
                )

                # Obtener la respuesta de cada consulta
                company_query_response = entities_response['Queries'][0]
                year_query_response = entities_response['Queries'][1]

                # Extraer el nombre de la empresa y el año del documento
                company_name = next((entity['Text'] for entity in company_query_response['Entities']), None)
                document_year = next((entity['Text'] for entity in year_query_response['Entities']), None)

                # Crear la estructura de prefijos
                prefix = f"{company_name}/{document_year}/"

                # Mover el documento al bucket de destino con la estructura de prefijos
                destination_key = f"{prefix}input/{key}"
                s3_client.copy_object(
                    CopySource={'Bucket': bucket, 'Key': key},
                    Bucket=DESTINATION_BUCKET,
                    Key=destination_key
                )

            else:
                print(f"Ya existe un trabajo en curso para el objeto {key}")

            # Eliminar el mensaje de la cola
            sqs_client.delete_message(
                QueueUrl=QUEUE_URL,
                ReceiptHandle=record['receiptHandle']
            )

        except s3_client.exceptions.NoSuchKey:
            print(f"Error: El objeto {key} no existe en el bucket {bucket}")

    return {
        'statusCode': 200,
        'body': 'Documentos procesados correctamente'
    }
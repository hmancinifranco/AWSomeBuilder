import json
import boto3
import os
import re
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
import logging

# Configurar el logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = os.getenv('BUCKET_NAME')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('idp-document-catalog')

    # Validación de entrada
    object_name = event['queryStringParameters'].get('object_name')
    if not object_name:
        logger.error('Missing object_name parameter')
        return {
            'statusCode': 400,
            'body': json.dumps('Missing object_name parameter')
        }

    # Expresión regular para validar el nombre del objeto
    pattern = r'^[a-zA-Z0-9_\-\.]+$'
    if not re.match(pattern, object_name):
        logger.error('Invalid object_name parameter')
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid object_name parameter')
        }

    # Obtener los valores de customer, year_month del evento
    customer = event['queryStringParameters'].get('customer')
    year_month = event['queryStringParameters'].get('year_month')
    filename = object_name

    # Consulta en el índice secundario global
    response = table.query(
        IndexName='customer-year_month-index',
        KeyConditionExpression=Key('customer').eq(customer) & Key('year_month').eq(year_month),
        FilterExpression=Attr('filename').eq(filename)
    )
    logger.info(f'Respuesta de la consulta a DynamoDB: {response}')

    # Verifica si hay resultados
    if response['Items']:
        logger.info('El documento ya existe')
        return {
            'statusCode': 400,
            'body': json.dumps('El documento ya existe')
        }

    try:
        key = f"landing/{customer}/{year_month}/{object_name}"
        presigned_url = s3.generate_presigned_url('put_object',
                                                  Params={'Bucket': bucket_name, 'Key': key},
                                                  ExpiresIn=3600,
                                                  HttpMethod='PUT')
        logger.info(f'Presigned URL generado correctamente: {presigned_url}')
    except Exception as e:
        # Manejo de errores con más detalles
        error_message = f'Error generating presigned URL: {e.__class__.__name__}: {str(e)}'
        logger.error(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps(error_message)
        }

    return {
        'statusCode': 200,
        'body': json.dumps({'url': presigned_url})
    }
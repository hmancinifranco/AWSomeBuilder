import json
import boto3
import os
import re
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = os.getenv('BUCKET_NAME')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('idp-document-catalog')

    # Validación de entrada
    object_name = event['queryStringParameters'].get('object_name')
    if not object_name:
        return {
            'statusCode': 400,
            'body': json.dumps('Missing object_name parameter')
        }

    # Expresión regular para validar el nombre del objeto
    pattern = r'^[a-zA-Z0-9_\-\.]+$'
    if not re.match(pattern, object_name):
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid object_name parameter')
        }

    # Obtener los valores de customer, year_month y date_time del evento
    customer = event['queryStringParameters'].get('customer')
    year_month = event['queryStringParameters'].get('year_month')
    date_time = event['queryStringParameters'].get('date_time')
    filename = object_name

    # Consulta en el índice secundario global
    response = table.query(
        IndexName='customer-year_month-index',
        KeyConditionExpression=Key('customer').eq(customer) & Key('year_month').eq(year_month),
        FilterExpression=Attr('filename').eq(filename)
    )

    # Verifica si hay resultados
    if response['Items']:
        return {
            'statusCode': 400,
            'body': json.dumps('El documento ya existe')
        }

    try:
        key = f"landing/var-{date_time}/{customer}/{year_month}/{object_name}"
        presigned_url = s3.generate_presigned_url('put_object',
                                                  Params={'Bucket': bucket_name, 'Key': key},
                                                  ExpiresIn=3600,
                                                  HttpMethod='PUT')
    except Exception as e:
        # Manejo de errores con más detalles
        error_message = f'Error generating presigned URL: {e.__class__.__name__}: {str(e)}'
        return {
            'statusCode': 500,
            'body': json.dumps(error_message)
        }

    return {
        'statusCode': 200,
        'body': json.dumps({'url': presigned_url})
    }
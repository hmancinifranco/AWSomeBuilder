import json
import boto3
import os
import re

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = os.getenv('BUCKET_NAME')
    
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
    
    try:
        presigned_url = s3.generate_presigned_url('put_object', 
                                                  Params={'Bucket': bucket_name, 'Key': object_name}, 
                                                  ExpiresIn=300,
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
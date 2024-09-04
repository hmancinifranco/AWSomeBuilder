import json
import boto3
import os

# Inicializar clientes de AWS
sqs_client = boto3.client('sqs')
textract_client = boto3.client('textract')

# Definir variables de entorno
INPUT_QUEUE_URL = os.getenv('INPUT_QUEUE_URL')
OUTPUT_QUEUE_URL = os.getenv('OUTPUT_QUEUE_URL')
ADAPTER_ID = '04e53b5c0c33'
ADAPTER_VERSION = '2'

def lambda_handler(event, context):
    # Obtener mensajes de la cola de entrada
    for record in event['Records']:
        message_body = json.loads(record['body'])
        bucket = message_body['bucket']
        key = message_body['key']

        # Realizar análisis síncrono con Textract
        response = textract_client.analyze_document(
{
   'AdaptersConfig': {
        'Adapters': [
        {
            'AdapterId': ADAPTER_ID,
            'Pages': ['*'],
            'Version': ADAPTER_VERSION
        }
        ]
    },

   "Document": { 
      "S3Object": { 
         "Bucket": bucket,
         "Name": key
      }
   },

   "FeatureTypes": [ "QUERIES" ],

   "QueriesConfig": { 
      "Queries": [ 
         { 
            "Alias": "company",
            "Pages": [ "*" ],
            "Text": "What is the name of the company?"
         },

         { 
            "Alias": "year",
            "Pages": [ "*" ],
            "Text": "From what year is this document?"
         },
      ]
   }
}
        )

        # Obtener resultados de las consultas
        company_name = None
        document_year = None
        for block in response['Blocks']:
            if block["BlockType"] == "QUERY_RESULT":
                if block["Query"]["Alias"] == "company":
                    company_name = block["Text"]
                elif block["Query"]["Alias"] == "year":
                    document_year = block["Text"]

        # Enviar mensaje a la cola de salida
        output_message_body = {
            'bucket': bucket,
            'key': key,
            'company': company_name,
            'year': document_year
        }
        sqs_client.send_message(
            QueueUrl=OUTPUT_QUEUE_URL,
            MessageBody=json.dumps(output_message_body)
        )

    return {
        'statusCode': 200,
        'body': 'Procesamiento completado'
    }
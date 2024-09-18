import json
import boto3
from urllib.parse import unquote_plus
import fitz
import os
import math

def lambda_handler(event, context):
    
    print('event parameter: {}'.format(event))

    record = event['Records'][0]
    s3bucket = record['s3']['bucket']['name']
    s3key = unquote_plus(str(record['s3']['object']['key']))
    filepath = os.path.split(s3key)[0]
    filename = os.path.basename(s3key)
    
    print(filename)
    
    s3 = boto3.resource("s3")
    obj = s3.Object(s3bucket, s3key)
    fs = obj.get()["Body"].read()

    document = fitz.open(stream=fs, filetype="pdf")
    newdocument = fitz.open()

    total_pages=0
    for idx, page in enumerate(document):
        total_pages = total_pages+1
    print(f'Total Pages : {total_pages}')
    
    # Determine the number of batches (each containing 50 images)
    batch_size = 20
    num_batches = math.ceil(total_pages / batch_size)
    
    # Trigger the main Lambda function for each batch
    lambda_client = boto3.client('lambda')
    for i in range(num_batches):
        start_page = i * batch_size + 1
        end_page = min((i + 1) * batch_size, total_pages)
        
        payload = {
            'start_page': start_page,
            'end_page': end_page,
            'bucket_name': s3bucket,
            'object_key': s3key,
            'batch_no' : int(i+1),
            'total_batch':num_batches
        }
        
        lambda_client.invoke(
            FunctionName='pdf-rotater-container',
            InvocationType='Event',
            Payload=json.dumps(payload)
        )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Batch processing triggered successfully!')
    }

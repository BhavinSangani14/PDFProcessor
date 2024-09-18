import json
import boto3
import time
from io import BytesIO
import tempfile
import io 
from PyPDF2 import PdfMerger

def lambda_handler(event, context):
    # TODO implement
    key = event['key']
    bucket = event['bucket']
    total_batch = event['total_batch']
    filename = event['filename']
    s3_client = boto3.client('s3')
    
    wait = True
    while wait:
        # List objects in the specified bucket and prefix
        response = s3_client.list_objects(
            Bucket=bucket,
            Prefix=key
        )
        if len(response['Contents'])-1 != total_batch:
            time.sleep(15)
        else:
            wait = False
            
    # newdocument = fitz.open()
    merger = PdfMerger()
    for i in range(total_batch):
        # Get the key (file name) of the object
        file_key = key + f'{i+1}.pdf'
        print(file_key)
        response = s3_client.get_object(Bucket=bucket, Key=file_key)
        pdf_bytes = response['Body'].read()
        pdf_stream = BytesIO(pdf_bytes)
        merger.append(pdf_stream)
        
    # Merge PDFs
    output_stream = BytesIO()
    merger.write(output_stream)
    output_stream.seek(0)
    # Upload merged PDF to S3
    output_key = f'corrected_rotation/{filename}.pdf'
    s3_client.put_object(Bucket=bucket, Key=output_key, Body=output_stream)
    return {
        'statusCode': 200,
        'body': json.dumps('pdf process completed!')
    }

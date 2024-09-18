import json
import os
import boto3
import sys
from PIL import Image
import logging
import pytesseract
from io import BytesIO
import fitz
import re
from urllib.parse import unquote_plus
import cv2
import numpy as np
import random

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def detect_text_orientation(image):
    # Convert image to grayscale
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # Apply edge detection
    edges = cv2.Canny(gray, 50, 150, apertureSize=3)

    # Detect lines using Hough Transform
    lines = cv2.HoughLines(edges, 1, np.pi / 180, 150)

    # Initialize variables to store angles
    angles = []

    # Calculate angles of lines
    if lines is not None:
        for rho, theta in lines[:, 0]:
            angle = np.rad2deg(theta)
            angles.append(angle)

    # Determine the dominant orientation angle
    if angles:
        orientation = np.median(angles)
    else:
        orientation = 0  # Default orientation

    return orientation

def lambda_handler(event, context):
    
    logger.info('event parameter: {}'.format(event))

    dataPath ="/opt/tesseract/share/tessdata"
    os.environ["TESSDATA_PREFIX"] = dataPath

    start_page = event['start_page']
    end_page = event['end_page']
    batch_no = event['batch_no']
    s3bucket = event['bucket_name']
    s3key = event['object_key']
    total_batch = event['total_batch']

    filepath = os.path.split(s3key)[0]
    filename = os.path.basename(s3key)
    
    print(filename)
    
    s3 = boto3.resource("s3")
    obj = s3.Object(s3bucket, s3key)
    fs = obj.get()["Body"].read()

    document = fitz.open(stream=fs, filetype="pdf")
    newdocument = fitz.open()

    count=0
    for idx, page in enumerate(document):
        count = count+1
        if count in range(start_page, end_page +1):
            print("Doc page: " + str(count))

            # Get page as image
            imagen = page.get_pixmap(alpha=False,dpi=600)
            
            try:
                # Get text using Tesseract
                texto = pytesseract.image_to_osd( Image.open(BytesIO(imagen.tobytes(output='jpg'))))

                # Get angle
                patron = r'(\w+(?: \w+)*):\s*([\w.]+)'
                pares = re.findall(patron, texto)
                diccionario = {clave: valor for clave, valor in pares}

                # print(diccionario)
                # print("Page.rotation: " + str(page.rotation))
                # print(page.rect.width > page.rect.height)

                angulo_rotacion= int(diccionario['Rotate'])
                angulo_orientation = int(diccionario['Orientation in degrees'])
                
                print(f'Orientation_p : {angulo_orientation}')
                print(f'Page rotation_p : {page.rotation}')

                # Rotate page if needed
                if(angulo_rotacion != 0):
                    rot = (page.rotation + angulo_rotacion)%360
                    print(rot)
                    page.set_rotation(rot)
            except:
                image = Image.frombytes("RGB", [imagen.width, imagen.height], imagen.samples)

                # Detect text orientation using image processing
                orientation = detect_text_orientation(np.array(image))
                
                print(f'Orientation_i : {orientation}')
                print(f'Page rotation_i : {page.rotation}')
                # Rotate page if needed
                if orientation != 0:
                    random_number = random.random()
                    r_t = float((orientation + page.rotation) % 360)
                    print(r_t)
                    page.set_rotation(r_t + random_number)
            
            mat = fitz.Matrix(3, 3)
            pix = page.get_pixmap(alpha=False, matrix=mat)

            bio_in = BytesIO(pix.tobytes()) # just to create a file-like object
            pil = Image.open(bio_in)  # to be used for Pillow 
            bio_out = BytesIO() # the output file-like
            pil.save(bio_out, format="jpeg") # write to it in a space-saving format
            imgdoc = fitz.open("jpeg", bio_out.getvalue()) # open it as a PyMuPDF document
            
            newdocument.insert_file(imgdoc)

    # Save file with corrected rotation
    document.close()

    new_bytes = newdocument.write()
    file_name = filename.split('.')[0]
    filename_output = f'corrected_rotation/batch/{file_name}_batch/{batch_no}.pdf'
    s3.Bucket(s3bucket).put_object(Key=filename_output, Body=new_bytes)
    newdocument.close() 

    lambda_client = boto3.client('lambda')
    if total_batch == batch_no:
        payload = {'key' : f'corrected_rotation/batch/{file_name}_batch/',
                   'bucket' : s3bucket,
                   'total_batch' : total_batch,
                   'filename' : file_name}
        response = lambda_client.invoke(
            FunctionName='CreateFinalDocument',
            InvocationType='Event',  # Synchronous invocation
            Payload=json.dumps(payload)  # Payload can be any JSON serializable object
        )
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Done!')
    }

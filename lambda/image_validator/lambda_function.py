import json
import os
import boto3

s3 = boto3.client('s3')

VALID_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif']

def is_valid_image(key):
    _, ext = os.path.splitext(key.lower())
    return ext in VALID_EXTENSIONS

def lambda_handler(event, context):
    print("=== image validator invoked ===")

    for record in event['Records']:
        sns_message = record['Sns']['Message']
        s3_event = json.loads(sns_message)

        for s3_record in s3_event['Records']:
            bucket = s3_record['s3']['bucket']['name']
            key = s3_record['s3']['object']['key']

            if is_valid_image(key):
                print(f"[VALID] {key} is a valid image file")

                filename = key.split('/')[-1]
                s3.copy_object(
                    Bucket=bucket,
                    Key=f"processed/valid/{filename}",
                    CopySource={'Bucket': bucket, 'Key': key}
                )
            else:
                print(f"[INVALID] {key} is not a valid image type")
                raise ValueError(f"Invalid file type: {key}")

    return {'statusCode': 200, 'body': 'validation complete'}

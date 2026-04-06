import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

VALID_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".tiff", ".svg"}


def handler(event, context):
    """
    Triggered via SNS which wraps an S3 event notification.
    Validates the file extension. Valid images are copied to processed/valid/.
    Invalid files raise an exception so the invocation fails and hits the DLQ.
    """
    for record in event["Records"]:
        # Parse nested SNS -> S3 event
        sns_message = json.loads(record["Sns"]["Message"])

        for s3_record in sns_message["Records"]:
            bucket = s3_record["s3"]["bucket"]["name"]
            key = s3_record["s3"]["object"]["key"]
            filename = key.split("/")[-1]

            # Check file extension
            ext = ""
            if "." in filename:
                ext = "." + filename.rsplit(".", 1)[1].lower()

            if ext in VALID_EXTENSIONS:
                logger.info(f"[VALID] {key} is a valid image file")

                copy_source = {"Bucket": bucket, "Key": key}
                dest_key = f"processed/valid/{filename}"

                s3_client.copy_object(
                    Bucket=bucket,
                    Key=dest_key,
                    CopySource=copy_source,
                )

                logger.info(f"[VALID] Copied to {dest_key}")
            else:
                logger.error(f"[INVALID] {key} is not a valid image type")
                raise ValueError(f"Invalid file type: {key}")

    return {"statusCode": 200, "body": "Validation complete"}

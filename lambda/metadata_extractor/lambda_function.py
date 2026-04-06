import json
import boto3
import logging
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")


def handler(event, context):
    """
    Triggered via SNS which wraps an S3 event notification.
    Extracts file metadata, logs it, and writes a JSON file to processed/metadata/.
    """
    for record in event["Records"]:
        # Parse nested SNS -> S3 event
        sns_message = json.loads(record["Sns"]["Message"])

        for s3_record in sns_message["Records"]:
            bucket = s3_record["s3"]["bucket"]["name"]
            key = s3_record["s3"]["object"]["key"]
            size = s3_record["s3"]["object"].get("size", 0)
            event_time = s3_record["eventTime"]

            # Log in required [METADATA] format
            logger.info(f"[METADATA] File: {key}")
            logger.info(f"[METADATA] Bucket: {bucket}")
            logger.info(f"[METADATA] Size: {size} bytes")
            logger.info(f"[METADATA] Upload Time: {event_time}")

            # Build metadata JSON
            metadata = {
                "file": key,
                "bucket": bucket,
                "size": size,
                "upload_time": event_time,
            }

            # Write JSON to processed/metadata/{filename}.json
            filename = key.split("/")[-1]
            base_name = filename.rsplit(".", 1)[0]
            output_key = f"processed/metadata/{base_name}.json"

            s3_client.put_object(
                Bucket=bucket,
                Key=output_key,
                Body=json.dumps(metadata),
                ContentType="application/json",
            )

            logger.info(f"[METADATA] Wrote metadata to {output_key}")

    return {"statusCode": 200, "body": "Metadata extraction complete"}

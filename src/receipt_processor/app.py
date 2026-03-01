import json
import os
import uuid
from datetime import datetime

import boto3

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])

def handler(event, context):
    # Handles both S3 triggers and API Gateway requests
    print("EVENT:", json.dumps(event))

    # S3 event
    if "Records" in event and event["Records"] and event["Records"][0].get("eventSource") == "aws:s3":
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        receipt_id = str(uuid.uuid4())

        table.put_item(
            Item={
                "receipt_id": receipt_id,
                "source": "s3",
                "bucket_name": bucket,
                "file_key": key,
                "upload_time": datetime.utcnow().isoformat(),
                "status": "UPLOADED",
            }
        )

        return {"statusCode": 200, "body": "S3 processed"}

    # API Gateway event (HTTP API / Lambda proxy)
    body = event.get("body") or "{}"
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        parsed = {"raw": body}

    receipt_id = str(uuid.uuid4())

    table.put_item(
        Item={
            "receipt_id": receipt_id,
            "source": "api",
            "data": parsed,
            "created_at": datetime.utcnow().isoformat(),
            "status": "CREATED",
        }
    )

    return {
        "statusCode": 200,
        "headers": {"content-type": "application/json"},
        "body": json.dumps({"receipt_id": receipt_id}),
    }

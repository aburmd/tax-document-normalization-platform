import csv
import io
import json
import logging
import os
import boto3

logger = logging.getLogger(__name__)
s3 = boto3.client("s3")


def download_file(bucket: str, key: str) -> str:
    local_path = f"/tmp/{os.path.basename(key)}"
    logger.info("Downloading s3://%s/%s → %s", bucket, key, local_path)
    s3.download_file(bucket, key, local_path)
    return local_path


def upload_json(bucket: str, key: str, data: dict):
    logger.info("Uploading JSON → s3://%s/%s", bucket, key)
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(data, indent=2, default=str), ContentType="application/json")


def upload_csv(bucket: str, key: str, canonical: dict):
    rows = canonical.get("transactions", [])
    if not rows:
        logger.info("No transaction rows for CSV: %s", key)
        return
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    logger.info("Uploading CSV (%d rows) → s3://%s/%s", len(rows), bucket, key)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue(), ContentType="text/csv")

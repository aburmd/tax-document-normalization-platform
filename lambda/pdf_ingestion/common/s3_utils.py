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
    """Write all list sections of canonical output as separate CSV files."""
    for section_name, rows in canonical.items():
        if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
            continue
        sanitized = [_sanitize_row(r) for r in rows]
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=sanitized[0].keys())
        writer.writeheader()
        writer.writerows(sanitized)
        section_key = key.replace(".csv", f"_{section_name}.csv")
        logger.info("Uploading CSV (%d rows) → s3://%s/%s", len(sanitized), bucket, section_key)
        s3.put_object(Bucket=bucket, Key=section_key, Body=buf.getvalue(), ContentType="text/csv")


def _sanitize_row(row: dict) -> dict:
    """Remove commas from all string values to ensure clean CSV output."""
    return {
        k: str(v).replace(",", "") if isinstance(v, str) else v
        for k, v in row.items()
    }

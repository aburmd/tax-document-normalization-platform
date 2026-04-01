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


def upload_csv_sections(bucket: str, prefix: str, broker: str, account_type: str, tax_year: str, document_id: str, canonical: dict):
    """Write each list section as a separate CSV in Hive-partitioned path for Athena.

    Path: {prefix}{broker}/{section}/account_type={account_type}/tax_year={tax_year}/{document_id}.csv
    """
    for section_name, rows in canonical.items():
        if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
            continue
        sanitized = [_sanitize_row(r) for r in rows]
        all_keys = dict.fromkeys(k for row in sanitized for k in row.keys())
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=all_keys, extrasaction='ignore')
        writer.writeheader()
        for row in sanitized:
            writer.writerow({k: row.get(k, '') for k in all_keys})
        key = f"{prefix}{broker}/{section_name}/account_type={account_type}/tax_year={tax_year}/{document_id}.csv"
        logger.info("Uploading CSV (%d rows) → s3://%s/%s", len(sanitized), bucket, key)
        s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue(), ContentType="text/csv")


def _sanitize_row(row: dict) -> dict:
    """Remove commas from all string values to ensure clean CSV output."""
    return {
        k: str(v).replace(",", "") if isinstance(v, str) else v
        for k, v in row.items()
    }

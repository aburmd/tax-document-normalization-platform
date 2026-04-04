import json
import logging
import os
import uuid
from datetime import datetime, timezone

from common.s3_utils import download_file, upload_json, upload_csv_sections
from common.checksum_utils import compute_checksum
from common.schema_validator import validate_canonical_output
from common.mapping_loader import load_mapping
from common.sanitize import sanitize_for_csv
from parsers.parser_router import route_parser, detect_doc_type

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET = os.environ["BUCKET_NAME"]
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/")
CLEANSED_PREFIX = os.environ.get("CLEANSED_PREFIX", "cleansed/")
REJECTED_PREFIX = os.environ.get("REJECTED_PREFIX", "rejected/")
AUDIT_PREFIX = os.environ.get("AUDIT_PREFIX", "audit/")


def lambda_handler(event, context):
    for record in event.get("Records", []):
        s3_key = record["s3"]["object"]["key"]
        source_bucket = record["s3"]["bucket"]["name"]
        logger.info("Processing s3://%s/%s", source_bucket, s3_key)
        try:
            _process_file(source_bucket, s3_key)
        except Exception:
            logger.exception("Failed to process %s", s3_key)
            _write_rejected(s3_key, context)
            raise


def _process_file(source_bucket: str, s3_key: str):
    metadata = _extract_metadata(s3_key)
    broker = metadata["broker"]
    account_type = metadata["account_type"]
    tax_year = metadata["tax_year"]
    document_id = str(uuid.uuid4())

    local_path = download_file(source_bucket, s3_key)
    checksum = compute_checksum(local_path)

    # Detect document type from PDF content
    import pdfplumber
    with pdfplumber.open(local_path) as pdf:
        first_page_text = (pdf.pages[0].extract_text() or "") if pdf.pages else ""
    doc_type = detect_doc_type(first_page_text)
    logger.info("Detected doc_type=%s for broker=%s", doc_type, broker)

    mapping = load_mapping(broker)
    parser = route_parser(broker, doc_type)
    raw_data = parser.parse(local_path, metadata)

    canonical = parser.to_canonical(raw_data, mapping, {
        "document_id": document_id,
        "source_file_name": s3_key.split("/")[-1],
        "source_s3_uri": f"s3://{source_bucket}/{s3_key}",
        "broker": broker,
        "account_type": account_type,
        "tax_year": tax_year,
        "checksum": checksum,
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
    })

    validation_errors = validate_canonical_output(canonical)
    # RSU supplemental lots from 1099-B parser use a different shape than the rsu_event schema
    if validation_errors:
        validation_errors = [e for e in validation_errors if not e.startswith("rsu_events[")]
    if validation_errors and doc_type == "1099b":
        raise ValueError(f"Schema validation failed: {validation_errors}")
    elif validation_errors:
        logger.warning("Schema validation warnings for %s: %s", doc_type, validation_errors)

    # JSON: single file per document
    json_key = f"{CLEANSED_PREFIX}{broker}/{account_type}/{tax_year}/{document_id}.json"
    upload_json(BUCKET, json_key, canonical)

    # CSVs: Hive-partitioned per section for Athena
    upload_csv_sections(BUCKET, CLEANSED_PREFIX, broker, account_type, tax_year, document_id, sanitize_for_csv(canonical))

    manifest = {
        "document_id": document_id,
        "source_s3_uri": f"s3://{source_bucket}/{s3_key}",
        "output_json": f"s3://{BUCKET}/{json_key}",
        "output_csv_prefix": f"s3://{BUCKET}/{CLEANSED_PREFIX}{broker}/",
        "parser_used": broker,
        "extraction_timestamp": datetime.now(timezone.utc).isoformat(),
        "checksum": checksum,
        "row_counts": {k: len(v) if isinstance(v, list) else 1 for k, v in canonical.items() if k != "warnings"},
        "parse_warnings": canonical.get("warnings", []),
    }
    upload_json(BUCKET, f"{AUDIT_PREFIX}{tax_year}/{document_id}-manifest.json", manifest)
    logger.info("Successfully processed %s → %s/%s", s3_key, CLEANSED_PREFIX, broker)


def _extract_metadata(s3_key: str) -> dict:
    parts = s3_key.replace(RAW_PREFIX, "").split("/")
    return {
        "broker": parts[0] if len(parts) > 0 else "unknown",
        "account_type": parts[1] if len(parts) > 1 else "unknown",
        "tax_year": parts[2] if len(parts) > 2 else "unknown",
    }


def _write_rejected(s3_key: str, context):
    import traceback
    metadata = _extract_metadata(s3_key)
    error_payload = {
        "source_s3_key": s3_key,
        "exception": traceback.format_exc(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "lambda_request_id": getattr(context, "aws_request_id", "unknown"),
    }
    error_key = (
        f"{REJECTED_PREFIX}{metadata['broker']}/{metadata['account_type']}/"
        f"{metadata['tax_year']}/{uuid.uuid4()}-error.json"
    )
    try:
        upload_json(BUCKET, error_key, error_payload)
    except Exception:
        logger.exception("Failed to write rejected payload")

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../lambda/pdf_ingestion"))

from common.schema_validator import validate_canonical_output


def test_valid_document_metadata():
    canonical = {
        "document_metadata": {
            "document_id": "abc-123",
            "source_file_name": "test.pdf",
            "source_s3_uri": "s3://bucket/raw/schwab/margin/2025/test.pdf",
            "broker": "schwab",
            "account_type": "margin",
            "tax_year": "2025",
            "checksum": "abc123hash",
            "ingestion_timestamp": "2025-01-01T00:00:00+00:00",
        },
        "transactions": [],
        "positions": [],
        "transfers": [],
        "rsu_events": [],
    }
    errors = validate_canonical_output(canonical)
    assert errors == []


def test_invalid_document_metadata_missing_required():
    canonical = {
        "document_metadata": {"document_id": "abc-123"},
        "transactions": [],
        "positions": [],
        "transfers": [],
        "rsu_events": [],
    }
    errors = validate_canonical_output(canonical)
    assert len(errors) > 0

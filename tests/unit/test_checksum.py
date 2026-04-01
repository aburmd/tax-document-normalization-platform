import os
import sys
import tempfile
import hashlib

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../lambda/pdf_ingestion"))

from common.checksum_utils import compute_checksum


def test_compute_checksum():
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
        f.write(b"test content for checksum")
        f.flush()
        path = f.name
    try:
        result = compute_checksum(path)
        expected = hashlib.sha256(b"test content for checksum").hexdigest()
        assert result == expected
    finally:
        os.unlink(path)

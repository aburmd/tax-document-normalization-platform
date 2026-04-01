import hashlib
import logging

logger = logging.getLogger(__name__)


def compute_checksum(file_path: str) -> str:
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    checksum = sha256.hexdigest()
    logger.info("Checksum for %s: %s", file_path, checksum)
    return checksum

import json
import logging
import os
from jsonschema import validate, ValidationError

logger = logging.getLogger(__name__)

def _find_schema_dir() -> str:
    d = os.path.dirname(os.path.abspath(__file__))
    while d != os.path.dirname(d):
        candidate = os.path.join(d, "schemas", "canonical")
        if os.path.isdir(candidate):
            return candidate
        d = os.path.dirname(d)
    return os.path.join(os.path.dirname(__file__), "../../schemas/canonical")


SCHEMA_DIR = os.environ.get("SCHEMA_DIR", _find_schema_dir())

_SECTION_SCHEMA_MAP = {
    "document_metadata": "document_metadata.json",
    "transactions": "transaction_event.json",
    "positions": "position_snapshot.json",
    "transfers": "transfer.json",
    "rsu_events": "rsu_event.json",
}


def validate_canonical_output(canonical: dict) -> list[str]:
    errors = []
    for section, schema_file in _SECTION_SCHEMA_MAP.items():
        data = canonical.get(section)
        if data is None:
            continue
        schema_path = os.path.join(SCHEMA_DIR, schema_file)
        if not os.path.exists(schema_path):
            logger.warning("Schema file not found: %s", schema_path)
            continue
        with open(schema_path) as f:
            schema = json.load(f)
        items = data if isinstance(data, list) else [data]
        for i, item in enumerate(items):
            try:
                validate(instance=item, schema=schema)
            except ValidationError as e:
                msg = f"{section}[{i}]: {e.message}"
                logger.error("Validation error: %s", msg)
                errors.append(msg)
    return errors

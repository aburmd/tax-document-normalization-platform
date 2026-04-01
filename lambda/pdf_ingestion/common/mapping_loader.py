import json
import logging
import os

logger = logging.getLogger(__name__)

MAPPING_DIR = os.path.join(os.path.dirname(__file__), "../../schemas/source")


def load_mapping(broker: str) -> dict:
    mapping_file = os.path.join(MAPPING_DIR, f"{broker.lower()}_mapping.json")
    if not os.path.exists(mapping_file):
        logger.warning("No mapping file for broker '%s', using empty mapping", broker)
        return {"field_mappings": {}, "source_columns": []}
    with open(mapping_file) as f:
        mapping = json.load(f)
    logger.info("Loaded mapping for %s: %d field mappings", broker, len(mapping.get("field_mappings", {})))
    return mapping

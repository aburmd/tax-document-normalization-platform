from abc import ABC, abstractmethod


class BaseParser(ABC):
    @abstractmethod
    def parse(self, file_path: str, metadata: dict) -> dict:
        """Extract raw structured data from the PDF file."""

    @abstractmethod
    def to_canonical(self, raw_data: dict, mapping: dict, doc_meta: dict) -> dict:
        """Map raw extracted data to canonical schema using the mapping config."""

    def _apply_mapping(self, raw_record: dict, mapping: dict) -> dict:
        result = {}
        for canonical_field, source_field in mapping.get("field_mappings", {}).items():
            result[canonical_field] = raw_record.get(source_field)
        return result

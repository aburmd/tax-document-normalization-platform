import logging
import pdfplumber
from parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)


class RsuParser(BaseParser):
    def parse(self, file_path: str, metadata: dict) -> dict:
        logger.info("Parsing RSU PDF: %s", file_path)
        pages = []
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                pages.append({
                    "text": page.extract_text() or "",
                    "tables": page.extract_tables() or [],
                })
        return {"pages": pages, "metadata": metadata}

    def to_canonical(self, raw_data: dict, mapping: dict, doc_meta: dict) -> dict:
        rsu_events, transactions = [], []
        for page in raw_data.get("pages", []):
            for table in page.get("tables", []):
                for row in table:
                    if not row or not any(row):
                        continue
                    raw_record = self._row_to_dict(row, mapping)
                    mapped = self._apply_mapping(raw_record, mapping)
                    event_type = (mapped.get("event_type") or "").upper()
                    if event_type in ("VEST", "SELL_TO_COVER", "TRANSFER_TO_MARGIN"):
                        rsu_events.append(mapped)
                    else:
                        transactions.append(mapped)
        return {
            "document_metadata": doc_meta,
            "transactions": transactions,
            "positions": [],
            "transfers": [],
            "rsu_events": rsu_events,
            "warnings": [],
        }

    def _row_to_dict(self, row: list, mapping: dict) -> dict:
        cols = mapping.get("source_columns", [])
        return {cols[i]: row[i] for i in range(min(len(cols), len(row))) if row[i] is not None}

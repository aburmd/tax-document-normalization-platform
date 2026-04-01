from parsers.base_parser import BaseParser
from parsers.schwab_statement_parser import SchwabStatementParser
from parsers.schwab_1099b_parser import Schwab1099BParser
from parsers.fidelity_parser import FidelityParser
from parsers.rsu_parser import RsuParser

_PARSERS: dict[str, type[BaseParser]] = {
    "schwab_statement": SchwabStatementParser,
    "schwab_1099b": Schwab1099BParser,
    "fidelity": FidelityParser,
    "rsu": RsuParser,
}

# Legacy broker-only keys map to default parser
_BROKER_DEFAULTS: dict[str, str] = {
    "schwab": "schwab_statement",
    "fidelity": "fidelity",
    "rsu": "rsu",
}


def route_parser(broker: str, doc_type: str = None) -> BaseParser:
    """Route to correct parser based on broker and optional document type.

    Args:
        broker: Broker name (schwab, fidelity, rsu)
        doc_type: Document type (statement, 1099b). If None, uses broker default.
    """
    if doc_type:
        key = f"{broker.lower()}_{doc_type.lower()}"
    else:
        key = _BROKER_DEFAULTS.get(broker.lower(), broker.lower())

    parser_cls = _PARSERS.get(key)
    if not parser_cls:
        raise ValueError(f"No parser registered for: {key}")
    return parser_cls()


def detect_doc_type(text: str) -> str:
    """Detect document type from PDF text content."""
    text_lower = text[:3000].lower()
    if "form 1099" in text_lower or "1099-b" in text_lower or "1099 composite" in text_lower:
        return "1099b"
    if "brokerage statement" in text_lower or "statement period" in text_lower or "transaction details" in text_lower:
        return "statement"
    if "tax information ytd" in text_lower or "year-to-date" in text_lower:
        return "taxytd"
    return "statement"  # default

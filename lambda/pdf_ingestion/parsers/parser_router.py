from parsers.base_parser import BaseParser
from parsers.schwab_statement_parser import SchwabStatementParser
from parsers.schwab_1099b_parser import Schwab1099BParser
from parsers.fidelity_statement_parser import FidelityStatementParser
from parsers.fidelity_1099b_parser import Fidelity1099BParser
from parsers.fidelity_taxytd_parser import FidelityTaxYtdParser
from parsers.robinhood_1099b_parser import Robinhood1099BParser
from parsers.webull_1099b_parser import Webull1099BParser
from parsers.etrade_1099b_parser import Etrade1099BParser
from parsers.rsu_parser import RsuParser

_PARSERS: dict[str, type[BaseParser]] = {
    "schwab_statement": SchwabStatementParser,
    "schwab_1099b": Schwab1099BParser,
    "fidelity_statement": FidelityStatementParser,
    "fidelity_1099b": Fidelity1099BParser,
    "fidelity_taxytd": FidelityTaxYtdParser,
    "robinhood_1099b": Robinhood1099BParser,
    "webull_1099b": Webull1099BParser,
    "ameritrade_1099b": Robinhood1099BParser,
    "fidelity_rsu": Fidelity1099BParser,
    "etrade_1099b": Etrade1099BParser,
    "rsu": RsuParser,
}

_BROKER_DEFAULTS: dict[str, str] = {
    "schwab": "schwab_statement",
    "fidelity": "fidelity_statement",
    "robinhood": "robinhood_1099b",
    "webull": "webull_1099b",
    "ameritrade": "ameritrade_1099b",
    "rsu": "rsu",
}


def route_parser(broker: str, doc_type: str = None) -> BaseParser:
    if doc_type:
        key = f"{broker.lower()}_{doc_type.lower()}"
    else:
        key = _BROKER_DEFAULTS.get(broker.lower(), broker.lower())
    parser_cls = _PARSERS.get(key)
    if not parser_cls:
        raise ValueError(f"No parser registered for: {key}")
    return parser_cls()


def detect_doc_type(text: str) -> str:
    text_lower = text[:5000].lower()
    if "form 1099" in text_lower or "1099-b" in text_lower or "tax reporting statement" in text_lower or "forms 1099" in text_lower:
        return "1099b"
    if "tax info ytd" in text_lower or "realized gain/loss" in text_lower and "year-to-date" in text_lower:
        return "taxytd"
    if "portfolio tax info" in text_lower or "realized gain/loss summary" in text_lower:
        return "taxytd"
    if "investment report" in text_lower or "statement period" in text_lower or "brokerage statement" in text_lower:
        return "statement"
    return "statement"

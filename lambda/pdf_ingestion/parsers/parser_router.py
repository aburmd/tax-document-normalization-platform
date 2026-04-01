from parsers.base_parser import BaseParser
from parsers.schwab_parser import SchwabParser
from parsers.fidelity_parser import FidelityParser
from parsers.rsu_parser import RsuParser

_PARSERS: dict[str, type[BaseParser]] = {
    "schwab": SchwabParser,
    "fidelity": FidelityParser,
    "rsu": RsuParser,
}


def route_parser(broker: str) -> BaseParser:
    parser_cls = _PARSERS.get(broker.lower())
    if not parser_cls:
        raise ValueError(f"No parser registered for broker: {broker}")
    return parser_cls()

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../lambda/pdf_ingestion"))

from parsers.parser_router import route_parser
from parsers.schwab_parser import SchwabParser
from parsers.fidelity_parser import FidelityParser
from parsers.rsu_parser import RsuParser


def test_route_schwab():
    assert isinstance(route_parser("schwab"), SchwabParser)


def test_route_fidelity():
    assert isinstance(route_parser("fidelity"), FidelityParser)


def test_route_rsu():
    assert isinstance(route_parser("rsu"), RsuParser)


def test_route_unknown():
    with pytest.raises(ValueError, match="No parser registered"):
        route_parser("unknown_broker")

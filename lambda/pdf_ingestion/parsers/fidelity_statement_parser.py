import logging
import re
import pdfplumber
from parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)


class FidelityStatementParser(BaseParser):
    def parse(self, file_path: str, metadata: dict) -> dict:
        logger.info("Parsing Fidelity monthly statement: %s", file_path)
        full_text = ""
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                full_text += (page.extract_text() or "") + "\n"
        return {
            "full_text": full_text,
            "metadata": metadata,
            "account_summary": self._parse_account_summary(full_text),
            "positions": self._parse_positions(full_text),
            "income_summary": self._parse_income_summary(full_text),
            "realized_gains": self._parse_realized_gains(full_text),
        }

    def to_canonical(self, raw_data: dict, mapping: dict, doc_meta: dict) -> dict:
        doc_meta["account_number_masked"] = self._extract_masked_account(raw_data["full_text"])
        period = self._extract_period(raw_data["full_text"])
        doc_meta["statement_period_start"] = period.get("start")
        doc_meta["statement_period_end"] = period.get("end")
        doc_meta["parse_status"] = "success"
        doc_meta["source_format"] = "fidelity_monthly_statement"
        return {
            "document_metadata": doc_meta,
            "account_summary": raw_data["account_summary"],
            "positions": raw_data["positions"],
            "income_summary": raw_data["income_summary"],
            "realized_gain_loss_summary": self._gains_to_list(raw_data["realized_gains"]),
            "transactions": [],
            "transfers": [],
            "rsu_events": [],
            "warnings": [],
        }

    def _extract_masked_account(self, text: str) -> str:
        m = re.search(r'Account Number:\s*([A-Z]\d{2})-?(\d{6})', text)
        if m:
            return f"****-{m.group(2)[-4:]}"
        return "unknown"

    def _extract_period(self, text: str) -> dict:
        m = re.search(r'(\w+ \d+, \d{4})\s*-\s*(\w+ \d+, \d{4})', text)
        if m:
            return {"start": m.group(1), "end": m.group(2)}
        return {"start": None, "end": None}

    def _parse_account_summary(self, text: str) -> dict:
        summary = {}
        patterns = {
            "ending_value": r'(?:Ending Account Value|Your Account Value):\s*\$?([\d,.]+)',
            "beginning_value": r'Beginning Account Value\s+\$?([\d,.]+)',
            "additions": r'Additions\s+([\d,.]+)',
            "subtractions": r'Subtractions\s+-([\d,.]+)',
            "change_in_value": r'Change in Investment Value\s+\*?\s+[+-]?([\d,.]+)',
            "free_credit_balance": r'Free Credit Balance\s+\$?([\d,.]+)',
        }
        for key, pattern in patterns.items():
            m = re.search(pattern, text)
            if m:
                summary[key] = _parse_amount(m.group(1))
                if key == "subtractions":
                    summary[key] = -summary[key]
        return summary

    def _parse_positions(self, text: str) -> list:
        positions = []

        # Core account (money market): DESCRIPTION (SYMBOL) ... QTY $PRICE $MARKET_VALUE
        core_m = re.search(
            r'(FIDELITY\s+\w+\s+MONEY\s+\w+)\s*\((\w+)\)\s+.*?'
            r'([\d,.]+)\s+\$([\d.]+)\s+\$([\d,.]+)',
            text, re.DOTALL
        )
        if core_m:
            positions.append({
                "type": "core",
                "symbol": core_m.group(2),
                "description": core_m.group(1).strip(),
                "quantity": _parse_amount(core_m.group(3)),
                "price": _parse_amount(core_m.group(4)),
                "market_value": _parse_amount(core_m.group(5)),
            })

        # ETPs and Stocks: M?DESCRIPTION (SYMBOL) BEG_MV QTY PRICE END_MV COST GAIN EAI
        # The M prefix indicates marginable
        pos_pattern = re.compile(
            r'M?([A-Z][A-Z\s&]+?)\s*\((\w+)\)\s+'  # description (symbol)
            r'([\d,.]+)\s+'                           # beginning MV
            r'([\d,.]+)\s+'                           # quantity
            r'([\d,.]+)\s+'                           # price
            r'([\d,.]+)\s+'                           # ending MV
            r'([\d,.]+)t?\s+'                         # cost basis (t = third party)
            r'-?\$?([\d,.]+)\s*'                      # unrealized gain/loss
        )
        for m in pos_pattern.finditer(text):
            desc = m.group(1).strip()
            symbol = m.group(2)
            # Skip if it's a subtotal line
            if 'Total' in desc or symbol in ('AI',):
                continue
            gain = _parse_amount(m.group(8))
            # Check if negative
            context = text[m.start():m.end()]
            if '-$' in context or '-' + m.group(8) in context:
                gain = -gain
            positions.append({
                "type": "etp" if symbol in _ETP_SYMBOLS else "stock",
                "symbol": symbol,
                "description": desc,
                "beginning_market_value": _parse_amount(m.group(3)),
                "quantity": _parse_amount(m.group(4)),
                "price": _parse_amount(m.group(5)),
                "market_value": _parse_amount(m.group(6)),
                "cost_basis": _parse_amount(m.group(7)),
                "unrealized_gain_loss": gain,
            })
        return positions

    def _parse_income_summary(self, text: str) -> dict:
        summary = {}
        m = re.search(r'Income Summary.*?Taxable\s+\$?([\d,.]+)\s+\$?([\d,.]+)', text, re.DOTALL)
        if m:
            summary["taxable_period"] = _parse_amount(m.group(1))
            summary["taxable_ytd"] = _parse_amount(m.group(2))
        div_m = re.search(r'Dividends\s+([\d,.]+)\s+([\d,.]+)', text)
        if div_m:
            summary["dividends_period"] = _parse_amount(div_m.group(1))
            summary["dividends_ytd"] = _parse_amount(div_m.group(2))
        return summary

    def _parse_realized_gains(self, text: str) -> dict:
        gains = {}
        for label, key in [
            ("Net Short-term Gain/Loss", "net_short_term"),
            ("Short-term Gain", "short_term_gain"),
            ("Short-term Loss", "short_term_loss"),
            ("Short-term Disallowed Loss", "short_term_disallowed"),
            ("Net Gain/Loss", "net_total"),
        ]:
            m = re.search(rf'{re.escape(label)}\s+[+-]?([\d,.]+)\s+[+-]?([\d,.]+)', text)
            if m:
                gains[f"{key}_period"] = _parse_amount(m.group(1))
                gains[f"{key}_ytd"] = _parse_amount(m.group(2))
        return gains

    @staticmethod
    def _gains_to_list(gains: dict) -> list:
        if not gains:
            return []
        return [{"field": k, "value": v} for k, v in gains.items()]


# Known ETP symbols for classification
_ETP_SYMBOLS = {"SOXX", "SLV", "IBIT", "ETHA", "GLD", "SPAXX", "UAE", "QQQ", "TQQQ", "SPY"}


def _parse_amount(s: str) -> float:
    if not s or s == "--":
        return 0.0
    return float(re.sub(r'[^0-9.]', '', s) or "0")

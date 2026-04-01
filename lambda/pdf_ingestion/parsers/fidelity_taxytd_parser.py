import logging
import re
import pdfplumber
from parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)


class FidelityTaxYtdParser(BaseParser):
    def parse(self, file_path: str, metadata: dict) -> dict:
        logger.info("Parsing Fidelity Tax YTD: %s", file_path)
        full_text = ""
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                full_text += (page.extract_text() or "") + "\n"
        return {
            "full_text": full_text,
            "metadata": metadata,
            "summary": self._parse_summary(full_text),
            "transactions": self._parse_transactions(full_text),
        }

    def to_canonical(self, raw_data: dict, mapping: dict, doc_meta: dict) -> dict:
        doc_meta["account_number_masked"] = self._extract_masked_account(raw_data["full_text"])
        doc_meta["parse_status"] = "success"
        doc_meta["source_format"] = "fidelity_tax_ytd"
        return {
            "document_metadata": doc_meta,
            "realized_gain_loss_summary": self._summary_to_list(raw_data["summary"]),
            "realized_gain_loss_detail": raw_data["transactions"],
            "transactions_1099b": [],
            "dividends_1099div": [],
            "interest_1099int": [],
            "positions": [],
            "transfers": [],
            "rsu_events": [],
            "warnings": [],
        }

    def _extract_masked_account(self, text: str) -> str:
        m = re.search(r'[A-Z]\d{2}[-]?\d{6}', text)
        return f"****{m.group()[-4:]}" if m else "unknown"

    def _parse_summary(self, text: str) -> dict:
        summary = {}
        for label, key in [("Short-term", "short_term"), ("Long-term", "long_term"), ("Total", "total")]:
            m = re.search(
                rf'{label}\s+([+\-])\$([\d,.]+)\s+([+\-])?\$?([\d,.]+|--)\s+([+\-])?\$?([\d,.]+|--)\s+([+\-])\$([\d,.]+)',
                text
            )
            if m:
                summary[key] = {
                    "realized_gain": _parse_amount(m.group(2)),
                    "realized_loss": -_parse_amount(m.group(4)),
                    "disallowed_loss": _parse_amount(m.group(6)),
                    "net_gain_loss": (1 if m.group(7) == '+' else -1) * _parse_amount(m.group(8)),
                }
        return summary

    def _parse_transactions(self, text: str) -> list:
        transactions = []
        # Pattern: SYMBOL DESCRIPTION QTY $PROCEEDS $COST_BASIS +/-$GAIN_LOSS
        # Some have CUSIP on next line
        lines = text.split('\n')
        current_cusip = None
        for i, line in enumerate(lines):
            line = line.strip()
            # Skip headers and non-data lines
            if not line or 'Symbol' in line or 'Security Description' in line or 'Related' in line:
                continue
            # Match transaction line: SYMBOL DESCRIPTION QTY $PROCEEDS $COST $GAIN
            m = re.match(
                r'([A-Z][A-Z0-9]{0,4})\s+'       # symbol
                r'(.+?)\s+'                        # description
                r'([\d,.]+)\s+'                    # quantity
                r'\$?([\d,.]+)\s+'                 # proceeds
                r'\$?([\d,.]+)\s+'                 # cost basis
                r'([+\-])\$?([\d,.]+)',            # gain/loss with sign
                line
            )
            if m:
                sign = 1 if m.group(6) == '+' else -1
                txn = {
                    "symbol": m.group(1),
                    "description": m.group(2).strip(),
                    "quantity": _parse_amount(m.group(3)),
                    "proceeds": _parse_amount(m.group(4)),
                    "cost_basis": _parse_amount(m.group(5)),
                    "realized_gain_loss": sign * _parse_amount(m.group(7)),
                    "cusip": None,
                    "holding_period": None,
                }
                # Check next line for CUSIP
                if i + 1 < len(lines):
                    cusip_m = re.match(r'^(\d{9}|[A-Z0-9]{9})\s*$', lines[i + 1].strip())
                    if cusip_m:
                        txn["cusip"] = cusip_m.group(1)
                # Detect holding period from column header context
                txn["holding_period"] = self._detect_holding_period(text, line)
                transactions.append(txn)
                continue

            # Option format: -SYMBOL_EXP_STRIKE PUT/CALL (UNDERLYING) DESC QTY $P $C $G
            opt_m = re.match(
                r'(-\w+)\s+(PUT|CALL)\s+\((\w+)\)\s+(.+?)\s+([\d,.]+)\s+\$?([\d,.]+)\s+\$?([\d,.]+)\s+([+\-])\$?([\d,.]+)',
                line
            )
            if opt_m:
                sign = 1 if opt_m.group(8) == '+' else -1
                transactions.append({
                    "symbol": opt_m.group(3),
                    "description": f"{opt_m.group(2)} {opt_m.group(1)} {opt_m.group(4).strip()}",
                    "quantity": _parse_amount(opt_m.group(5)),
                    "proceeds": _parse_amount(opt_m.group(6)),
                    "cost_basis": _parse_amount(opt_m.group(7)),
                    "realized_gain_loss": sign * _parse_amount(opt_m.group(9)),
                    "cusip": None,
                    "holding_period": "SHORT_TERM",
                    "is_option": True,
                })
        return transactions

    def _detect_holding_period(self, text: str, line: str) -> str:
        pos = text.find(line)
        preceding = text[:pos] if pos > 0 else ""
        if "Long-term" in preceding[max(0, len(preceding)-500):]:
            return "LONG_TERM"
        return "SHORT_TERM"

    @staticmethod
    def _summary_to_list(summary: dict) -> list:
        return [{"category": k, **v} for k, v in summary.items()]


def _parse_amount(s: str) -> float:
    if not s or s == "--":
        return 0.0
    return float(re.sub(r'[^0-9.]', '', s) or "0")


def _parse_signed_amount_from_context(val: str, context: str) -> float:
    amount = _parse_amount(val)
    if '-$' in context or context.strip().startswith('-'):
        return -amount
    return amount

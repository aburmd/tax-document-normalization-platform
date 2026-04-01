import logging
import re
import pdfplumber
from parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)


class Fidelity1099BParser(BaseParser):
    def parse(self, file_path: str, metadata: dict) -> dict:
        logger.info("Parsing Fidelity 1099-B: %s", file_path)
        full_text = ""
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                full_text += (page.extract_text() or "") + "\n"
        return {
            "full_text": full_text,
            "metadata": metadata,
            "dividends": self._parse_1099_div(full_text),
            "summary": self._parse_1099b_summary(full_text),
            "transactions": self._parse_1099b_transactions(full_text),
        }

    def to_canonical(self, raw_data: dict, mapping: dict, doc_meta: dict) -> dict:
        doc_meta["account_number_masked"] = self._extract_masked_account(raw_data["full_text"])
        doc_meta["tax_year"] = self._extract_tax_year(raw_data["full_text"])
        doc_meta["parse_status"] = "success"
        doc_meta["source_format"] = "fidelity_1099b_composite"
        return {
            "document_metadata": doc_meta,
            "dividends_1099div": [raw_data["dividends"]],
            "interest_1099int": [],
            "transactions_1099b": raw_data["transactions"],
            "realized_gain_loss_summary": self._summary_to_list(raw_data["summary"]),
            "realized_gain_loss_detail": [],
            "positions": [],
            "transfers": [],
            "rsu_events": [],
            "warnings": [],
        }

    def _extract_masked_account(self, text: str) -> str:
        m = re.search(r'Account No\.\s*([A-Z]\d{2})-?(\d{6})', text)
        if m:
            return f"****-{m.group(2)[-4:]}"
        return "unknown"

    def _extract_tax_year(self, text: str) -> str:
        m = re.search(r'(\d{4})\s+TAX REPORTING STATEMENT', text)
        return m.group(1) if m else "unknown"

    def _parse_1099_div(self, text: str) -> dict:
        div = {}
        patterns = {
            "total_ordinary_dividends": r'1a Total Ordinary Dividends[.\s]+([\d,.]+)',
            "qualified_dividends": r'1b Qualified Dividends[.\s]+([\d,.]+)',
            "total_capital_gain_distributions": r'2a Total Capital Gain Distributions[.\s]+([\d,.]+)',
            "nondividend_distributions": r'3 Nondividend Distributions[.\s]+([\d,.]+)',
            "federal_tax_withheld": r'4 Federal Income Tax Withheld[.\s]+([\d,.]+)',
            "foreign_tax_paid": r'7 Foreign Tax Paid[.\s]+([\d,.]+)',
            "exempt_interest_dividends": r'12 Exempt Interest Dividends[.\s]+([\d,.]+)',
            "state_tax_withheld": r'16 State Tax Withheld[.\s]+([\d,.]+)',
        }
        for key, pattern in patterns.items():
            m = re.search(pattern, text)
            div[key] = _parse_amount(m.group(1)) if m else 0.0
        return div

    def _parse_1099b_summary(self, text: str) -> dict:
        summary = {}
        # Lines have no spaces between words: "Short-termtransactionsforwhichbasisisreportedtotheIRS 163,399.99 ..."
        patterns = [
            ("short_term_reported", r'Short-termtransactionsforwhichbasisisreportedtotheIRS\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)'),
            ("short_term_not_reported", r'Short-termtransactionsforwhichbasisisnotreportedtotheIRS\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)'),
            ("long_term_reported", r'Long-termtransactionsforwhichbasisisreportedtotheIRS\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)'),
            ("long_term_not_reported", r'Long-termtransactionsforwhichbasisisnotreportedtotheIRS\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)'),
        ]
        for key, pattern in patterns:
            m = re.search(pattern, text)
            if m:
                summary[key] = {
                    "proceeds": _parse_amount(m.group(1)),
                    "cost_basis": _parse_amount(m.group(2)),
                    "market_discount": _parse_amount(m.group(3)),
                    "wash_sale": _parse_amount(m.group(4)),
                    "gain_loss": _parse_amount(m.group(5)),
                }

        # Total line: starts with a large number (total proceeds)
        total_m = re.search(r'^([\d,]+\.\d{2})\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)$', text, re.MULTILINE)
        if total_m:
            summary["total"] = {
                "proceeds": _parse_amount(total_m.group(1)),
                "cost_basis": _parse_amount(total_m.group(2)),
                "market_discount": _parse_amount(total_m.group(3)),
                "wash_sale": _parse_amount(total_m.group(4)),
                "gain_loss": _parse_amount(total_m.group(5)),
            }
        return summary

    def _parse_1099b_transactions(self, text: str) -> list:
        transactions = []
        current_section = "short_term_reported"
        current_symbol = None
        current_cusip = None
        current_description = None

        for line in text.split('\n'):
            line = line.strip()

            # Detect section changes
            ll = line.lower()
            if "short-term" in ll and "basis is reported" in ll and "box a" in ll:
                current_section = "short_term_reported"
                continue
            elif "short-term" in ll and "not reported" in ll and "box b" in ll:
                current_section = "short_term_not_reported"
                continue
            elif "long-term" in ll and "basis is reported" in ll and "box d" in ll:
                current_section = "long_term_reported"
                continue
            elif "long-term" in ll and "not reported" in ll and "box e" in ll:
                current_section = "long_term_not_reported"
                continue

            # Security header: DESCRIPTION,SYMBOL,CUSIP
            sec_m = re.match(r'^(.+?),\s*([A-Z][A-Z0-9]{0,4}),\s*([A-Z0-9]{9})$', line)
            if sec_m:
                current_description = sec_m.group(1).strip()
                current_symbol = sec_m.group(2)
                current_cusip = sec_m.group(3)
                continue

            # Transaction line: Sale QTY DATE_ACQ DATE_SOLD PROCEEDS COST_BASIS [GAIN_LOSS] [WASH_SALE]
            txn_m = re.match(
                r'Sale\s+'
                r'([\d,.]+)\s+'           # quantity
                r'(\d{2}/\d{2}/\d{2})\s+' # date acquired
                r'(\d{2}/\d{2}/\d{2})\s+' # date sold
                r'([\d,.]+)\s+'           # proceeds
                r'([\d,.]+)'              # cost basis
                r'(.*)',                   # rest of line (gain, wash)
                line
            )
            if txn_m and current_symbol:
                proceeds = _parse_amount(txn_m.group(4))
                cost_basis = _parse_amount(txn_m.group(5))
                rest = txn_m.group(6).strip()

                # Parse remaining numbers from rest of line
                nums = re.findall(r'-?[\d,.]+', rest)
                wash_sale = 0.0
                if len(nums) >= 2:
                    # Format: GAIN -WASH
                    gain_loss = _parse_signed(nums[0])
                    wash_sale = abs(_parse_signed(nums[1]))
                elif len(nums) == 1:
                    gain_loss = _parse_signed(nums[0])
                else:
                    gain_loss = round(proceeds - cost_basis, 2)

                transactions.append({
                    "symbol": current_symbol,
                    "cusip": current_cusip,
                    "description": current_description,
                    "quantity": _parse_amount(txn_m.group(1)),
                    "date_acquired": _normalize_date(txn_m.group(2)),
                    "date_sold": _normalize_date(txn_m.group(3)),
                    "proceeds": proceeds,
                    "cost_basis": cost_basis,
                    "wash_sale_loss_disallowed": wash_sale,
                    "realized_gain_loss": gain_loss,
                    "section": current_section,
                    "holding_period": "LONG_TERM" if "long_term" in current_section else "SHORT_TERM",
                })

        self._fix_wash_sales(transactions, text)
        return transactions

    def _fix_wash_sales(self, transactions: list, text: str):
        """No-op — wash sales are now handled inline during parsing."""
        pass

    @staticmethod
    def _summary_to_list(summary: dict) -> list:
        return [{"category": k, **v} for k, v in summary.items()]


def _parse_amount(s: str) -> float:
    if not s or s == "--":
        return 0.0
    return float(re.sub(r'[^0-9.]', '', s) or "0")


def _parse_signed(s: str) -> float:
    if not s or s == "--":
        return 0.0
    negative = s.strip().startswith('-')
    val = float(re.sub(r'[^0-9.]', '', s) or "0")
    return -val if negative else val


def _normalize_date(d: str) -> str:
    if not d:
        return d
    parts = d.split("/")
    if len(parts) == 3 and len(parts[2]) == 2:
        year = int(parts[2])
        year = year + 2000 if year < 50 else year + 1900
        return f"{year}-{parts[0]}-{parts[1]}"
    return d

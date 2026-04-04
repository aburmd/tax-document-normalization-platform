import logging
import re
import pdfplumber
from parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)


class Webull1099BParser(BaseParser):
    def parse(self, file_path: str, metadata: dict) -> dict:
        logger.info("Parsing Webull 1099-B: %s", file_path)
        full_text = ""
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                full_text += (page.extract_text() or "") + "\n"
        fmt = "iso" if re.search(r'\d{4}-\d{2}-\d{2}', full_text) else "us"
        logger.info("Detected Webull date format: %s", fmt)
        return {
            "full_text": full_text,
            "metadata": metadata,
            "format": fmt,
            "dividends": self._parse_1099_div(full_text),
            "interest": self._parse_1099_int(full_text),
            "transactions": self._parse_transactions(full_text, fmt),
            "summary": self._parse_sale_summary(full_text),
        }

    def to_canonical(self, raw_data: dict, mapping: dict, doc_meta: dict) -> dict:
        doc_meta["account_number_masked"] = self._extract_masked_account(raw_data["full_text"])
        doc_meta["tax_year"] = self._extract_tax_year(raw_data["full_text"])
        doc_meta["parse_status"] = "success"
        doc_meta["source_format"] = f"webull_apex_{raw_data['format']}"

        txns = raw_data["transactions"]
        summary = raw_data["summary"]
        warnings = self._cross_validate(txns, summary)

        return {
            "document_metadata": doc_meta,
            "dividends_1099div": [raw_data["dividends"]] if any(v for v in raw_data["dividends"].values()) else [],
            "interest_1099int": [raw_data["interest"]] if any(v for v in raw_data["interest"].values()) else [],
            "transactions_1099b": txns,
            "realized_gain_loss_summary": summary,
            "realized_gain_loss_detail": [],
            "positions": [],
            "transfers": [],
            "rsu_events": [],
            "warnings": warnings,
        }

    def _extract_masked_account(self, text: str) -> str:
        m = re.search(r'Account\s+(\w+)', text)
        return f"****-{m.group(1)[-4:]}" if m else "unknown"

    def _extract_tax_year(self, text: str) -> str:
        m = re.search(r'Composite\s+(\d{4})', text)
        if m:
            return m.group(1)
        m = re.search(r'(?:CONSOLIDATED\s+)(\d{4})\s+(?:FORMS?\s+1099|Form 1099)', text)
        if m:
            return m.group(1)
        m = re.search(r'(\d{4})Form1099', text)
        if m:
            return m.group(1)
        m = re.search(r'Tax Summary\s+(\d{4})', text)
        if m:
            return m.group(1)
        m = re.search(r'Statement Date:.*?(\d{4})', text)
        return m.group(1) if m else "unknown"

    def _parse_1099_div(self, text: str) -> dict:
        div = {}
        patterns = {
            "total_ordinary_dividends": r'1a[-.]?\s*Total [Oo]rdinary [Dd]ividends.*?(\d[\d,.]+)',
            "qualified_dividends": r'1b[-.]?\s*Qualified [Dd]ividends.*?(\d[\d,.]+)',
            "total_capital_gain_distributions": r'2a[-.]?\s*Total [Cc]apital [Gg]ain.*?(\d[\d,.]+)',
            "nondividend_distributions": r'3[-.]?\s*Non[Dd]ividend [Dd]istributions.*?(\d[\d,.]+)',
            "federal_tax_withheld": r'4[-.]?\s*Federal [Ii]ncome [Tt]ax [Ww]ithheld.*?(\d[\d,.]+)',
        }
        for key, pattern in patterns.items():
            m = re.search(pattern, text)
            div[key] = _parse_amount(m.group(1)) if m else 0.0
        return div

    def _parse_1099_int(self, text: str) -> dict:
        interest = {}
        section = re.search(r'Interest Income.*?(?=REGULATED|Miscellaneous|$)', text, re.DOTALL | re.IGNORECASE)
        block = section.group(0) if section else ""
        patterns = {
            "interest_income": r'1[-.]?\s*Interest [Ii]ncome.*?(\d[\d,.]+)',
            "federal_tax_withheld": r'4[-.]?\s*Federal [Ii]ncome [Tt]ax [Ww]ithheld.*?(\d[\d,.]+)',
            "tax_exempt_interest": r'8[-.]?\s*Tax-[Ee]xempt [Ii]nterest.*?(\d[\d,.]+)',
        }
        for key, pattern in patterns.items():
            m = re.search(pattern, block, re.IGNORECASE)
            interest[key] = _parse_amount(m.group(1)) if m else 0.0
        return interest

    def _parse_sale_summary(self, text: str) -> list:
        summary = []
        section = re.search(r'Summary Of Sale Proceeds.*?(?=Page \d|Proceeds from Broker|$)', text, re.DOTALL | re.IGNORECASE)
        if not section:
            return summary
        block = section.group(0)
        patterns = [
            ("short_term_covered", r'Short-term transactions for covered.*?(\d[\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+(-?[\d,.]+)'),
            ("short_term_noncovered", r'Short-term transactions for noncovered.*?(\d[\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+(-?[\d,.]+)'),
            ("total_short_term", r'Total Short-term\s+(-?[\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+(-?[\d,.]+)'),
            ("long_term_covered", r'Long-term transactions for covered.*?(\d[\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+(-?[\d,.]+)'),
            ("long_term_noncovered", r'Long-term transactions for noncovered.*?(\d[\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+(-?[\d,.]+)'),
            ("total_long_term", r'Total Long-term\s+(-?[\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+(-?[\d,.]+)'),
        ]
        for cat, pattern in patterns:
            m = re.search(pattern, block)
            if m:
                summary.append({
                    "category": cat,
                    "proceeds": _parse_amount(m.group(1)),
                    "cost_basis": _parse_amount(m.group(2)),
                    "market_discount": _parse_amount(m.group(3)),
                    "wash_sale": _parse_amount(m.group(4)),
                    "gain_loss": _parse_signed(m.group(5)),
                })
        return summary

    def _parse_transactions(self, text: str, fmt: str) -> list:
        if fmt == "iso":
            return self._parse_iso_transactions(text)
        return self._parse_us_transactions(text)

    def _parse_iso_transactions(self, text: str) -> list:
        transactions = []
        current_section = "short_term_covered"
        current_description = None
        current_cusip = None
        current_symbol = None

        for line in text.split("\n"):
            line = line.strip()
            ll = line.upper()

            if "SHORT-TERM" in ll and "COVERED" in ll:
                current_section = "short_term_noncovered" if "NONCOVERED" in ll else "short_term_covered"
                continue
            if "LONG-TERM" in ll and "COVERED" in ll:
                current_section = "long_term_noncovered" if "NONCOVERED" in ll else "long_term_covered"
                continue

            sec_m = re.match(r'^(.+?)\s*\|\s*CUSIP:\s*(\S+)\s*\|\s*Symbol:\s*(\S+)', line)
            if sec_m:
                current_description = sec_m.group(1).strip().rstrip('*')
                current_cusip = sec_m.group(2)
                current_symbol = sec_m.group(3)
                continue

            if line.startswith("Security Totals:") or line.startswith("Totals:"):
                continue

            txn_m = re.match(
                r'(\d{4}-\d{2}-\d{2})\s+'
                r'([\d,.]+)\s+'
                r'([\d,.]+)\s+'
                r'(\d{4}-\d{2}-\d{2}|Various)\s+'
                r'([\d,.]+)\s+'
                r'([\d,.]+)\s+'
                r'(-?[\d,.]+)',
                line
            )
            if txn_m:
                transactions.append({
                    "description": current_description or "",
                    "cusip": current_cusip or "",
                    "symbol": current_symbol or "",
                    "quantity": _parse_amount(txn_m.group(2)),
                    "date_sold": txn_m.group(1),
                    "date_acquired": txn_m.group(4),
                    "proceeds": _parse_amount(txn_m.group(3)),
                    "cost_basis": _parse_amount(txn_m.group(5)),
                    "market_discount": 0.0,
                    "wash_sale_loss_disallowed": _parse_amount(txn_m.group(6)),
                    "realized_gain_loss": _parse_signed(txn_m.group(7)),
                    "section": current_section,
                    "holding_period": "LONG_TERM" if "long_term" in current_section else "SHORT_TERM",
                })

        return transactions

    def _parse_us_transactions(self, text: str) -> list:
        transactions = []
        current_section = "short_term_covered"
        current_description = None
        current_cusip = None
        current_symbol = None

        for line in text.split("\n"):
            line = line.strip()
            ll = line.upper()

            if "SHORT-TERM" in ll or "SHORT TERM" in ll:
                if "NONCOVERED" in ll or "NOT REPORTED" in ll or "BOX B" in ll:
                    current_section = "short_term_noncovered"
                elif "COVERED" in ll or "BOX A" in ll:
                    current_section = "short_term_covered"
                continue
            if "LONG-TERM" in ll or "LONG TERM" in ll:
                if "NONCOVERED" in ll or "NOT REPORTED" in ll or "BOX E" in ll:
                    current_section = "long_term_noncovered"
                elif "COVERED" in ll or "BOX D" in ll:
                    current_section = "long_term_covered"
                continue

            sec_m = re.match(r'^(.+?)\s*\|\s*CUSIP:\s*(\S+)\s*\|\s*Symbol:\s*(\S+)', line)
            if sec_m:
                current_description = sec_m.group(1).strip().rstrip('*')
                current_cusip = sec_m.group(2)
                current_symbol = sec_m.group(3)
                continue

            if "Security Totals:" in line or line.startswith("Totals:"):
                continue

            # Format: DATE_SOLD QTY PROCEEDS DATE_ACQ|Various COST WASH GAIN [info]
            txn_m = re.match(
                r'(\d{2}/\d{2}/\d{4})\s+'
                r'([\d,.]+)\s+'
                r'([\d,.]+)\s+'
                r'(\d{2}/\d{2}/\d{4}|Various)\s+'
                r'([\d,.]+)\s+'
                r'([\d,.]+)\s+'
                r'(-?[\d,.]+)',
                line
            )
            if txn_m:
                transactions.append({
                    "description": current_description or "",
                    "cusip": current_cusip or "",
                    "symbol": current_symbol or "",
                    "quantity": _parse_amount(txn_m.group(2)),
                    "date_sold": _normalize_date(txn_m.group(1)),
                    "date_acquired": _normalize_date(txn_m.group(4)),
                    "proceeds": _parse_amount(txn_m.group(3)),
                    "cost_basis": _parse_amount(txn_m.group(5)),
                    "market_discount": 0.0,
                    "wash_sale_loss_disallowed": _parse_amount(txn_m.group(6)),
                    "realized_gain_loss": _parse_signed(txn_m.group(7)),
                    "section": current_section,
                    "holding_period": "LONG_TERM" if "long_term" in current_section else "SHORT_TERM",
                })

        return transactions

    def _cross_validate(self, transactions: list, summary: list) -> list:
        warnings = []
        if not transactions or not summary:
            if summary and not transactions:
                warnings.append("Summary found but no transactions parsed")
            return warnings

        calc_proceeds = round(sum(t["proceeds"] for t in transactions), 2)
        calc_cost = round(sum(t["cost_basis"] for t in transactions), 2)
        calc_wash = round(sum(t["wash_sale_loss_disallowed"] for t in transactions), 2)
        calc_gain = round(sum(t["realized_gain_loss"] for t in transactions), 2)

        total = {"proceeds": 0, "cost_basis": 0, "wash_sale": 0, "gain_loss": 0}
        for s in summary:
            if "total" in s["category"]:
                for k in total:
                    total[k] += s.get(k, 0)

        tolerance = 0.10
        for field, calc_val, sum_key in [
            ("proceeds", calc_proceeds, "proceeds"),
            ("cost_basis", calc_cost, "cost_basis"),
            ("wash_sale", calc_wash, "wash_sale"),
            ("gain_loss", calc_gain, "gain_loss"),
        ]:
            expected = total[sum_key]
            if abs(calc_val - expected) > tolerance:
                warnings.append(
                    f"VALIDATION MISMATCH: {field} -- parsed_sum={calc_val}, summary={expected}, diff={round(calc_val - expected, 2)}"
                )

        if not warnings:
            logger.info("Cross-validation PASSED")
        else:
            for w in warnings:
                logger.warning(w)
        return warnings


def _parse_amount(s: str) -> float:
    if not s or s == "--" or s == "0":
        return 0.0
    return float(re.sub(r'[^0-9.]', '', s) or "0")


def _parse_signed(s: str) -> float:
    if not s or s == "--":
        return 0.0
    negative = s.strip().startswith("-")
    val = float(re.sub(r'[^0-9.]', '', s) or "0")
    return -val if negative else val


def _normalize_date(d: str) -> str:
    if not d or d == "Various":
        return d
    parts = d.split("/")
    if len(parts) == 3 and len(parts[2]) == 4:
        return f"{parts[2]}-{parts[0]}-{parts[1]}"
    return d

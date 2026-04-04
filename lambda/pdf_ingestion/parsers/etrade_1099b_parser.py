import logging
import re
import pdfplumber
from parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)


class Etrade1099BParser(BaseParser):
    def parse(self, file_path: str, metadata: dict) -> dict:
        logger.info("Parsing E*Trade 1099-B: %s", file_path)
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
        doc_meta["account_number_masked"] = self._extract_account(raw_data["full_text"])
        doc_meta["tax_year"] = self._extract_tax_year(raw_data["full_text"])
        doc_meta["parse_status"] = "success"
        doc_meta["source_format"] = "etrade_1099b"
        warnings = self._cross_validate(raw_data)
        return {
            "document_metadata": doc_meta,
            "dividends_1099div": [],
            "interest_1099int": [],
            "transactions_1099b": raw_data["transactions"],
            "realized_gain_loss_summary": self._summary_to_list(raw_data["summary"]),
            "realized_gain_loss_detail": [],
            "positions": [],
            "transfers": [],
            "rsu_events": [],
            "warnings": warnings,
        }

    def _extract_account(self, text: str) -> str:
        m = re.search(r'Account No:\s*(\d+)', text)
        return f"****{m.group(1)[-4:]}" if m else "unknown"

    def _extract_tax_year(self, text: str) -> str:
        m = re.search(r'CONSOLIDATED\s+(\d{4})\s+FORMS\s+1099', text)
        if m:
            return m.group(1)
        m = re.search(r'(\d{4})\s+FORM\s+1099', text)
        return m.group(1) if m else "unknown"

    def _parse_summary(self, text: str) -> dict:
        summary = {}
        patterns = [
            ("short_term_reported", r'Box A \(basis reported to IRS\)\s+(.+)'),
            ("short_term_not_reported", r'Box B \(basis not reported to IRS\)\s+(.+)'),
            ("long_term_reported", r'Box D \(basis reported to IRS\)\s+(.+)'),
            ("long_term_not_reported", r'Box E \(basis not reported to IRS\)\s+(.+)'),
        ]
        for key, pattern in patterns:
            m = re.search(pattern, text)
            if m:
                nums = re.findall(r'\$[\d,.]+|\(\$[\d,.]+\)', m.group(1))
                if len(nums) >= 5:
                    summary[key] = {
                        "proceeds": _parse_dollar(nums[0]),
                        "cost_basis": _parse_dollar(nums[1]),
                        "market_discount": _parse_dollar(nums[2]),
                        "wash_sale": _parse_dollar(nums[3]),
                        "gain_loss": _parse_dollar(nums[4]),
                    }
        return summary

    def _parse_transactions(self, text: str) -> list:
        transactions = []
        current_section = "short_term_reported"
        lines = text.split('\n')
        i = 0
        while i < len(lines):
            line = lines[i].strip()

            # Section detection
            section = self._detect_section(line)
            if section:
                current_section = section
                i += 1
                continue

            # Skip non-transaction lines
            if not line or 'ITEMS − TOTAL' in line or line.startswith('Subtotals'):
                i += 1
                continue
            if any(skip in line for skip in [
                'FORM 1099', 'Account No:', 'Account Name:', 'JERSEY CITY',
                'PO BOX', 'ORIGINAL:', 'RECIPIENT', 'PAYER', 'FATCA',
                'Telephone', 'The information', 'carefully when',
                'position or', 'The 1099-B', 'Box 6:', 'Box 5:',
                'Accrued', 'Description of property',
                '(Box 1a)', 'THIS IS YOUR', 'This is important',
                'may be imposed', 'FOOTNOTES', 'SHORT SALE',
                'END OF', 'OMB NO.', 'E*TRADE', 'MORGAN STANLEY',
                'Recipient', 'Page ',
            ]):
                i += 1
                continue

            # Try to parse a transaction line
            txn, consumed = self._try_parse_transaction(lines, i, current_section)
            if txn:
                transactions.append(txn)
                i += consumed
            else:
                i += 1

        return transactions

    def _detect_section(self, line: str) -> str | None:
        ll = line.lower().replace('\u2212', '-').replace('\u2013', '-')
        if 'covered short' in ll and 'term' in ll:
            return "short_term_reported"
        if 'covered long' in ll and 'term' in ll:
            return "long_term_reported"
        if 'report on form 8949' in ll:
            if 'box a' in ll:
                return "short_term_reported"
            if 'box b' in ll:
                return "short_term_not_reported"
            if 'box d' in ll:
                return "long_term_reported"
            if 'box e' in ll:
                return "long_term_not_reported"
        return None

    def _try_parse_transaction(self, lines: list, idx: int, section: str) -> tuple:
        """Try to parse a transaction starting at idx. Returns (txn_dict, lines_consumed) or (None, 0)."""
        line = lines[idx].strip()

        # Transaction line pattern: DESCRIPTION QTY DATE_ACQ DATE_SOLD $PROCEEDS $COST $MKT_DISC $WASH ($GL)
        # The description may have *** prefix and the amounts are $-prefixed
        txn_m = re.match(
            r'(?:\*{3})?(.+?)\s+'
            r'(\d+\.\d{5})\s+'                    # quantity (5 decimal places)
            r'(\d{2}/\d{2}/\d{4})\s+'              # date acquired
            r'(\d{2}/\d{2}/\d{4})\s+'              # date sold
            r'(\$[\d,.]+)\s+'                       # proceeds
            r'(\$[\d,.]+)\s+'                       # cost basis
            r'(\$[\d,.]+)\s+'                       # market discount
            r'(\$[\d,.]+)\s+'                       # wash sale
            r'(\(?-?\$[\d,.]+\)?)',                  # gain/loss
            line
        )
        if txn_m:
            description = txn_m.group(1).strip()
            cusip = None
            consumed = 1

            # Look ahead for CUSIP and additional description lines
            for j in range(idx + 1, min(idx + 5, len(lines))):
                next_line = lines[j].strip()
                cusip_m = re.match(r'CUSIP:\s*([A-Z0-9]+)$', next_line)
                if cusip_m:
                    cusip = cusip_m.group(1)
                    consumed = j - idx + 1
                    break
                # CUSIP line that also has transaction data — extract CUSIP but don't consume
                cusip_txn = re.match(r'CUSIP:\s*([A-Z0-9]+)\s+\d+\.\d{5}', next_line)
                if cusip_txn:
                    cusip = cusip_txn.group(1)
                    break
                # Additional description line (no numbers = continuation)
                if next_line and not re.search(r'\d{2}/\d{2}/\d{4}', next_line) and not next_line.startswith('CUSIP:'):
                    description += " " + next_line
                    consumed = j - idx + 1
                else:
                    break

            symbol = _extract_symbol(description)
            return {
                "symbol": symbol,
                "cusip": cusip,
                "description": description,
                "quantity": float(txn_m.group(2)),
                "date_acquired": _normalize_date(txn_m.group(3)),
                "date_sold": _normalize_date(txn_m.group(4)),
                "proceeds": _parse_dollar(txn_m.group(5)),
                "cost_basis": _parse_dollar(txn_m.group(6)),
                "market_discount": _parse_dollar(txn_m.group(7)),
                "wash_sale_loss_disallowed": _parse_dollar(txn_m.group(8)),
                "realized_gain_loss": _parse_dollar(txn_m.group(9)),
                "section": section,
                "holding_period": "LONG_TERM" if "long_term" in section else "SHORT_TERM",
            }, consumed

        # Continuation transaction (same CUSIP group, no description prefix)
        # e.g. "0.01427 12/30/2021 09/30/2022 $1.08 $1.44 $0.00 $0.00 ($0.36) CASH IN LIEU"
        # Also handles: "CUSIP: 22542D225 0.01427 12/30/2021 ..."
        stripped = re.sub(r'^CUSIP:\s*[A-Z0-9]+\s+', '', line)
        cont_m = re.match(
            r'(\d+\.\d{5})\s+'
            r'(\d{2}/\d{2}/\d{4})\s+'
            r'(\d{2}/\d{2}/\d{4})\s+'
            r'(\$[\d,.]+)\s+'
            r'(\$[\d,.]+)\s+'
            r'(\$[\d,.]+)\s+'
            r'(\$[\d,.]+)\s+'
            r'(\(?-?\$[\d,.]+\)?)',
            stripped
        )
        if cont_m:
            return {
                "symbol": None,
                "cusip": None,
                "description": None,
                "quantity": float(cont_m.group(1)),
                "date_acquired": _normalize_date(cont_m.group(2)),
                "date_sold": _normalize_date(cont_m.group(3)),
                "proceeds": _parse_dollar(cont_m.group(4)),
                "cost_basis": _parse_dollar(cont_m.group(5)),
                "market_discount": _parse_dollar(cont_m.group(6)),
                "wash_sale_loss_disallowed": _parse_dollar(cont_m.group(7)),
                "realized_gain_loss": _parse_dollar(cont_m.group(8)),
                "section": section,
                "holding_period": "LONG_TERM" if "long_term" in section else "SHORT_TERM",
            }, 1

        return None, 0

    def _cross_validate(self, raw_data: dict) -> list:
        warnings = []
        summary = raw_data.get("summary", {})
        txns = raw_data.get("transactions", [])
        if not summary:
            return warnings

        for section_key in ["short_term_reported", "short_term_not_reported",
                            "long_term_reported", "long_term_not_reported"]:
            s = summary.get(section_key)
            if not s or s["proceeds"] == 0:
                continue
            section_txns = [t for t in txns if t["section"] == section_key]
            txn_proceeds = sum(t["proceeds"] for t in section_txns)
            diff = abs(txn_proceeds - s["proceeds"])
            if diff > 0.02:
                warnings.append(
                    f"{section_key}: proceeds diff={txn_proceeds - s['proceeds']:.2f} "
                    f"(parsed={txn_proceeds:.2f}, summary={s['proceeds']:.2f})"
                )
            else:
                logger.info("E*Trade %s cross-validation passed: proceeds=%.2f",
                            section_key, s["proceeds"])
        return warnings

    @staticmethod
    def _summary_to_list(summary: dict) -> list:
        return [{"category": k, **v} for k, v in summary.items()]


def _parse_dollar(s: str) -> float:
    if not s:
        return 0.0
    negative = '(' in s
    val = float(re.sub(r'[^0-9.]', '', s) or "0")
    return -val if negative else val


def _normalize_date(d: str) -> str:
    if not d:
        return d
    parts = d.split("/")
    if len(parts) == 3:
        return f"{parts[2]}-{parts[0]}-{parts[1]}"
    return d


def _extract_symbol(description: str) -> str:
    """Best-effort symbol extraction from E*Trade description."""
    # Options: "PUT AAPL 09/11/20 111.2" or "CALL MSFT 12/24/20 222.5"
    opt_m = re.match(r'(?:PUT|CALL)\s+([A-Z]+)', description)
    if opt_m:
        return opt_m.group(1)
    # Stocks: take first word-like token that looks like a ticker
    words = description.split()
    for w in words:
        if re.match(r'^[A-Z]{1,5}$', w):
            return w
    return description.split()[0] if description.split() else "UNKNOWN"

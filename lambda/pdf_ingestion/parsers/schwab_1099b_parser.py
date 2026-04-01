import logging
import re
import pdfplumber
from parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)


class Schwab1099BParser(BaseParser):
    def parse(self, file_path: str, metadata: dict) -> dict:
        logger.info("Parsing Schwab 1099-B: %s", file_path)
        full_text = ""
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                full_text += (page.extract_text() or "") + "\n"
        return {
            "full_text": full_text,
            "metadata": metadata,
            "cusip_symbol_map": self._build_cusip_symbol_map(full_text),
            "dividends": self._parse_1099_div(full_text),
            "interest": self._parse_1099_int(full_text),
            "short_term_reported": self._parse_1099b_transactions(full_text, "SHORT-TERM", "Box A"),
            "short_term_not_reported": self._parse_1099b_transactions(full_text, "SHORT-TERM", "Box B"),
            "long_term_reported": self._parse_1099b_transactions(full_text, "LONG-TERM", "Box D"),
            "realized_gain_loss_detail": self._parse_year_end_realized(full_text),
            "realized_gain_loss_summary": self._parse_realized_summary(full_text),
            "dividend_detail": self._parse_dividend_detail(full_text),
            "interest_detail": self._parse_interest_detail(full_text),
        }

    def to_canonical(self, raw_data: dict, mapping: dict, doc_meta: dict) -> dict:
        doc_meta["account_number_masked"] = self._extract_masked_account(raw_data["full_text"])
        doc_meta["tax_year"] = self._extract_tax_year(raw_data["full_text"])
        doc_meta["parse_status"] = "success"
        doc_meta["source_format"] = "schwab_1099b_composite"

        # Build CUSIP→symbol map from all sources
        cusip_symbol_map = raw_data.get("cusip_symbol_map", {})
        for txn_list in [raw_data["short_term_reported"], raw_data["short_term_not_reported"], raw_data["long_term_reported"]]:
            for txn in txn_list:
                if txn.get("cusip") and txn.get("symbol"):
                    cusip_symbol_map[txn["cusip"]] = txn["symbol"]

        all_transactions = []
        for txn in raw_data["short_term_reported"]:
            txn["holding_period"] = "SHORT_TERM"
            txn["irs_reporting"] = "Box A - basis reported to IRS"
            if not txn.get("symbol") and txn.get("cusip"):
                txn["symbol"] = cusip_symbol_map.get(txn["cusip"])
            if not txn.get("symbol"):
                txn["symbol"] = _symbol_from_description(txn["description"])
            all_transactions.append(txn)
        for txn in raw_data["short_term_not_reported"]:
            txn["holding_period"] = "SHORT_TERM"
            txn["irs_reporting"] = "Box B - basis available but not reported"
            if not txn.get("symbol") and txn.get("cusip"):
                txn["symbol"] = cusip_symbol_map.get(txn["cusip"])
            if not txn.get("symbol"):
                txn["symbol"] = _symbol_from_description(txn["description"])
            all_transactions.append(txn)
        for txn in raw_data["long_term_reported"]:
            txn["holding_period"] = "LONG_TERM"
            txn["irs_reporting"] = "Box D - basis reported to IRS"
            if not txn.get("symbol") and txn.get("cusip"):
                txn["symbol"] = cusip_symbol_map.get(txn["cusip"])
            if not txn.get("symbol"):
                txn["symbol"] = _symbol_from_description(txn["description"])
            all_transactions.append(txn)

        # Backfill symbols in year-end detail
        for txn in raw_data["realized_gain_loss_detail"]:
            if not txn.get("symbol") and txn.get("cusip"):
                txn["symbol"] = cusip_symbol_map.get(txn["cusip"])
            if not txn.get("symbol"):
                txn["symbol"] = _symbol_from_description(txn["description"])

        return {
            "document_metadata": doc_meta,
            "dividends_1099div": [raw_data["dividends"]],
            "interest_1099int": [raw_data["interest"]],
            "transactions_1099b": all_transactions,
            "realized_gain_loss_detail": raw_data["realized_gain_loss_detail"],
            "realized_gain_loss_summary": self._summary_dict_to_list(raw_data["realized_gain_loss_summary"]),
            "dividend_detail": raw_data["dividend_detail"],
            "interest_detail": raw_data["interest_detail"],
            "transfers": [],
            "rsu_events": [],
            "positions": [],
            "warnings": [],
        }

    @staticmethod
    def _summary_dict_to_list(summary: dict) -> list:
        """Convert realized_gain_loss_summary dict-of-dicts to list-of-dicts for CSV."""
        rows = []
        for category, values in summary.items():
            if isinstance(values, dict):
                row = {"category": category}
                row.update(values)
                rows.append(row)
        return rows

    def _extract_masked_account(self, text: str) -> str:
        m = re.search(r'(\d{4})-(\d{4})', text)
        return f"****-{m.group(2)}" if m else "unknown"

    def _extract_tax_year(self, text: str) -> str:
        m = re.search(r'TAX YEAR (\d{4})', text)
        return m.group(1) if m else "unknown"

    def _build_cusip_symbol_map(self, text: str) -> dict:
        """Build CUSIP→symbol map from all pages."""
        cmap = {}
        # Pattern 1: CUSIP / SYMBOL on 1099-B pages (most reliable)
        for m in re.finditer(r'(\d{9})\s*/\s*([A-Z]{1,5})\s', text):
            cmap[m.group(1)] = m.group(2)
        # Pattern 2: Dividend detail lines "DESCRIPTION SYMBOL CUSIP $ amount"
        for m in re.finditer(r'\b([A-Z]{2,5})\s+(\d{9})\s+\$\s*[\d,.]+', text):
            sym = m.group(1)
            cusip = m.group(2)
            if cusip not in cmap and len(sym) <= 5:
                cmap[cusip] = sym
        # Pattern 3: Year-End lines with known format "SYMBOL CUSIP QTY DATE"
        # Only use if symbol is 1-5 uppercase letters followed by 9-digit CUSIP
        for m in re.finditer(r'\b([A-Z]{1,5})\s+(\d{9})\s+[\d,.]+\s+\d{2}/\d{2}/\d{2}', text):
            sym = m.group(1)
            cusip = m.group(2)
            if cusip not in cmap:
                cmap[cusip] = sym
        return cmap

    def _parse_1099_div(self, text: str) -> dict:
        div = {}
        div_section = _extract_section(text, "Dividends and Distributions", "INSTRUCTIONS FOR RECIPIENTS")
        if not div_section:
            div_section = _extract_section(text, "1099-DIV", "INSTRUCTIONS FOR RECIPIENTS")
        if not div_section:
            div_section = text

        patterns = {
            "total_ordinary_dividends": r'1a\s+Total Ordinary Dividends\s+\$\s*([\d,.]+)',
            "qualified_dividends": r'1b\s+Qualified Dividends\s+\$\s*([\d,.]+)',
            "total_capital_gain_distributions": r'2a\s+Total Capital Gain Distributions\s+\$\s*([\d,.]+)',
            "unrecap_sec_1250_gain": r'2b\s+Unrecap.*?Gain\s+\$\s*([\d,.]+)',
            "section_1202_gain": r'2c\s+Section 1202 Gain\s+\$\s*([\d,.]+)',
            "collectibles_gain": r'2d\s+Collectibles.*?Gain\s+\$\s*([\d,.]+)',
            "section_897_ordinary": r'2e\s+Section 897 Ordinary Dividends\s+\$\s*([\d,.]+)',
            "section_897_capital_gains": r'2f\s+Section 897 Capital Gains\s+\$\s*([\d,.]+)',
            "nondividend_distributions": r'3\s+Nondividend Distributions\s+\$\s*([\d,.]+)',
            "federal_tax_withheld": r'4\s+Federal Income Tax Withheld\s+\$\s*([\d,.]+)',
            "section_199a_dividends": r'5\s+Section 199A Dividends\s+\$\s*([\d,.]+)',
            "investment_expenses": r'6\s+Investment Expenses\s+\$\s*([\d,.]+)',
            "foreign_tax_paid": r'7\s+Foreign Tax Paid\s+\$\s*([\d,.]+)',
            "exempt_interest_dividends": r'12\s+Exempt-Interest Dividends\s+\$\s*([\d,.]+)',
            "state_tax_withheld": r'16\s+State Tax Withheld\s+\$\s*([\d,.]+)',
        }
        # Also try patterns with space variations (Schwab PDFs have inconsistent spacing)
        alt_patterns = {
            "total_ordinary_dividends": r'Total Ordinary Dividends\s+\$\s*([\d,.]+)',
            "qualified_dividends": r'Qualified Dividends\s+\$\s*([\d,.]+)',
        }
        search_text = div_section or text
        for key, pattern in patterns.items():
            m = re.search(pattern, search_text)
            if not m and key in alt_patterns:
                m = re.search(alt_patterns[key], search_text)
            div[key] = _parse_amount(m.group(1)) if m else 0.0
        return div

    def _parse_1099_int(self, text: str) -> dict:
        interest = {}
        # Search full text — section extraction is unreliable due to PDF formatting
        patterns = {
            "interest_income": r'1\s+Interest Income\s+\$\s*([\d,.]+)',
            "us_savings_bond_interest": r'3\s+Interest on U\.S\. Savings.*?\$\s*([\d,.]+)',
            "federal_tax_withheld": r'4\s+Federal Income Tax Withheld\s+\$\s*([\d,.]+)',
            "tax_exempt_interest": r'8\s+Tax-Exempt Interest\s+\$\s*([\d,.]+)',
            "market_discount": r'10\s+Market Discount\s+\$\s*([\d,.]+)',
            "bond_premium": r'11\s+Bond Premium\s+\$\s*([\d,.]+)',
            "state_tax_withheld": r'17\s+State Tax Withheld\s+\$\s*([\d,.]+)',
        }
        for key, pattern in patterns.items():
            m = re.search(pattern, text)
            interest[key] = _parse_amount(m.group(1)) if m else 0.0
        return interest

    def _parse_1099b_transactions(self, text: str, term: str, box: str) -> list:
        transactions = []
        # Find sections matching the term and box
        if term == "SHORT-TERM" and box == "Box A":
            section_header = "SHORT-TERM TRANSACTIONS FOR WHICH BASIS IS REPORTED TO THE IRS"
        elif term == "SHORT-TERM" and box == "Box B":
            section_header = "SHORT-TERM TRANSACTIONS FOR WHICH BASIS IS AVAILABLE BUTNOT REPORTED"
        elif term == "LONG-TERM" and box == "Box D":
            section_header = "LONG-TERM TRANSACTIONS FOR WHICH BASIS IS REPORTED TO THE IRS"
        else:
            return transactions

        # Find all occurrences of this section (can span multiple pages)
        sections = []
        pos = 0
        while True:
            idx = text.find(section_header, pos)
            if idx < 0:
                break
            # Find end: next section header or FATCA
            end = len(text)
            for marker in ["LONG-TERM TRANSACTIONS", "SHORT-TERM TRANSACTIONS FOR WHICH BASIS IS AVAILABLE",
                           "Total Short-Term", "Total Long-Term", "FATCA Filing"]:
                next_idx = text.find(marker, idx + len(section_header))
                if 0 < next_idx < end:
                    end = next_idx
            sections.append(text[idx:end])
            pos = idx + len(section_header)

        full_section = "\n".join(sections)
        if not full_section:
            return transactions

        # Parse transaction lines
        # Format: QTY DESCRIPTION TERM DATE $ PROCEEDS $ COST_BASIS WASH $ GAIN_LOSS
        #         CUSIP / SYMBOL DATE
        # Multi-line: first line has qty, description, term, date_acquired, proceeds, cost, wash, gain
        #             second line has CUSIP / symbol, date_sold
        txn_pattern = re.compile(
            r'(\d+)\s+'                          # quantity
            r'(.+?)\s+'                          # description
            r'(S|SC)\s+'                         # term indicator (S=sold, SC=sold to close)
            r'(\d{2}/\d{2}/\d{2}|VARIOUS)\s+'   # date acquired
            r'\$\s*([\d,.]+)\s+'                 # proceeds
            r'\$\s*([\d,.]+)\s+'                 # cost basis
            r'(--|\$\s*[\d,.]+)\s+'              # wash sale
            r'\$?\s*\(?([\d,.]+)\)?'             # gain/loss
        )
        cusip_pattern = re.compile(r'(\d{9})\s*/?\s*(\w+)?\s+(\d{2}/\d{2}/\d{2})')

        lines = full_section.split('\n')

        # First pass: build CUSIP→symbol map from lines that have both
        cusip_symbol_map = {}
        for line in lines:
            cm = re.search(r'(\d{9})\s*/\s*(\w+)', line)
            if cm:
                cusip_symbol_map[cm.group(1)] = cm.group(2)

        i = 0
        while i < len(lines):
            line = lines[i].strip()
            m = txn_pattern.search(line)
            if m:
                txn = {
                    "quantity": float(m.group(1)),
                    "description": m.group(2).strip(),
                    "action": m.group(3),
                    "date_acquired": _normalize_date(m.group(4)),
                    "proceeds": _parse_amount(m.group(5)),
                    "cost_basis": _parse_amount(m.group(6)),
                    "wash_sale_loss_disallowed": _parse_amount(m.group(7).replace("--", "0").replace("$", "")),
                    "realized_gain_loss": _parse_signed_amount(m.group(8)),
                    "cusip": None,
                    "symbol": None,
                    "date_sold": None,
                }
                # Check if gain is actually a loss (negative in original)
                if "(" in line and txn["realized_gain_loss"] > 0:
                    txn["realized_gain_loss"] = -txn["realized_gain_loss"]

                # Look for CUSIP/symbol on next line
                if i + 1 < len(lines):
                    cusip_m = cusip_pattern.search(lines[i + 1])
                    if cusip_m:
                        txn["cusip"] = cusip_m.group(1)
                        txn["symbol"] = cusip_m.group(2) or cusip_symbol_map.get(cusip_m.group(1))
                        txn["date_sold"] = _normalize_date(cusip_m.group(3))
                        i += 1
                    else:
                        # Try CUSIP-only line without symbol
                        cusip_only = re.search(r'(\d{9})\s+(\d{2}/\d{2}/\d{2})', lines[i + 1])
                        if cusip_only:
                            txn["cusip"] = cusip_only.group(1)
                            txn["symbol"] = cusip_symbol_map.get(cusip_only.group(1))
                            txn["date_sold"] = _normalize_date(cusip_only.group(2))
                            i += 1
                transactions.append(txn)
            i += 1

        # Also try to extract Security Subtotals
        subtotal_pattern = re.compile(
            r'Security Subtotal\s+\$\s*([\d,.]+)\s+\$\s*([\d,.]+)\s+(--|\$\s*[\d,.]+)\s+\$?\s*\(?([\d,.]+)\)?'
        )
        for m in subtotal_pattern.finditer(full_section):
            pass  # Subtotals are informational, individual txns are what we need

        return transactions

    def _parse_year_end_realized(self, text: str) -> list:
        """Parse the Year-End Summary Realized Gain/Loss detail — most complete data."""
        transactions = []
        ye_start = text.find("YEAR-END SUMMARY")
        if ye_start < 0:
            return transactions

        ye_text = text[ye_start:]

        # Pattern for Year-End realized gain/loss lines:
        # DESCRIPTION CUSIP QTY DATE_ACQUIRED DATE_SOLD $ PROCEEDS $ COST_BASIS WASH $ GAIN_LOSS
        txn_pattern = re.compile(
            r'(.+?)\s+'                          # description
            r'(\d{9})\s+'                        # CUSIP
            r'([\d,.]+)\s+'                      # quantity
            r'(\d{2}/\d{2}/\d{2})'               # date acquired
            r'(\d{2}/\d{2}/\d{2})'               # date sold (no space between dates)
            r'\$\s*([\d,.]+)\s+'                 # proceeds
            r'\$\s*([\d,.]+)\s+'                 # cost basis
            r'(--|\$\s*[\d,.]+)\s+'              # wash sale
            r'\$?\s*\(?([\d,.]+)\)?'             # gain/loss
        )

        # Determine section type for each transaction
        current_section = "short_term_box_a"  # default
        for line in ye_text.split('\n'):
            ll = line.lower()
            if "short-term" in ll and "box a checked" in ll:
                current_section = "short_term_box_a"
            elif "basis is reported to the irs" in ll and "short-term" not in ll and "long-term" not in ll:
                # continuation line like "...Box A checked."
                if "box a" in ll:
                    current_section = "short_term_box_a"
                elif "box b" in ll:
                    current_section = "short_term_box_b"
                elif "box d" in ll:
                    current_section = "long_term_box_d"
            elif "short-term" in ll and ("not reported" in ll or "box b" in ll):
                current_section = "short_term_box_b"
            elif "long-term" in ll and ("box d" in ll or "basis is reported" in ll):
                current_section = "long_term_box_d"

            m = txn_pattern.search(line)
            if m:
                gain_loss = _parse_amount(m.group(9))
                if "(" in line[line.rfind("$"):]:
                    gain_loss = -gain_loss

                transactions.append({
                    "description": m.group(1).strip(),
                    "cusip": m.group(2),
                    "quantity": _parse_amount(m.group(3)),
                    "date_acquired": _normalize_date_full(m.group(4)),
                    "date_sold": _normalize_date_full(m.group(5)),
                    "proceeds": _parse_amount(m.group(6)),
                    "cost_basis": _parse_amount(m.group(7)),
                    "wash_sale_loss_disallowed": _parse_amount(m.group(8).replace("--", "0").replace("$", "")),
                    "realized_gain_loss": gain_loss,
                    "section": current_section,
                })

        # Also try the option format: QQQ 01/16/2026 520.00 C QTY DATE DATE $ ...
        opt_pattern = re.compile(
            r'(\w+)\s+(\d{2}/\d{2}/\d{4})\s+([\d,.]+)\s+([CP])\s+'
            r'([\d,.]+)\s+'
            r'(\d{2}/\d{2}/\d{2})'
            r'(\d{2}/\d{2}/\d{2})'
            r'\$\s*([\d,.]+)\s+'
            r'\$\s*([\d,.]+)\s+'
            r'(--|\$\s*[\d,.]+)\s+'
            r'\$?\s*\(?([\d,.]+)\)?'
        )
        for m in opt_pattern.finditer(ye_text):
            gain_loss = _parse_amount(m.group(11))
            opt_type = "CALL" if m.group(4) == "C" else "PUT"
            transactions.append({
                "description": f"{opt_type} {m.group(1)} ${m.group(3)} EXP {m.group(2)}",
                "cusip": None,
                "symbol": m.group(1),
                "quantity": _parse_amount(m.group(5)),
                "date_acquired": _normalize_date_full(m.group(6)),
                "date_sold": _normalize_date_full(m.group(7)),
                "proceeds": _parse_amount(m.group(8)),
                "cost_basis": _parse_amount(m.group(9)),
                "wash_sale_loss_disallowed": _parse_amount(m.group(10).replace("--", "0").replace("$", "")),
                "realized_gain_loss": gain_loss,
                "section": current_section,
                "is_option": True,
            })

        return transactions

    def _parse_realized_summary(self, text: str) -> dict:
        summary = {}
        # Find the actual summary section (not the table of contents reference)
        summary_start = text.rfind("Realized Gain or (Loss) Summary")
        if summary_start < 0:
            return summary
        summary_section = text[summary_start:summary_start + 3000]

        # Join lines and use broader matching for summary totals
        joined = re.sub(r'\n\s*', ' ', summary_section)
        
        # Match "Total Short-Term" or "Total Long-Term" lines with $ amounts
        line_pattern = re.compile(
            r'(Total (?:Short|Long)-Term Realized Gain or \(Loss\)[^$]*)'
            r'\$\s*([\d,.]+)\s+\$\s*([\d,.]+)\s+\$?\s*([\d,.]+|--)\s+\$\s*([\d,.]+)'
        )
        for m in line_pattern.finditer(joined):
            desc = m.group(1).strip()
            entry = {
                "proceeds": _parse_amount(m.group(2)),
                "cost_basis": _parse_amount(m.group(3)),
                "wash_sale": _parse_amount(m.group(4).replace("--", "0")),
                "gain_loss": _parse_amount(m.group(5)),
            }
            if "Short-Term" in desc and "reported to the IRS" in desc:
                summary["total_short_term_box_a"] = entry
            elif "Short-Term" in desc and "not reported" in desc:
                summary["total_short_term_box_b"] = entry
            elif "Short-Term" in desc:
                summary["total_short_term"] = entry
            elif "Long-Term" in desc and "reported to the IRS" in desc:
                summary["total_long_term_box_d"] = entry
            elif "Long-Term" in desc:
                summary["total_long_term"] = entry

        # Total line
        total_m = re.search(
            r'TOTAL REALIZED GAIN OR \(LOSS\)[^$]*'
            r'\$\s*([\d,.]+)\s+\$\s*([\d,.]+)\s+\$?\s*([\d,.]+|--)\s+\$\s*([\d,.]+)',
            joined
        )
        if total_m:
            summary["total"] = {
                "proceeds": _parse_amount(total_m.group(1)),
                "cost_basis": _parse_amount(total_m.group(2)),
                "wash_sale": _parse_amount(total_m.group(3).replace("--", "0")),
                "gain_loss": _parse_amount(total_m.group(4)),
            }
        return summary

    def _parse_dividend_detail(self, text: str) -> list:
        """Parse Detail Information of Dividends and Distributions from Year-End Summary."""
        dividends = []
        section = _extract_section_rfind(text, "Detail Information of Dividends", "Detail Information of Interest")
        if not section:
            return dividends

        # Pattern: DESCRIPTION SYMBOL CUSIP $ PAID_2025 $ ADJUSTED $ AMOUNT
        div_pattern = re.compile(
            r'(.+?)\s+(\w+)\s+(\d{9})\s+\$\s*([\d,.]+)\s+\$\s*\(?([\d,.]+)\)?\s+\$\s*([\d,.]+)'
        )
        current_category = "unknown"
        for line in section.split('\n'):
            if "Non-Qualified" in line:
                current_category = "non_qualified"
            elif "Qualified" in line and "Total" not in line:
                current_category = "qualified"

            m = div_pattern.search(line)
            if m:
                dividends.append({
                    "description": m.group(1).strip(),
                    "symbol": m.group(2),
                    "cusip": m.group(3),
                    "paid_in_year": _parse_amount(m.group(4)),
                    "adjusted": _parse_signed_amount(m.group(5)),
                    "amount": _parse_amount(m.group(6)),
                    "category": current_category,
                })
        return dividends

    def _parse_interest_detail(self, text: str) -> list:
        """Parse Detail Information of Interest Income from Year-End Summary."""
        interests = []
        section = _extract_section_rfind(text, "Detail Information of Interest", "REALIZED GAIN")
        if not section:
            return interests

        pattern = re.compile(r'(.+?)\s+\$\s*([\d,.]+)\s+\$\s*([\d,.]+)\s+\$\s*([\d,.]+)')
        for m in pattern.finditer(section):
            desc = m.group(1).strip()
            if desc and "Total" not in desc and "Description" not in desc:
                interests.append({
                    "description": desc,
                    "paid_in_year": _parse_amount(m.group(2)),
                    "adjusted": _parse_amount(m.group(3)),
                    "amount": _parse_amount(m.group(4)),
                })
        return interests


def _extract_section(text: str, start_marker: str, end_marker: str) -> str:
    start = text.find(start_marker)
    if start < 0:
        return ""
    end = text.find(end_marker, start + len(start_marker))
    if end < 0:
        end = start + 5000
    return text[start:end]


_DESC_SYMBOL_MAP = {
    "ADVANCED MICRO": "AMD", "ALPHABET": "GOOGL", "APPLE": "AAPL",
    "BARRICK GOLD": "GOLD", "BNK OF MON MCRSCT FNG": "FNGA",
    "MICROSECTORS FANG": "FNGU", "INVESCO NASDAQ 100": "QQQM",
    "INVESCO QQQ": "QQQ", "ISHARES BITCOIN": "IBIT",
    "ISHARES NASDAQ TOP": "QTOP", "NVIDIA": "NVDA",
    "PROSHARES ULTRAPRO QQQ": "TQQQ", "RIVIAN": "RIVN",
    "TESLA": "TSLA", "UNITEDHEALTH": "UNH", "META PLATFORMS": "META",
    "MICROSOFT": "MSFT", "SPDR GOLD": "GLD",
}


def _symbol_from_description(desc: str) -> str | None:
    desc_upper = desc.upper()
    for key, sym in _DESC_SYMBOL_MAP.items():
        if key in desc_upper:
            return sym
    return None


def _extract_section_rfind(text: str, start_marker: str, end_marker: str) -> str:
    start = text.rfind(start_marker)
    if start < 0:
        return ""
    end = text.find(end_marker, start + len(start_marker))
    if end < 0:
        end = start + 5000
    return text[start:end]


def _parse_amount(s: str) -> float:
    if not s:
        return 0.0
    return float(re.sub(r'[^0-9.]', '', s) or "0")


def _parse_signed_amount(s: str) -> float:
    if not s:
        return 0.0
    s = s.strip()
    negative = "(" in s or s.startswith("-")
    val = float(re.sub(r'[^0-9.]', '', s) or "0")
    return -val if negative else val


def _normalize_date(d: str) -> str:
    """Convert MM/DD/YY to YYYY-MM-DD."""
    if not d or d == "VARIOUS":
        return d
    parts = d.split("/")
    if len(parts) == 3 and len(parts[2]) == 2:
        year = int(parts[2])
        year = year + 2000 if year < 50 else year + 1900
        return f"{year}-{parts[0]}-{parts[1]}"
    return d


def _normalize_date_full(d: str) -> str:
    """Convert MM/DD/YY (6 chars no separator sometimes) to YYYY-MM-DD."""
    if not d or d == "VARIOUS":
        return d
    if len(d) == 8 and "/" in d:
        return _normalize_date(d)
    # Handle MMDDYY format (no separators between dates)
    if len(d) == 6 and d.isdigit():
        mm, dd, yy = d[:2], d[2:4], d[4:6]
        year = int(yy) + 2000 if int(yy) < 50 else int(yy) + 1900
        return f"{year}-{mm}-{dd}"
    return _normalize_date(d)

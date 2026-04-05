import logging
import re
import pdfplumber
from parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)

# Lazy imports for OCR — only needed for image-based PDFs
_ocr_available = None


def _ensure_ocr():
    global _ocr_available
    if _ocr_available is None:
        try:
            import pytesseract  # noqa: F401
            from pdf2image import convert_from_path  # noqa: F401
            _ocr_available = True
        except ImportError:
            _ocr_available = False
    return _ocr_available


def _ocr_pdf(file_path: str, dpi: int = 300) -> str:
    from pdf2image import convert_from_path
    import pytesseract
    images = convert_from_path(file_path, dpi=dpi)
    pages = []
    for img in images:
        pages.append(pytesseract.image_to_string(img))
    return "\n".join(pages)


class Robinhood1099BParser(BaseParser):
    def parse(self, file_path: str, metadata: dict) -> dict:
        logger.info("Parsing Robinhood 1099-B: %s", file_path)
        full_text = self._extract_text(file_path)
        fmt = self._detect_format(full_text)
        logger.info("Detected Robinhood format: %s", fmt)
        return {
            "full_text": full_text,
            "metadata": metadata,
            "format": fmt,
            "dividends": self._parse_1099_div(full_text),
            "interest": self._parse_1099_int(full_text),
            "transactions": self._parse_transactions(full_text, fmt),
            "summary": self._parse_summary(full_text, fmt),
        }

    def to_canonical(self, raw_data: dict, mapping: dict, doc_meta: dict) -> dict:
        doc_meta["account_number_masked"] = self._extract_masked_account(raw_data["full_text"])
        doc_meta["tax_year"] = self._extract_tax_year(raw_data["full_text"])
        doc_meta["parse_status"] = "success"
        doc_meta["source_format"] = f"robinhood_{raw_data['format']}"

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

    def _extract_text(self, file_path: str) -> str:
        with pdfplumber.open(file_path) as pdf:
            text = ""
            for page in pdf.pages:
                text += (page.extract_text() or "") + "\n"
        if len(text.strip()) > 200:
            return text
        logger.info("Text extraction yielded minimal content, falling back to OCR")
        if not _ensure_ocr():
            raise RuntimeError("OCR packages (pytesseract, pdf2image) not available for image-based PDF")
        return _ocr_pdf(file_path)

    def _detect_format(self, text: str) -> str:
        if "APEX CLEARING" in text.upper():
            return "apex"
        return "robinhood_securities"

    def _extract_masked_account(self, text: str) -> str:
        m = re.search(r'Account\s+(?:No:?\s*)?(\w+)', text)
        return f"****-{m.group(1)[-4:]}" if m else "unknown"

    def _extract_tax_year(self, text: str) -> str:
        m = re.search(r'(?:CONSOLIDATED\s+)?(\d{4})\s+(?:FORMS?\s+1099|Tax Information)', text)
        if m:
            return m.group(1)
        m = re.search(r'Tax Information Statement.*?(\d{4})\s*$', text[:500], re.MULTILINE)
        return m.group(1) if m else "unknown"

    # --- 1099-DIV ---
    def _parse_1099_div(self, text: str) -> dict:
        div = {}
        patterns = {
            "total_ordinary_dividends": r'1a[-.]\s*Total ordinary dividends.*?(\d[\d,.]+)',
            "qualified_dividends": r'1b[-.]\s*Qualified dividends.*?(\d[\d,.]+)',
            "total_capital_gain_distributions": r'2a[-.]\s*Total capital gain.*?(\d[\d,.]+)',
            "nondividend_distributions": r'3[-.]\s*Nondividend distributions.*?(\d[\d,.]+)',
            "federal_tax_withheld": r'4[-.]\s*Federal income tax withheld.*?(\d[\d,.]+)',
            "foreign_tax_paid": r'7[-.]\s*Foreign tax paid.*?(\d[\d,.]+)',
        }
        for key, pattern in patterns.items():
            m = re.search(pattern, text, re.IGNORECASE)
            div[key] = _parse_amount(m.group(1)) if m else 0.0
        return div

    # --- 1099-INT ---
    def _parse_1099_int(self, text: str) -> dict:
        interest = {}
        patterns = {
            "interest_income": r'1[-.]\s*Interest income.*?(\d[\d,.]+)',
            "us_savings_bond_interest": r'3[-.]\s*Interest on US Savings.*?(\d[\d,.]+)',
            "federal_tax_withheld": r'4[-.]\s*Federal income tax withheld.*?(\d[\d,.]+)',
            "tax_exempt_interest": r'8[-.]\s*Tax-exempt interest.*?(\d[\d,.]+)',
        }
        section = re.search(r'INTEREST INCOME.*?(?=ORIGINAL ISSUE|SECTION 1256|$)', text, re.DOTALL | re.IGNORECASE)
        block = section.group(0) if section else ""
        for key, pattern in patterns.items():
            m = re.search(pattern, block, re.IGNORECASE)
            interest[key] = _parse_amount(m.group(1)) if m else 0.0
        return interest

    # --- Transactions ---
    def _parse_transactions(self, text: str, fmt: str) -> list:
        if fmt == "apex":
            return self._parse_apex_transactions(text)
        return self._parse_rh_securities_transactions(text)

    def _parse_apex_transactions(self, text: str) -> list:
        transactions = []
        current_section = "short_term_covered"
        current_description = None
        current_cusip = None

        for line in text.split("\n"):
            line = line.strip()
            ll = line.upper()

            # Section detection
            if "COVERED SHORT-TERM" in ll or ("SHORT-TERM" in ll and "BOX A" in ll) or ("SHORT TERM" in ll and "COVERED" in ll and "BOX A" in ll):
                current_section = "short_term_covered"
                continue
            if ("SHORT-TERM" in ll or "SHORT TERM" in ll) and ("NONCOVERED" in ll or "NOT REPORTED" in ll or "BOX B" in ll):
                current_section = "short_term_noncovered"
                continue
            if "COVERED LONG-TERM" in ll or ("LONG-TERM" in ll and "BOX D" in ll) or ("LONG TERM" in ll and "COVERED" in ll and "BOX D" in ll):
                current_section = "long_term_covered"
                continue
            if ("LONG-TERM" in ll or "LONG TERM" in ll) and ("NONCOVERED" in ll or "NOT REPORTED" in ll or "BOX E" in ll):
                current_section = "long_term_noncovered"
                continue

            # CUSIP line
            cusip_m = re.search(r'CUSIP:\s*([A-Z0-9]{8,9})', line)
            if cusip_m:
                current_cusip = cusip_m.group(1)

            # Description: all-caps text before the first transaction number on the line
            desc_m = re.match(r'^([A-Z][A-Z\s,.\'\-&]+?)\s+\d+\.\d+\s+\d{2}/\d{2}/\d{4}', line)
            if desc_m:
                current_description = desc_m.group(1).strip()

            # Full transaction: QTY DATE_ACQ DATE_SOLD $PROCEEDS $COST $MKTDISC $WASH (GAIN)
            txn_m = re.search(
                r'(\d[\d,.]+)\s+'
                r'(\d{2}/\d{2}/\d{4})\s+'
                r'(\d{2}/\d{2}/\d{4})\s+'
                r'\$?([\d,.]+)\s+'
                r'\$?([\d,.]+)\s+'
                r'\$?([\d,.]+)\s+'
                r'\$?([\d,.]+)\s+'
                r'(.*)',
                line
            )
            if txn_m:
                gain_str = txn_m.group(8).strip()
                gain_m = re.search(r'[($]*(\d[\d,.]*)', gain_str)
                gain = _parse_amount(gain_m.group(1)) if gain_m else 0.0
                if "(" in gain_str:
                    gain = -gain
                transactions.append({
                    "description": current_description or "",
                    "cusip": current_cusip or "",
                    "quantity": _parse_amount(txn_m.group(1)),
                    "date_acquired": _normalize_date(txn_m.group(2)),
                    "date_sold": _normalize_date(txn_m.group(3)),
                    "proceeds": _parse_amount(txn_m.group(4)),
                    "cost_basis": _parse_amount(txn_m.group(5)),
                    "market_discount": _parse_amount(txn_m.group(6)),
                    "wash_sale_loss_disallowed": _parse_amount(txn_m.group(7)),
                    "realized_gain_loss": gain,
                    "section": current_section,
                    "holding_period": "LONG_TERM" if "long_term" in current_section else "SHORT_TERM",
                })
                continue

            # Truncated OCR line: QTY DATE_ACQ DATE_SOLD $PROCEEDS $COST $MKTDISC (wash+gain cut off)
            trunc_m = re.search(
                r'(\d[\d,.]+)\s+'
                r'(\d{2}/\d{2}/\d{4})\s+'
                r'(\d{2}/\d{2}/\d{4})\s+'
                r'\$?([\d,.]+)\s+'
                r'\$?([\d,.]+)\s+'
                r'\$?([\d,.]+)\s*$',
                line
            )
            if trunc_m:
                proceeds = _parse_amount(trunc_m.group(4))
                cost = _parse_amount(trunc_m.group(5))
                transactions.append({
                    "description": current_description or "",
                    "cusip": current_cusip or "",
                    "quantity": _parse_amount(trunc_m.group(1)),
                    "date_acquired": _normalize_date(trunc_m.group(2)),
                    "date_sold": _normalize_date(trunc_m.group(3)),
                    "proceeds": proceeds,
                    "cost_basis": cost,
                    "market_discount": _parse_amount(trunc_m.group(6)),
                    "wash_sale_loss_disallowed": 0.0,
                    "realized_gain_loss": round(proceeds - cost, 2),
                    "section": current_section,
                    "holding_period": "LONG_TERM" if "long_term" in current_section else "SHORT_TERM",
                })

        return transactions

    def _parse_rh_securities_transactions(self, text: str) -> list:
        transactions = []
        current_section = "short_term_covered"
        current_description = None
        current_cusip = None
        pending_date_sold = None
        _in_grouped_block = False

        lines = text.split("\n")
        for i, line in enumerate(lines):
            line = line.strip()
            ll = line.lower()

            # Section detection
            if "short term" in ll and "covered" in ll:
                if "noncovered" in ll:
                    current_section = "short_term_noncovered"
                else:
                    current_section = "short_term_covered"
                continue
            if "long term" in ll and "covered" in ll:
                if "noncovered" in ll:
                    current_section = "long_term_noncovered"
                else:
                    current_section = "long_term_covered"
                continue

            # Security header: DESCRIPTION / CUSIP: XXXXXXXXX / Symbol:
            sec_m = re.match(r'^(.+?)\s*/\s*CUSIP:\s*(\w*)\s*/\s*Symbol:', line)
            if sec_m:
                current_description = sec_m.group(1).strip()
                current_cusip = sec_m.group(2) or ""
                continue

            # "N transactions for DATE" — captures date_sold for sub-transactions
            group_m = re.match(r'\d+ transactions for (\d{2}/\d{2}/\d{2})', line)
            if group_m:
                pending_date_sold = group_m.group(1)
                _in_grouped_block = True
                continue

            # Skip summary lines
            if line.startswith("Securitytotal:") or line.startswith("Totals:"):
                continue

            # "Total of N transactions" lines: skip if individual lots were listed
            # (Robinhood pattern: preceded by "N transactions for DATE" block),
            # but capture if lots are NOT listed (Ameritrade pattern: standalone grouped total).
            if "Total of" in line and "transactions" in line:
                if _in_grouped_block:
                    _in_grouped_block = False
                    pending_date_sold = None
                    continue
                # Fall through to txn_m regex below — line has valid proceeds/cost/GL

            # Format 1: DATE_SOLD QTY PROCEEDS DATE_ACQ COST REST
            # Also matches "Total of N transactions" lines (date_acquired = Various)
            txn_m = re.match(
                r'(\d{2}/\d{2}/\d{2})\s+'
                r'([\d,.]+)\s+'
                r'(-?[\d,.]+)\s+'
                r'(\d{2}/\d{2}/\d{2}|Various)\s+'
                r'(-?[\d,.]+)\s+'
                r'(.*)',
                line
            )
            if txn_m:
                rest = txn_m.group(6)
                wash, gain = self._parse_rest(rest)
                transactions.append({
                    "description": current_description or "",
                    "cusip": current_cusip or "",
                    "quantity": _parse_amount(txn_m.group(2)),
                    "date_acquired": _normalize_date(txn_m.group(4)),
                    "date_sold": _normalize_date(txn_m.group(1)),
                    "proceeds": _parse_amount(txn_m.group(3)),
                    "cost_basis": _parse_amount(txn_m.group(5)),
                    "market_discount": 0.0,
                    "wash_sale_loss_disallowed": wash,
                    "realized_gain_loss": gain,
                    "section": current_section,
                    "holding_period": "LONG_TERM" if "long_term" in current_section else "SHORT_TERM",
                })
                pending_date_sold = None
                continue

            # Format 2: QTY PROCEEDS DATE_ACQ COST REST (sub-transaction, no date_sold)
            sub_m = re.match(
                r'([\d,.]+)\s+'
                r'([\d,.]+)\s+'
                r'(\d{2}/\d{2}/\d{2})\s+'
                r'([\d,.]+)\s+'
                r'(.*)',
                line
            )
            if sub_m and pending_date_sold and ("Sale" in line or "Option" in line or "..." in line or " W " in line):
                rest = sub_m.group(5)
                wash, gain = self._parse_rest(rest)
                transactions.append({
                    "description": current_description or "",
                    "cusip": current_cusip or "",
                    "quantity": _parse_amount(sub_m.group(1)),
                    "date_acquired": _normalize_date(sub_m.group(3)),
                    "date_sold": _normalize_date(pending_date_sold),
                    "proceeds": _parse_amount(sub_m.group(2)),
                    "cost_basis": _parse_amount(sub_m.group(4)),
                    "market_discount": 0.0,
                    "wash_sale_loss_disallowed": wash,
                    "realized_gain_loss": gain,
                    "section": current_section,
                    "holding_period": "LONG_TERM" if "long_term" in current_section else "SHORT_TERM",
                })

        return transactions

    @staticmethod
    def _parse_rest(rest: str) -> tuple:
        """Parse the rest of a transaction line for wash sale and gain/loss."""
        rest_clean = rest.replace("...", "").strip()
        wash = 0.0
        gain = 0.0
        if " W " in rest_clean:
            parts = re.split(r'\s+W\s+', rest_clean, maxsplit=1)
            wash_nums = re.findall(r'-?[\d,.]+', parts[0])
            if wash_nums:
                wash = _parse_amount(wash_nums[-1])
            gain_nums = re.findall(r'-?[\d,.]+', parts[1]) if len(parts) > 1 else []
            if gain_nums:
                gain = _parse_signed(gain_nums[0])
        else:
            nums = re.findall(r'-?[\d,.]+', rest_clean)
            if nums:
                gain = _parse_signed(nums[0])
        return wash, gain

    # --- Summary ---
    def _parse_summary(self, text: str, fmt: str) -> list:
        if fmt == "apex":
            return self._parse_apex_summary(text)
        return self._parse_rh_securities_summary(text)

    def _parse_apex_summary(self, text: str) -> list:
        summary = []
        section = re.search(r'REALIZED GAIN\s*/?\s*LOSS SUMMARY.*?(?=Page \d|RECIPIENT|$)', text, re.DOTALL | re.IGNORECASE)
        if not section:
            return summary
        block = section.group(0)

        categories = [
            ("short_term_box_a", r'Box A \(basis reported to IRS\)\s+\$?([\d,.]+)\s+\$?([\d,.]+)\s+\$?([\d,.]+)\s+\$?([\d,.]+)\s+[($]*([\d,.]+)\)?'),
            ("short_term_box_b", r'Box B \(basis not reported to IRS\)\s+\$?([\d,.]+)\s+\$?([\d,.]+)\s+\$?([\d,.]+)\s+\$?([\d,.]+)\s+[($]*([\d,.]+)\)?'),
            ("total_short_term", r'Total Short-Term\s+\$?([\d,.]+)\s+\$?([\d,.]+)\s+\$?([\d,.]+)\s+\$?([\d,.]+)\s+[($]*([\d,.]+)\)?'),
            ("long_term_box_d", r'Box D \(basis reported to IRS\)\s+\$?([\d,.]+)\s+\$?([\d,.]+)\s+\$?([\d,.]+)\s+\$?([\d,.]+)\s+[($]*([\d,.]+)\)?'),
            ("long_term_box_e", r'Box E \(basis not reported to IRS\)\s+\$?([\d,.]+)\s+\$?([\d,.]+)\s+\$?([\d,.]+)\s+\$?([\d,.]+)\s+[($]*([\d,.]+)\)?'),
            ("total_long_term", r'Total Long-Term\s+\$?([\d,.]+)\s+\$?([\d,.]+)\s+\$?([\d,.]+)\s+\$?([\d,.]+)\s+[($]*([\d,.]+)\)?'),
        ]
        for cat, pattern in categories:
            m = re.search(pattern, block)
            if m:
                gain = _parse_amount(m.group(5))
                # Check for parentheses indicating negative
                full_match = m.group(0)
                if "(" in full_match.split(m.group(4))[-1]:
                    gain = -gain
                summary.append({
                    "category": cat,
                    "proceeds": _parse_amount(m.group(1)),
                    "cost_basis": _parse_amount(m.group(2)),
                    "market_discount": _parse_amount(m.group(3)),
                    "wash_sale": _parse_amount(m.group(4)),
                    "gain_loss": gain,
                })
        return summary

    def _parse_rh_securities_summary(self, text: str) -> list:
        summary = []
        # Robinhood Securities format has a single Totals line per section
        # Find all "Totals:" lines
        for line in text.split("\n"):
            if line.strip().startswith("Totals:"):
                nums = re.findall(r'-?[\d,.]+', line)
                if len(nums) >= 4:
                    gain = _parse_signed(nums[3]) if len(nums) > 3 else 0.0
                    wash = _parse_amount(nums[2]) if " W " in line else 0.0
                    if " W " not in line and len(nums) >= 3:
                        gain = _parse_signed(nums[2])
                        wash = 0.0
                    summary.append({
                        "category": "total",
                        "proceeds": _parse_amount(nums[0]),
                        "cost_basis": _parse_amount(nums[1]),
                        "market_discount": 0.0,
                        "wash_sale": wash,
                        "gain_loss": gain,
                    })
        return summary

    # --- Cross-validation ---
    def _cross_validate(self, transactions: list, summary: list) -> list:
        warnings = []
        if not transactions or not summary:
            if summary and not transactions:
                warnings.append("Summary found but no transactions parsed — possible parsing issue")
            return warnings

        calc_proceeds = round(sum(t["proceeds"] for t in transactions), 2)
        calc_cost = round(sum(t["cost_basis"] for t in transactions), 2)
        calc_wash = round(sum(t["wash_sale_loss_disallowed"] for t in transactions), 2)
        calc_gain = round(sum(t["realized_gain_loss"] for t in transactions), 2)

        # Find the total summary row
        total_row = None
        for s in summary:
            if "total" in s["category"].lower() and "short" not in s["category"] and "long" not in s["category"]:
                total_row = s
                break
        if not total_row:
            # Sum all summary rows that aren't sub-totals
            total_row = {
                "proceeds": sum(s["proceeds"] for s in summary if "total" in s["category"]),
                "cost_basis": sum(s["cost_basis"] for s in summary if "total" in s["category"]),
                "wash_sale": sum(s["wash_sale"] for s in summary if "total" in s["category"]),
                "gain_loss": sum(s["gain_loss"] for s in summary if "total" in s["category"]),
            }

        tolerance = 0.05
        for field, calc_val, sum_key in [
            ("proceeds", calc_proceeds, "proceeds"),
            ("cost_basis", calc_cost, "cost_basis"),
            ("wash_sale", calc_wash, "wash_sale"),
            ("gain_loss", calc_gain, "gain_loss"),
        ]:
            expected = total_row.get(sum_key, 0.0)
            if abs(calc_val - expected) > tolerance:
                warnings.append(
                    f"VALIDATION MISMATCH: {field} — parsed_sum={calc_val}, summary={expected}, diff={round(calc_val - expected, 2)}"
                )

        if not warnings:
            logger.info("Cross-validation PASSED: parsed transactions match summary totals")
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
    if len(parts) == 3:
        if len(parts[2]) == 2:
            year = int(parts[2])
            year = year + 2000 if year < 50 else year + 1900
            return f"{year}-{parts[0]}-{parts[1]}"
        elif len(parts[2]) == 4:
            return f"{parts[2]}-{parts[0]}-{parts[1]}"
    return d

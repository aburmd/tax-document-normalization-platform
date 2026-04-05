import logging
import re
import pdfplumber
from parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)


class Ameritrade1099BParser(BaseParser):
    """Parser for TD Ameritrade yearly Realized Capital Gains & Losses reports.

    These reports list every individual lot with wash sale adjustments,
    making them more complete than the 1099-B which groups lots behind
    'Total of N transactions' lines.
    """

    _REC_TYPES = r'(?:Short Sell |Short CoverBox |Short Expired |Expired |Short WS Adj |Wash Sale Adj )?'
    _TXN_RE = re.compile(
        r'^(\d{2}/\d{2}/\d{4})'
        r'(' + _REC_TYPES.strip() + r')'
        r'\s*(\d{2}/\d{2}/\d{4})'
        r'(.+?)\s+US\s+'
        r'([\d,]+)\s+'
        r'(-?[\d,.]+)\s+'
        r'(-?[\d,.]+)\s+'
        r'(.*?)'
        r'(\d{2}/\d{2}/\d{4})\s*$'
    )

    def parse(self, file_path: str, metadata: dict) -> dict:
        logger.info("Parsing Ameritrade Realized G/L: %s", file_path)
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
        doc_meta["source_format"] = "ameritrade_realized_gl"
        summary = raw_data["summary"]
        warnings = self._cross_validate(raw_data["transactions"], summary)
        return {
            "document_metadata": doc_meta,
            "dividends_1099div": [],
            "interest_1099int": [],
            "transactions_1099b": raw_data["transactions"],
            "realized_gain_loss_summary": [summary] if summary else [],
            "realized_gain_loss_detail": [],
            "positions": [],
            "transfers": [],
            "rsu_events": [],
            "warnings": warnings,
        }

    @staticmethod
    def _extract_account(text: str) -> str:
        m = re.search(r'Account:\s*(\d+)', text)
        return f"****-{m.group(1)[-4:]}" if m else "unknown"

    @staticmethod
    def _extract_tax_year(text: str) -> str:
        m = re.search(r'(\d{4})\s+Realized Capital Gains', text)
        return m.group(1) if m else "unknown"

    def _parse_summary(self, text: str) -> dict:
        s = {}
        for key, pattern in [
            ("st_gain", r'Short Term Gain\s+([\d,.]+)'),
            ("st_loss", r'Short Term Loss\s+(-[\d,.]+)'),
            ("st_net", r'Short Term Net\s+(-?[\d,.]+)'),
            ("st_sales", r'Short Term Sales\s+([\d,.]+)'),
            ("st_cost", r'Short Term Cost\s+([\d,.]+)'),
            ("lt_gain", r'Long Term Gain\s+([\d,.]+)'),
            ("lt_loss", r'Long Term Loss\s+(-[\d,.]+)'),
            ("lt_net", r'Long Term Net\s+(-?[\d,.]+)'),
            ("lt_sales", r'Long Term Sales\s+([\d,.]+)'),
            ("lt_cost", r'Long Term Cost\s+([\d,.]+)'),
        ]:
            m = re.search(pattern, text)
            s[key] = _parse_signed(m.group(1)) if m else 0.0
        s["category"] = "total"
        s["proceeds"] = s["st_sales"] + s["lt_sales"]
        s["cost_basis"] = s["st_cost"] + s["lt_cost"]
        s["gain_loss"] = s["st_net"] + s["lt_net"]
        return s

    def _parse_transactions(self, text: str) -> list:
        transactions = []
        for line in text.split('\n'):
            ls = line.strip()
            m = self._TXN_RE.match(ls)
            if not m:
                continue

            close_date = m.group(1)
            rec_type = m.group(2).strip()
            open_date = m.group(3)
            security = m.group(4).strip()
            shares = int(m.group(5).replace(',', ''))
            proceeds = _parse_amount(m.group(6))
            book_cost = _parse_amount(m.group(7))
            middle = m.group(8).strip()
            settle_date = m.group(9)

            # Wash sale adjustment lines adjust the cost basis of a subsequent transaction.
            # They have proceeds=0, negative cost, and positive GL that offsets the WS cost.
            # Include them as transactions since they contribute to the total GL.
            is_wash_adj = rec_type in ('Wash Sale Adj', 'Short WS Adj')

            nums = [_parse_signed(n) for n in re.findall(r'-?[\d,.]+', middle)]

            if is_wash_adj:
                # Wash adj: middle has just the GL adjustment amount
                ws_gl = nums[0] if nums else 0.0
                transactions.append({
                    "symbol": symbol,
                    "description": security,
                    "quantity": shares,
                    "date_acquired": _normalize_date(open_date),
                    "date_sold": _normalize_date(close_date),
                    "proceeds": proceeds,
                    "cost_basis": book_cost,
                    "wash_sale_cost": 0.0,
                    "adjusted_cost_basis": book_cost,
                    "realized_gain_loss": ws_gl,
                    "st_gain_loss": ws_gl,
                    "lt_gain_loss": 0.0,
                    "rec_type": rec_type,
                    "section": "short_term_covered",
                    "holding_period": "SHORT_TERM",
                    "settle_date": _normalize_date(settle_date),
                })
                continue

            ws_cost = 0.0
            adj_cost = book_cost
            st_gl = 0.0
            lt_gl = 0.0

            if len(nums) == 1:
                st_gl = nums[0]
            elif len(nums) == 2:
                if nums[0] < 0:
                    # Both negative: Section 1256 split (st_gl, lt_gl)
                    st_gl = nums[0]
                    lt_gl = nums[1]
                else:
                    adj_cost = nums[0]
                    st_gl = nums[1]
            elif len(nums) == 3:
                # Distinguish: WS (ws_cost, adj_cost, st_gl) vs Sec1256 (adj_cost, st_gl, lt_gl)
                if abs(nums[0] + nums[1] - book_cost) < 0.02 or abs(nums[1] - (book_cost + nums[0])) < 0.02:
                    # WS pattern: book_cost + ws_cost ≈ adj_cost
                    ws_cost = nums[0]
                    adj_cost = nums[1]
                    st_gl = nums[2]
                else:
                    adj_cost = nums[0]
                    st_gl = nums[1]
                    lt_gl = nums[2]

            symbol = self._extract_symbol(security)
            holding = "LONG_TERM" if lt_gl != 0 and st_gl == 0 else "SHORT_TERM"

            transactions.append({
                "symbol": symbol,
                "description": security,
                "quantity": shares,
                "date_acquired": _normalize_date(open_date),
                "date_sold": _normalize_date(close_date),
                "proceeds": proceeds,
                "cost_basis": book_cost,
                "wash_sale_cost": ws_cost,
                "adjusted_cost_basis": adj_cost,
                "realized_gain_loss": st_gl + lt_gl,
                "st_gain_loss": st_gl,
                "lt_gain_loss": lt_gl,
                "rec_type": rec_type or "Sale",
                "section": "short_term_covered" if holding == "SHORT_TERM" else "long_term_covered",
                "holding_period": holding,
                "settle_date": _normalize_date(settle_date),
            })

        return transactions

    @staticmethod
    def _extract_symbol(security: str) -> str:
        # Stock: "ABBOTT LABORATORIES (ABT)" -> ABT
        m = re.search(r'\(([A-Z]+)\)', security)
        if m:
            return m.group(1)
        # Option: "AAPL Aug 11 2023 182.5 Put" -> AAPL
        m = re.match(r'([A-Z]+)\s+', security)
        return m.group(1) if m else security.split()[0] if security else "UNKNOWN"

    def _cross_validate(self, transactions: list, summary: dict) -> list:
        warnings = []
        if not transactions or not summary:
            return warnings
        calc_gl = round(sum(t["realized_gain_loss"] for t in transactions), 2)
        expected = round(summary.get("gain_loss", 0), 2)
        if abs(calc_gl - expected) > 0.10:
            warnings.append(
                f"GL mismatch: parsed={calc_gl}, summary={expected}, diff={calc_gl - expected:.2f}"
            )
        else:
            logger.info("Cross-validation passed: GL=%.2f, %d transactions", calc_gl, len(transactions))
        return warnings


def _parse_amount(s: str) -> float:
    if not s or s == '--':
        return 0.0
    return float(re.sub(r'[^0-9.]', '', s) or '0')


def _parse_signed(s: str) -> float:
    if not s or s == '--':
        return 0.0
    negative = s.strip().startswith('-')
    val = float(re.sub(r'[^0-9.]', '', s) or '0')
    return -val if negative else val


def _normalize_date(d: str) -> str:
    if not d:
        return d
    parts = d.split('/')
    if len(parts) == 3:
        if len(parts[2]) == 4:
            return f"{parts[2]}-{parts[0]}-{parts[1]}"
        elif len(parts[2]) == 2:
            year = int(parts[2])
            year = year + 2000 if year < 50 else year + 1900
            return f"{year}-{parts[0]}-{parts[1]}"
    return d

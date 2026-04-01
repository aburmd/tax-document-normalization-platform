import logging
import re
import pdfplumber
from parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)


class SchwabStatementParser(BaseParser):
    def parse(self, file_path: str, metadata: dict) -> dict:
        logger.info("Parsing Schwab monthly statement: %s", file_path)
        full_text = ""
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                full_text += (page.extract_text() or "") + "\n"
        return {
            "full_text": full_text,
            "metadata": metadata,
            "account_summary": self._parse_account_summary(full_text),
            "positions": self._parse_positions(full_text),
            "transactions": self._parse_transactions(full_text),
            "bank_sweep": self._parse_bank_sweep(full_text),
            "pending": self._parse_pending(full_text),
            "income_summary": self._parse_income_summary(full_text),
            "gain_loss_summary": self._parse_gain_loss_summary(full_text),
        }

    def to_canonical(self, raw_data: dict, mapping: dict, doc_meta: dict) -> dict:
        doc_meta["account_number_masked"] = self._extract_masked_account(raw_data["full_text"])
        period = self._extract_period(raw_data["full_text"])
        doc_meta["statement_period_start"] = period.get("start")
        doc_meta["statement_period_end"] = period.get("end")
        doc_meta["parse_status"] = "success"
        doc_meta["source_format"] = "schwab_monthly_statement"

        return {
            "document_metadata": doc_meta,
            "account_summary": raw_data["account_summary"],
            "transactions": raw_data["transactions"],
            "positions": raw_data["positions"],
            "bank_sweep": raw_data["bank_sweep"],
            "pending": raw_data["pending"],
            "income_summary": raw_data["income_summary"],
            "gain_loss_summary": raw_data["gain_loss_summary"],
            "transfers": [],
            "rsu_events": [],
            "warnings": [],
        }

    def _extract_masked_account(self, text: str) -> str:
        m = re.search(r'(\d{4})-(\d{4})', text)
        if m:
            return f"****-{m.group(2)}"
        return "unknown"

    def _extract_period(self, text: str) -> dict:
        m = re.search(r'StatementPeriod\s*\n.*?(\w+)\s*(\d+)\s*[-–]\s*(\d+)\s*,\s*(\d{4})', text)
        if not m:
            m = re.search(r'(\w+)\s*(\d+)\s*[-–]\s*(\d+)\s*,\s*(\d{4})', text)
        if m:
            month_str, day_start, day_end, year = m.group(1), m.group(2), m.group(3), m.group(4)
            month_num = _month_to_num(month_str)
            if month_num:
                return {
                    "start": f"{year}-{month_num:02d}-{int(day_start):02d}",
                    "end": f"{year}-{month_num:02d}-{int(day_end):02d}",
                }
        return {"start": None, "end": None}

    def _parse_account_summary(self, text: str) -> dict:
        summary = {}
        m = re.search(r'EndingAccountValueasof(\d{2}/\d{2})\s*BeginningAccountValueasof(\d{2}/\d{2})', text)
        if m:
            summary["ending_date"] = m.group(1)
            summary["beginning_date"] = m.group(2)
        vals = re.search(r'\$([0-9,.]+)\s+\$([0-9,.]+)', text[:500])
        if vals:
            summary["ending_value"] = _parse_amount(vals.group(1))
            summary["beginning_value"] = _parse_amount(vals.group(2))

        for label, key in [
            ("Deposits", "deposits"),
            ("Withdrawals", "withdrawals"),
            ("DividendsandInterest", "dividends_and_interest"),
            ("TransferofSecurities", "transfer_of_securities"),
            ("Expenses", "expenses"),
        ]:
            m = re.search(rf'{label}\s+([($0-9,.)\-]+)\s+([($0-9,.)\-]+)', text)
            if m:
                summary[f"{key}_period"] = _parse_signed_amount(m.group(1))
                summary[f"{key}_ytd"] = _parse_signed_amount(m.group(2))
        return summary

    def _parse_positions(self, text: str) -> list:
        positions = []
        # Cash position
        cash_m = re.search(
            r'BankSweep\s+(\S+)\s+([\d,.]+)\s+([\d,.]+)\s+\(?([\d,.]+)\)?',
            text
        )
        if cash_m:
            positions.append({
                "type": "cash",
                "symbol": "CASH_SWEEP",
                "description": "CHARLES SCHWAB BANK",
                "beginning_balance": _parse_amount(cash_m.group(2)),
                "ending_balance": _parse_amount(cash_m.group(3)),
                "change_in_period": _parse_signed_amount(f"({cash_m.group(4)})") if "(" in text[cash_m.start():cash_m.end()+5] else _parse_amount(cash_m.group(4)),
            })

        # ETF/Stock positions — format: SYMBOL DESCRIPTION, QTY PRICE MARKET COST (GAIN) YIELD INCOME PCT%
        etf_pattern = re.compile(
            r'^([A-Z]{2,5})\s+'           # symbol (2-5 uppercase letters)
            r'(\S.*?),\s+'               # description (ends with comma)
            r'([\d,.]+)\s+'              # quantity
            r'([\d,.]+)\s+'              # price
            r'([\d,.]+)\s+'              # market value
            r'([\d,.]+)\s+'              # cost basis
            r'\(?([\d,.]+)\)?\s+'        # unrealized gain/loss
            r'(?:N/A|[\d,.]+%)\s+'       # yield
            r'(?:N/A|[\d,.]+)\s+'        # income
            r'(\d+)%',                    # pct of account
            re.MULTILINE
        )
        for m in etf_pattern.finditer(text):
            positions.append({
                "type": "etf",
                "symbol": m.group(1),
                "description": m.group(2).strip(),
                "quantity": _parse_amount(m.group(3)),
                "price": _parse_amount(m.group(4)),
                "market_value": _parse_amount(m.group(5)),
                "cost_basis": _parse_amount(m.group(6)),
                "unrealized_gain_loss": _parse_signed_amount(m.group(7)),
                "pct_of_account": int(m.group(8)),
            })
        return positions

    def _parse_transactions(self, text: str) -> list:
        transactions = []
        # Find Transaction Details section
        txn_section = ""
        start = text.find("Transaction Details")
        if start >= 0:
            end = text.find("Bank Sweep Activity", start)
            if end < 0:
                end = text.find("Pending", start)
            if end < 0:
                end = start + 5000
            txn_section = text[start:end]

        # Pattern: MM/DD Category Action SYMBOL DESCRIPTION QTY PRICE AMOUNT
        # Some lines span multiple rows, so we match date-anchored lines
        txn_pattern = re.compile(
            r'(\d{2}/\d{2})\s+'
            r'(Withdrawal|Purchase|Sale|Interest|Dividend|Transfer|Journal|MoneyLinkTxn)\s*'
            r'(.*?)(?:\n|$)',
            re.IGNORECASE
        )
        for m in txn_pattern.finditer(txn_section):
            date = m.group(1)
            category = m.group(2).strip()
            rest = m.group(3).strip()
            txn = self._parse_txn_line(date, category, rest)
            if txn:
                transactions.append(txn)

        # Fallback: broader pattern
        if not transactions:
            line_pattern = re.compile(
                r'(\d{2}/\d{2})\s+(\w+)\s+(\w+)?\s*(.*?)\s+([\d,.]+)\s+([\d,.]+)\s+\(?([\d,.]+)\)?'
            )
            for m in line_pattern.finditer(txn_section):
                transactions.append({
                    "date": m.group(1),
                    "category": m.group(2),
                    "symbol": m.group(3),
                    "description": m.group(4).strip(),
                    "quantity": _parse_amount(m.group(5)),
                    "price": _parse_amount(m.group(6)),
                    "amount": _parse_signed_amount(m.group(7)),
                })
        return transactions

    def _parse_txn_line(self, date: str, category: str, rest: str) -> dict:
        txn = {"date": date, "category": category, "symbol": None, "description": "", "quantity": None, "price": None, "amount": None}

        if category.lower() in ("withdrawal", "moneylinktxn"):
            amt_m = re.search(r'\(?([\d,.]+)\)?', rest)
            txn["description"] = re.sub(r'[\d,.$()]+$', '', rest).strip()
            txn["amount"] = -_parse_amount(amt_m.group(1)) if amt_m else None
            txn["category"] = "Withdrawal"
            return txn

        if category.lower() == "interest":
            amt_m = re.search(r'([\d,.]+)$', rest)
            txn["description"] = re.sub(r'[\d,.]+$', '', rest).strip()
            txn["amount"] = _parse_amount(amt_m.group(1)) if amt_m else None
            return txn

        if category.lower() in ("purchase", "sale"):
            parts = rest.split()
            if parts:
                txn["symbol"] = parts[0]
                # Try to find qty, price, amount
                nums = re.findall(r'[\d,.]+', rest)
                if len(nums) >= 3:
                    txn["quantity"] = _parse_amount(nums[-3])
                    txn["price"] = _parse_amount(nums[-2])
                    txn["amount"] = _parse_signed_amount(nums[-1])
                    if "(" in rest:
                        txn["amount"] = -abs(txn["amount"])
                txn["description"] = " ".join(parts[1:]).split(str(txn["quantity"]) if txn["quantity"] else "XXX")[0].strip()
            return txn

        txn["description"] = rest
        return txn

    def _parse_bank_sweep(self, text: str) -> list:
        entries = []
        sweep_start = text.find("Bank Sweep Activity")
        if sweep_start < 0:
            return entries
        sweep_end = text.find("Pending", sweep_start)
        if sweep_end < 0:
            sweep_end = sweep_start + 3000
        sweep_section = text[sweep_start:sweep_end]

        pattern = re.compile(r'(\d{2}/\d{2})\s+(.*?)\s+\$?([\d,.]+(?:\.\d{2}))')
        for m in pattern.finditer(sweep_section):
            desc = m.group(2).strip()
            amount_str = m.group(3)
            # Check if negative (preceded by minus or in parens)
            pre_context = sweep_section[max(0, m.start()-2):m.start()]
            amount = _parse_amount(amount_str)
            if "(" in desc or "-" in pre_context:
                amount = -amount
            entries.append({"date": m.group(1), "description": desc, "amount": amount})
        return entries

    def _parse_pending(self, text: str) -> list:
        pending = []
        pend_start = text.find("Pending / Open Activity")
        if pend_start < 0:
            return pending
        pend_section = text[pend_start:pend_start + 2000]

        pattern = re.compile(
            r'(Pending|OpenOrders?)\s+(\d{2}/\d{2})\s+(\w+)\s+(\w+)\s+(.+?)\s+([\d,.]+)\s+([\d,.]+)'
        )
        for m in pattern.finditer(pend_section):
            pending.append({
                "type": m.group(1),
                "date": m.group(2),
                "action": m.group(3),
                "symbol": m.group(4),
                "description": m.group(5).strip(),
                "quantity": _parse_amount(m.group(6)),
                "price_or_amount": _parse_amount(m.group(7)),
            })
        return pending

    def _parse_income_summary(self, text: str) -> dict:
        summary = {}
        for label, key in [
            ("BankSweepInterest", "bank_sweep_interest"),
            ("TotalIncome", "total_income"),
        ]:
            m = re.search(rf'{label}\s+[\d.]+\s+([\d.]+)\s+[\d.]+\s+([\d.]+)', text)
            if m:
                summary[f"{key}_period_taxable"] = _parse_amount(m.group(1))
                summary[f"{key}_ytd_taxable"] = _parse_amount(m.group(2))
        return summary

    def _parse_gain_loss_summary(self, text: str) -> dict:
        summary = {}
        m = re.search(r'Unrealized\s+\(\$([\d,.]+)\)', text)
        if m:
            summary["unrealized_gain_loss"] = -_parse_amount(m.group(1))
        ytd_m = re.search(r'YTD\s+([\d,.]+)\s+([\d,.]+)', text)
        if ytd_m:
            summary["ytd_short_term"] = _parse_amount(ytd_m.group(1))
            summary["ytd_long_term"] = _parse_amount(ytd_m.group(2))
        return summary


def _month_to_num(month_str: str) -> int | None:
    months = {"january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
              "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12}
    return months.get(month_str.lower())


def _parse_amount(s: str) -> float:
    if not s:
        return 0.0
    cleaned = s.replace(",", "").replace("$", "").strip()
    if not cleaned:
        return 0.0
    return float(cleaned)


def _parse_signed_amount(s: str) -> float:
    if not s:
        return 0.0
    s = s.strip()
    negative = "(" in s or s.startswith("-")
    val = float(re.sub(r'[^0-9.]', '', s))
    return -val if negative else val

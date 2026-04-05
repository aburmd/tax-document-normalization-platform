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
        transactions = self._parse_1099b_transactions(full_text)
        rsu_lots = self._parse_rsu_supplemental(full_text)
        if rsu_lots:
            self._enrich_rsu_transactions(transactions, rsu_lots)
        return {
            "full_text": full_text,
            "metadata": metadata,
            "dividends": self._parse_1099_div(full_text),
            "summary": self._parse_1099b_summary(full_text),
            "transactions": transactions,
            "rsu_lots": rsu_lots,
        }

    def to_canonical(self, raw_data: dict, mapping: dict, doc_meta: dict) -> dict:
        doc_meta["account_number_masked"] = self._extract_masked_account(raw_data["full_text"])
        doc_meta["tax_year"] = self._extract_tax_year(raw_data["full_text"])
        doc_meta["parse_status"] = "success"
        has_rsu = bool(raw_data.get("rsu_lots"))
        doc_meta["source_format"] = "fidelity_1099b_rsu" if has_rsu else "fidelity_1099b_composite"
        warnings = self._cross_validate(raw_data) if has_rsu else []
        return {
            "document_metadata": doc_meta,
            "dividends_1099div": [raw_data["dividends"]],
            "interest_1099int": [],
            "transactions_1099b": raw_data["transactions"],
            "realized_gain_loss_summary": self._summary_to_list(raw_data["summary"]),
            "realized_gain_loss_detail": [],
            "positions": [],
            "transfers": [],
            "rsu_events": raw_data.get("rsu_lots", []),
            "warnings": warnings,
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
        # GL can be negative (-421.45), so use -? prefix. Some years have 6 columns (extra fed tax withheld).
        _NUM = r'(-?[\d,.]+)'
        _OPT = r'(?:\s+-?[\d,.]+)?'  # optional trailing column
        patterns = [
            ("short_term_reported", rf'Short-termtransactionsforwhichbasisisreportedtotheIRS\s+{_NUM}\s+{_NUM}\s+{_NUM}\s+{_NUM}\s+{_NUM}{_OPT}'),
            ("short_term_not_reported", rf'Short-termtransactionsforwhichbasisisnotreportedtotheIRS\s+{_NUM}\s+{_NUM}\s+{_NUM}\s+{_NUM}\s+{_NUM}{_OPT}'),
            ("long_term_reported", rf'Long-termtransactionsforwhichbasisisreportedtotheIRS\s+{_NUM}\s+{_NUM}\s+{_NUM}\s+{_NUM}\s+{_NUM}{_OPT}'),
            ("long_term_not_reported", rf'Long-termtransactionsforwhichbasisisnotreportedtotheIRS\s+{_NUM}\s+{_NUM}\s+{_NUM}\s+{_NUM}\s+{_NUM}{_OPT}'),
        ]
        for key, pattern in patterns:
            m = re.search(pattern, text)
            if m:
                summary[key] = {
                    "proceeds": _parse_signed(m.group(1)),
                    "cost_basis": _parse_signed(m.group(2)),
                    "market_discount": _parse_signed(m.group(3)),
                    "wash_sale": _parse_signed(m.group(4)),
                    "gain_loss": _parse_signed(m.group(5)),
                }

        # Total line: starts with a large number (total proceeds), 5 or 6 columns
        total_m = re.search(r'^(-?[\d,]+\.\d{2})\s+(-?[\d,.]+)\s+(-?[\d,.]+)\s+(-?[\d,.]+)\s+(-?[\d,.]+)(?:\s+-?[\d,.]+)?\s*$', text, re.MULTILINE)
        if total_m:
            summary["total"] = {
                "proceeds": _parse_signed(total_m.group(1)),
                "cost_basis": _parse_signed(total_m.group(2)),
                "market_discount": _parse_signed(total_m.group(3)),
                "wash_sale": _parse_signed(total_m.group(4)),
                "gain_loss": _parse_signed(total_m.group(5)),
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
            # Symbol can be up to ~10 chars for options (e.g. AMZN23021)
            sec_m = re.match(r'^(.+?),\s*([A-Z][A-Z0-9]{0,9}),\s*([A-Z0-9]{9}\w*)$', line)
            if sec_m:
                current_description = sec_m.group(1).strip()
                current_symbol = sec_m.group(2)
                current_cusip = sec_m.group(3)
                continue

            # ISIN-format header: DESCRIPTION ISIN #XXXXXXXXXXXX
            isin_m = re.match(r'^(.+?)\s+ISIN\s+#?([A-Z0-9,]+)$', line)
            if isin_m:
                current_description = isin_m.group(1).strip()
                current_cusip = isin_m.group(2).replace(',', '')
                # Extract symbol from description if possible
                words = current_description.split()
                current_symbol = words[0] if words else "UNKNOWN"
                continue

            # Transaction line: ACTION QTY DATE_ACQ DATE_SOLD PROCEEDS COST_BASIS [WASH_SALE] [GAIN_LOSS]
            # Actions: Sale (normal), Expire (option expiry), Principal (bond/ETF principal return)
            txn_m = re.match(
                r'(?:Sale|Expire|Principal)\s+'
                r'([\d,.]+)\s+'           # quantity
                r'(\d{2}/\d{2}/\d{2})\s+' # date acquired
                r'(\d{2}/\d{2}/\d{2})\s+' # date sold
                r'(-?[\d,.]+)\s+'         # proceeds (can be negative for option assignments)
                r'([\d,.]+)'              # cost basis
                r'(.*)',                   # rest of line (gain, wash)
                line
            )
            if txn_m and current_symbol:
                proceeds = _parse_signed(txn_m.group(4))
                cost_basis = _parse_amount(txn_m.group(5))
                rest = txn_m.group(6).strip()

                # Parse remaining numbers from rest of line
                # PDF column order: Wash Sale | Gain/Loss (-)
                nums = re.findall(r'-?[\d,.]+', rest)
                wash_sale = 0.0
                if len(nums) >= 2:
                    wash_sale = abs(_parse_signed(nums[0]))
                    gain_loss = _parse_signed(nums[1])
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
        pass

    def _parse_rsu_supplemental(self, text: str) -> list:
        """Parse Supplemental Stock Plan Lot Detail section."""
        lots = []
        in_section = False
        current_symbol = None
        current_cusip = None
        current_description = None
        holding = "SHORT_TERM"

        for line in text.split('\n'):
            line = line.strip()
            if 'Supplemental Stock Plan Lot Detail' in line:
                in_section = True
                continue
            if not in_section:
                continue
            ll = line.lower()
            if 'short-term transactions' in ll:
                holding = "SHORT_TERM"
                continue
            if 'long-term transactions' in ll:
                holding = "LONG_TERM"
                continue

            # Security header
            sec_m = re.match(r'^(.+?),\s*([A-Z][A-Z0-9]{0,4}),\s*([A-Z0-9]{9})$', line)
            if sec_m:
                current_description = sec_m.group(1).strip()
                current_symbol = sec_m.group(2)
                current_cusip = sec_m.group(3)
                continue

            # RSU lot: GRANT_TYPE QTY DATE_ACQ DATE_SOLD PROCEEDS ORD_INCOME ADJ_COST WASH ADJ_GL
            lot_m = re.match(
                r'(RSU|RSA|NQSOP|NQSP|DO|QSP|QSOP|SAR|NSR)\s+'
                r'([\d,.]+)\s+'           # quantity
                r'(\d{2}/\d{2}/\d{2})\s+' # date acquired
                r'(\d{2}/\d{2}/\d{2})\s+' # date sold
                r'([\d,.]+)\s+'           # proceeds
                r'([\d,.]+)\s+'           # ordinary income reported
                r'([\d,.]+)\s+'           # adjusted cost basis
                r'([\d,.]+)\s+'           # wash sale
                r'(-?[\d,.]+)',            # adjusted gain/loss
                line
            )
            if lot_m and current_symbol:
                lots.append({
                    "grant_type": lot_m.group(1),
                    "symbol": current_symbol,
                    "cusip": current_cusip,
                    "description": current_description,
                    "quantity": _parse_amount(lot_m.group(2)),
                    "date_acquired": _normalize_date(lot_m.group(3)),
                    "date_sold": _normalize_date(lot_m.group(4)),
                    "proceeds": _parse_amount(lot_m.group(5)),
                    "ordinary_income_reported": _parse_amount(lot_m.group(6)),
                    "adjusted_cost_basis": _parse_amount(lot_m.group(7)),
                    "wash_sale_loss_disallowed": _parse_amount(lot_m.group(8)),
                    "adjusted_gain_loss": _parse_signed(lot_m.group(9)),
                    "holding_period": holding,
                    "is_rsu": True,
                })
        return lots

    def _enrich_rsu_transactions(self, transactions: list, rsu_lots: list):
        """Enrich 1099-B transactions with adjusted cost basis from RSU supplemental."""
        # Build lookup: (symbol, date_acquired, date_sold, proceeds) -> rsu_lot
        rsu_map = {}
        for lot in rsu_lots:
            key = (lot["symbol"], lot["date_acquired"], lot["date_sold"], lot["proceeds"])
            rsu_map[key] = lot

        for txn in transactions:
            key = (txn["symbol"], txn["date_acquired"], txn["date_sold"], txn["proceeds"])
            rsu = rsu_map.get(key)
            if rsu:
                txn["is_rsu"] = True
                txn["grant_type"] = rsu["grant_type"]
                txn["ordinary_income_reported"] = rsu["ordinary_income_reported"]
                txn["adjusted_cost_basis"] = rsu["adjusted_cost_basis"]
                txn["adjusted_gain_loss"] = rsu["adjusted_gain_loss"]

    def _cross_validate(self, raw_data: dict) -> list:
        """Cross-validate RSU supplemental totals against 1099-B transactions."""
        warnings = []
        rsu_lots = raw_data.get("rsu_lots", [])
        txns = raw_data.get("transactions", [])
        if not rsu_lots:
            return warnings

        rsu_proceeds = sum(l["proceeds"] for l in rsu_lots)
        rsu_txns = [t for t in txns if t.get("is_rsu")]
        txn_proceeds = sum(t["proceeds"] for t in rsu_txns)

        if abs(rsu_proceeds - txn_proceeds) > 0.02:
            warnings.append(
                f"RSU supplemental proceeds ({rsu_proceeds:.2f}) != "
                f"matched 1099-B proceeds ({txn_proceeds:.2f}), "
                f"diff={rsu_proceeds - txn_proceeds:.2f}"
            )
        else:
            logger.info("RSU cross-validation passed: proceeds=%.2f, %d lots matched",
                        rsu_proceeds, len(rsu_lots))

        unmatched = len(rsu_lots) - len(rsu_txns)
        if unmatched:
            warnings.append(f"{unmatched} RSU supplemental lots not matched to 1099-B transactions")
        return warnings

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

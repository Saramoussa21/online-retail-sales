"""
Data Transformation Engine (pure)
- No DB access here. Output contains normalized fields and natural keys.
"""
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from ..utils.logging_config import ETLLogger

import re

@dataclass
class TransformationMetrics:
    total_records: int = 0
    successful: int = 0
    failed: int = 0

class RetailDataTransformer:
    def __init__(self):
        self.logger = ETLLogger("transformation.retail")
        self.metrics = TransformationMetrics()

    def _parse_invoice(self, invoice_raw: Optional[str]):
        invoice = "" if invoice_raw is None else str(invoice_raw).strip()
        is_return = invoice.startswith("C")
        invoice_num = invoice[1:] if is_return else invoice
        try:
            invoice_no = int(invoice_num) if invoice_num.isdigit() else 0
        except Exception:
            invoice_no = 0
        return invoice, invoice_no, is_return

    def _to_decimal(self, value) -> Decimal:
        try:
            return Decimal(str(value)) if value is not None and str(value) != "" else Decimal("0.00")
        except (InvalidOperation, TypeError):
            return Decimal("0.00")

    def _safe_float(self, value) -> float:
        try:
            return float(value)
        except Exception:
            return 0.0
    def _categorize_stock_code(self, stock_code: str, description: str = "") -> tuple[str, str, bool]:
        """
        Pure, no-DB categorization for special stock codes.
        Returns (category, subcategory, is_gift).
        """
        sc = (stock_code or "").upper().strip()
        desc = (description or "").upper()

        exact = {
            "AMAZONFEE":   ("Fees",        "Marketplace Fee", False),
            "BANKCHARGES": ("Fees",        "Bank Charge",     False),
            "POST":        ("Shipping",    "Postage",         False),
            "DOT":         ("Adjustment",  "Rounding",        False),
            "D":           ("Discount",    "Manual Discount", False),
            "M":           ("Adjustment",  "Manual",          False),
            "S":           ("Services",    "Service Charge",  False),
            "CRUK":        ("Charity",     "Donation",        False),
            "PADS":        ("Stationery",  "Pads",            False),
            "C2":          ("Shipping",    "Carrier Surcharge", False),
        }
        if sc in exact:
            return exact[sc]

        if sc.startswith("GIFT_"):
            m = re.search(r'GIFT_[A-Z0-9]+_(\d+)', sc)
            amount = m.group(1) if m else ""
            sub = f"Voucher £{amount}" if amount else "Voucher"
            return ("Gift Voucher", sub, True)

        if sc == "DCGSSBOY":
            return ("Gift Sets", "Boy", True)
        if sc == "DCGSSGIRL":
            return ("Gift Sets", "Girl", True)
        if sc.startswith("DCGS"):
            return ("Gift Sets", "DCGS", True)

        # Description hints (fallbacks)
        if "POSTAGE" in desc or "SHIPPING" in desc:
            return ("Shipping", "Postage", False)
        if "DISCOUNT" in desc:
            return ("Discount", "Promotion", False)

        return ("Merchandise", "General", False)


    def _classify_transaction(self, stock_code: str, qty: int, unit_price: float, invoice_raw: str,
                            category: str, subcategory: str, line_total: float) -> str:
        """
        Classify transaction types beyond SALE/RETURN.
        Uses stock code patterns + qty sign + invoice prefix + computed amount.
        """
        sc = (stock_code or "").upper()
        inv = (invoice_raw or "").upper()
        is_credit_invoice = inv.startswith("C")

        # Priority 1: non-merch / operational codes by meaning
        fee_codes = {"AMAZONFEE", "BANKCHARGES"}
        if sc in fee_codes or category == "Fees":
            # Fees may come as +/-; we treat them as FEE regardless of sign
            return "FEE"

        if sc in {"POST", "C2"} or category == "Shipping":
            return "SHIPPING"

        if sc in {"D"} or category == "Discount" or "DISCOUNT" in (subcategory or "").upper():
            return "DISCOUNT"

        if sc in {"CRUK"} or category == "Charity":
            return "DONATION"

        if sc in {"DOT", "M", "S"} or category == "Adjustment":
            return "ADJUSTMENT"

        # Gift Voucher logic (sale of voucher vs redemption)
        if category == "Gift Voucher":
            # Typical patterns:
            #  - Voucher SALE: qty>0 and amount>0 (selling a voucher)
            #  - Voucher REDEMPTION: amount<0 or qty<0 (redeeming against a sale)
            if line_total < 0 or qty < 0 or is_credit_invoice:
                return "VOUCHER_REDEMPTION"
            return "VOUCHER_SALE"

        # Services
        if category == "Services":
            return "SERVICE"

        # Merchandise / Gift Sets, Stationery, etc.
        # Don’t assume all negative qty are returns: use invoice + sign
        if is_credit_invoice and qty <= 0:
            return "RETURN"
        if sc.startswith("DCGS") or category in {"Gift Sets", "Stationery", "Merchandise"}:
            # If it's not a credit invoice but qty<0, treat as stock adjustment
            if not is_credit_invoice and qty < 0:
                return "ADJUSTMENT"
            return "SALE"

        # Fallbacks
        if is_credit_invoice and qty <= 0:
            return "RETURN"
        if qty < 0:
            return "ADJUSTMENT"
        return "SALE"

    def transform(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Transform a cleaned input record into normalized dict.
        This function does NOT access DB or perform lookups.
        It returns natural keys (customer_id, stock_code, transaction_date) so
        the loader can resolve surrogate keys & persist.
        """
        self.metrics.total_records += 1
        try:
            invoice_raw = record.get("InvoiceNo")
            invoice_str, invoice_no, is_return = self._parse_invoice(invoice_raw)

            customer_id = record.get("CustomerID") or "GUEST"
            if isinstance(customer_id, float) and customer_id.is_integer():
                customer_id = str(int(customer_id))
            customer_id = str(customer_id).strip()

            qty_raw = record.get("Quantity", 0)
            qty = int(self._safe_float(qty_raw))

            unit_price = float(self._to_decimal(record.get("UnitPrice")))
            line_total = float(self._to_decimal(record.get("Quantity", 0)) * self._to_decimal(record.get("UnitPrice", 0)))

            stock_code = str(record.get("StockCode") or "").strip()
            description = record.get("Description") or ""
            txn_dt = record.get("InvoiceDate")
            txn_date = None
            if isinstance(txn_dt, datetime):
                txn_date = txn_dt.date()
            elif isinstance(txn_dt, date):
                txn_date = txn_dt
            else:
                try:
                    txn_date = datetime.fromisoformat(str(txn_dt)).date()
                except Exception:
                    txn_date = None
            # derive category/subcategory/is_gift (pure, no DB)
            category, subcategory, is_gift = self._categorize_stock_code(stock_code, description)

            invoice_raw = record.get("InvoiceNo")
            invoice_str, invoice_no, is_credit_prefix = self._parse_invoice(invoice_raw)

            # NEW: robust transaction_type (not only credit prefix / negative qty)
            transaction_type = self._classify_transaction(
                stock_code=stock_code,
                qty=qty,
                unit_price=unit_price,
                invoice_raw=invoice_str,
                category=category,
                subcategory=subcategory,
                line_total=line_total
            )

            transformed = {
                "invoice_no": invoice_no,
                "invoice_raw": invoice_str,
                "transaction_type": transaction_type,    # <-- now multi-type
                "quantity": qty,                         # sign preserved
                "unit_price": unit_price,
                "line_total": line_total,
                "transaction_datetime": record.get("InvoiceDate"),
                "transaction_date": txn_date,
                "customer_id": customer_id,
                "stock_code": stock_code,
                "description": description,
                "country": record.get("Country", "Unknown"),
                "created_at": datetime.utcnow(),
                "batch_id": record.get("batch_id") or record.get("_ingestion_batch_id") or "",
                "data_source": record.get("data_source", "CSV"),
                "category": category,
                "subcategory": subcategory,
                "is_gift": is_gift,
            }
            self.metrics.successful += 1
            return transformed

        except Exception as e:
            self.metrics.failed += 1
            self.logger.info(f"Transformation failed for record: {e}")
            return None

    def transform_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return self.transform(record)

def create_transformation_pipeline() -> RetailDataTransformer:
    return RetailDataTransformer()
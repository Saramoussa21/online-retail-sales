"""
Data Transformation Engine (pure)
- No DB access here. Output contains normalized fields and natural keys.
"""
from typing import Dict, Any, Optional
from dataclasses import dataclass
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
        is_credit = invoice.startswith("C")
        invoice_num = invoice[1:] if is_credit else invoice
        try:
            invoice_no = int(invoice_num) if invoice_num.isdigit() else 0
        except Exception:
            invoice_no = 0
        return invoice_no, is_credit   # <-- return only what we need downstream

    def _classify_transaction(
        self,
        *,
        stock_code: str,
        qty: int,
        unit_price_abs: float,
        is_credit_invoice: bool,
        category: str,
        subcategory: str,
        line_total_signed: float
    ) -> str:
        """
        Return a granular transaction_type that encodes direction when needed.
        This removes any dependency on invoice_raw and avoids storing signed fields.
        """
        sc = (stock_code or "").upper()

        # Non-merch / operational
        if category == "Fees" or sc in {"AMAZONFEE", "BANKCHARGES"}:
            return "FEE_REVERSAL" if is_credit_invoice else "FEE"

        if category == "Shipping" or sc in {"POST", "C2"}:
            return "SHIPPING_REFUND" if is_credit_invoice else "SHIPPING_CHARGE"

        if category == "Discount" or sc == "D" or "DISCOUNT" in (subcategory or "").upper():
            # discount on credit note is typically a reversal (positive)
            return "DISCOUNT_REVERSAL" if is_credit_invoice else "DISCOUNT"

        if category == "Charity" or sc == "CRUK":
            return "DONATION"  # treat as negative in finance mapping

        if category == "Adjustment" or sc in {"DOT", "M", "S"}:
            # direction comes from quantity sign
            if qty < 0:
                return "ADJUSTMENT_OUT"
            elif qty > 0:
                return "ADJUSTMENT_IN"
            else:
                return "ADJUSTMENT"  # rare zero-line

        if category == "Gift Voucher":
            # Redemption reduces revenue, sale is typically neutral (configure per policy)
            if is_credit_invoice or qty < 0 or line_total_signed < 0:
                return "VOUCHER_REDEMPTION"
            return "VOUCHER_SALE"

        if category == "Services":
            return "SERVICE"

        # Merchandise logic
        if is_credit_invoice and qty <= 0:
            return "RETURN"
        if not is_credit_invoice and qty < 0:
            # negative qty on normal invoice -> stock correction out
            return "ADJUSTMENT_OUT"
        if not is_credit_invoice and qty > 0:
            return "SALE"

        # Fallbacks
        if is_credit_invoice:
            return "RETURN"
        return "SALE"

    def transform(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        self.metrics.total_records += 1
        try:
            # Parse invoice -> numeric + credit flag (no invoice_raw persisted)
            invoice_no, is_credit = self._parse_invoice(record.get("InvoiceNo"))

            customer_id = str(record.get("CustomerID")).strip()

            qty = int(self._safe_float(record.get("Quantity", 0)))  # keep sign for classification
            unit_price_abs = float(self._to_decimal(record.get("UnitPrice")))
            if unit_price_abs < 0:
                unit_price_abs = abs(unit_price_abs)

            signed_line_total = float(qty) * unit_price_abs

            stock_code = str(record.get("StockCode") or "").strip()
            description = record.get("Description") or "Unknown"

            txn_dt = record.get("InvoiceDate")
            if isinstance(txn_dt, datetime):
                txn_date = txn_dt.date()
            elif isinstance(txn_dt, date):
                txn_date = txn_dt
            else:
                try:
                    txn_date = datetime.fromisoformat(str(txn_dt)).date()
                except Exception:
                    txn_date = None

            category, subcategory, is_gift = self._categorize_stock_code(stock_code, description)

            # GRANULAR type encodes direction when needed
            transaction_type = self._classify_transaction(
                stock_code=stock_code,
                qty=qty,
                unit_price_abs=unit_price_abs,
                is_credit_invoice=is_credit,
                category=category,
                subcategory=subcategory,
                line_total_signed=signed_line_total
            )

            transformed = {
                "invoice_no": invoice_no,                   # numeric only
                "transaction_type": transaction_type,       # granular type (direction baked in)
                "quantity": abs(qty),                       # store positive in DW
                "unit_price": unit_price_abs,               # non-negative
                "line_total": abs(signed_line_total),       # store positive in DW
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

    def transform_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return self.transform(record)


def create_transformation_pipeline() -> RetailDataTransformer:
    return RetailDataTransformer()

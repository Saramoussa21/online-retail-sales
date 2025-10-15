# ... ...
from typing import List, Dict, Any, Optional
from datetime import datetime, date
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError, ProgrammingError
from ..database.connection import get_db_session
from ..database.models import DimCustomer, DimProduct, DimDate, FactSales
from ..utils.logging_config import ETLLogger
from ..database.schema import schema_manager
from sqlalchemy import text

_logger = ETLLogger("etl.loader")

_query_cache = None
try:
    from retail_data_platform.performance.cache import query_cache as _query_cache
    print ("cache",_query_cache)
except Exception as e:
    _logger.info("performance.query_cache not available; loader will run without persistent cache")

class LoaderService:
    """
    LoaderService that resolves dimension keys and persists fact rows.
    Optimized to perform batch lookups and bulk inserts for dimensions and facts.
    """
    def __init__(self):
        self.cache = _query_cache  # may be None
        self.logger = _logger

    def _cache_get(self, prefix: str, key: str):
        if not self.cache:
            return None
        try:
            return self.cache.get(f"dim:{prefix}:{key}")
        except Exception:
            return None

    def _cache_set(self, prefix: str, key: str, value):
        if not self.cache:
            return
        try:
            self.cache.set(f"dim:{prefix}:{key}", value)
        except Exception:
            pass

    # ---------- product (per-row fallback) ----------
    def get_or_create_product_key(
        self,
        stock_code: str,
        description: str = "",
        category: Optional[str] = None,
        subcategory: Optional[str] = None,
        is_gift: bool = False,
        data_source: str = "CSV",
    ) -> Optional[int]:
        stock = (stock_code or "").strip()
        if not stock:
            return None
        cached = self._cache_get("product", stock)
        if cached:
            return cached

        now = datetime.utcnow()

        try:
            with get_db_session() as session:
                try:
                    stmt = pg_insert(DimProduct).values(
                        stock_code=stock,
                        description=(description or ""),
                        category=category,
                        subcategory=subcategory,
                        is_gift=is_gift,
                        is_active=True,
                        data_source=data_source,
                        created_at=now,
                        updated_at=now,
                    )
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['stock_code'],
                        set_={
                            'description': stmt.excluded.description,
                            'category': stmt.excluded.category,
                            'subcategory': stmt.excluded.subcategory,
                            'is_gift': stmt.excluded.is_gift,
                            'updated_at': stmt.excluded.updated_at,
                            'data_source': stmt.excluded.data_source
                        }
                    ).returning(DimProduct.product_key)
                    res = session.execute(stmt)
                    key = res.scalar()
                    session.commit()
                except (ProgrammingError, SQLAlchemyError):
                    try:
                        session.rollback()
                    except Exception:
                        pass
                    try:
                        existing = session.query(DimProduct).filter(DimProduct.stock_code == stock).first()
                        if existing:
                            # optional: update fields if new info provided
                            changed = False
                            if description and description != (existing.description or ""):
                                existing.description = description; changed = True
                            if category and category != (existing.category or ""):
                                existing.category = category; changed = True
                            if subcategory and subcategory != (existing.subcategory or ""):
                                existing.subcategory = subcategory; changed = True
                            if bool(is_gift) != bool(existing.is_gift):
                                existing.is_gift = bool(is_gift); changed = True
                            if changed:
                                existing.updated_at = now
                                session.commit()
                            key = existing.product_key
                        else:
                            new = DimProduct(
                                stock_code=stock,
                                description=(description or ""),
                                category=category,
                                subcategory=subcategory,
                                is_gift=is_gift,
                                is_active=True,
                                data_source=data_source,
                                created_at=now,
                                updated_at=now
                            )
                            session.add(new)
                            session.commit()
                            key = new.product_key
                    except Exception as e2:
                        try:
                            session.rollback()
                        except Exception:
                            pass
                        self.logger.info(f"Product lookup/insert failed (fallback): {e2}")
                        return None
                if key:
                    self._cache_set("product", stock, key)
                    return key
        except Exception as e:
            self.logger.info(f"Product lookup/insert failed: {e}")
        return None


    # ---------- customer (per-row fallback) ----------
    def get_or_create_customer_key(self, customer_id: str, country: str = "Unknown") -> Optional[int]:
        cid = str(customer_id or "").strip() or "GUEST"
        cache_key = f"{cid}|{country}"
        cached = self._cache_get("customer", cache_key)
        if cached:
            return cached

        try:
            with get_db_session() as session:
                try:
                    stmt = pg_insert(DimCustomer).values(
                        customer_id=cid,
                        country=country or "Unknown",
                        is_current=True,
                        effective_date=datetime.utcnow()
                    ).on_conflict_do_nothing(index_elements=['customer_id']).returning(DimCustomer.customer_key)
                    res = session.execute(stmt)
                    key = res.scalar()
                    if not key:
                        session.rollback()
                        existing = session.query(DimCustomer).filter(DimCustomer.customer_id == cid).first()
                        key = existing.customer_key if existing else None
                    else:
                        session.commit()
                except (ProgrammingError, SQLAlchemyError):
                    try:
                        session.rollback()
                    except Exception:
                        pass
                    try:
                        existing = session.query(DimCustomer).filter(DimCustomer.customer_id == cid).first()
                        if existing:
                            key = existing.customer_key
                        else:
                            new = DimCustomer(customer_id=cid, country=country or "Unknown", is_current=True,
                                              effective_date=datetime.utcnow())
                            session.add(new)
                            session.commit()
                            key = new.customer_key
                    except Exception as e2:
                        try:
                            session.rollback()
                        except Exception:
                            pass
                        self.logger.info(f"Customer lookup/insert failed (fallback): {e2}")
                        return None
                if key:
                    self._cache_set("customer", cache_key, key)
                    return key
        except Exception as e:
            self.logger.info(f"Customer lookup/insert failed: {e}")
        return None

    # ---------- date (per-row fallback) ----------
    def _compute_date_fields(self, dt_value: date) -> Dict[str, Any]:
        quarter = (dt_value.month - 1) // 3 + 1
        week = dt_value.isocalendar()[1]
        day_of_year = dt_value.timetuple().tm_yday
        day_of_month = dt_value.day
        day_of_week = dt_value.isoweekday()
        month_name = dt_value.strftime("%B")
        day_name = dt_value.strftime("%A")
        quarter_name = f"Q{quarter}"
        is_weekend = dt_value.weekday() >= 5
        is_holiday = False
        return {
            "date_key": int(dt_value.strftime("%Y%m%d")),
            "date_value": dt_value,
            "year": dt_value.year,
            "quarter": quarter,
            "month": dt_value.month,
            "week": week,
            "day_of_year": day_of_year,
            "day_of_month": day_of_month,
            "day_of_week": day_of_week,
            "month_name": month_name,
            "day_name": day_name,
            "quarter_name": quarter_name,
            "is_weekend": is_weekend,
            "is_holiday": is_holiday
        }

    def get_or_create_date_key(self, dt_value: date) -> Optional[int]:
        if not isinstance(dt_value, date):
            return None
        iso = dt_value.isoformat()
        cached = self._cache_get("date", iso)
        if cached:
            return cached

        fields = self._compute_date_fields(dt_value)
        date_key = fields["date_key"]
        try:
            with get_db_session() as session:
                try:
                    stmt = pg_insert(DimDate).values(
                        date_key=fields["date_key"],
                        date_value=fields["date_value"],
                        year=fields["year"],
                        quarter=fields["quarter"],
                        month=fields["month"],
                        week=fields["week"],
                        day_of_year=fields["day_of_year"],
                        day_of_month=fields["day_of_month"],
                        day_of_week=fields["day_of_week"],
                        month_name=fields["month_name"],
                        day_name=fields["day_name"],
                        quarter_name=fields["quarter_name"],
                        is_weekend=fields["is_weekend"],
                        is_holiday=fields["is_holiday"]
                    ).on_conflict_do_nothing(index_elements=['date_key']).returning(DimDate.date_key)
                    res = session.execute(stmt)
                    key = res.scalar()
                    if not key:
                        session.rollback()
                        existing = session.query(DimDate).filter(DimDate.date_key == date_key).first()
                        key = existing.date_key if existing else None
                    else:
                        session.commit()
                except (ProgrammingError, SQLAlchemyError):
                    try:
                        session.rollback()
                    except Exception:
                        pass
                    try:
                        existing = session.query(DimDate).filter(DimDate.date_key == date_key).first()
                        if existing:
                            key = existing.date_key
                        else:
                            new = DimDate(
                                date_key=fields["date_key"],
                                date_value=fields["date_value"],
                                year=fields["year"],
                                quarter=fields["quarter"],
                                month=fields["month"],
                                week=fields["week"],
                                day_of_year=fields["day_of_year"],
                                day_of_month=fields["day_of_month"],
                                day_of_week=fields["day_of_week"],
                                month_name=fields["month_name"],
                                day_name=fields["day_name"],
                                quarter_name=fields["quarter_name"],
                                is_weekend=fields["is_weekend"],
                                is_holiday=fields["is_holiday"]
                            )
                            session.add(new)
                            session.commit()
                            key = new.date_key
                    except Exception as e2:
                        try:
                            session.rollback()
                        except Exception:
                            pass
                        self.logger.info(f"Date lookup/insert failed (fallback): {e2}")
                        return None
                if key:
                    self._cache_set("date", iso, key)
                    return key
        except Exception as e:
            self.logger.info(f"Date lookup/insert failed: {e}")
        return None

    # ---------- load (BATCHED) ----------
    def load_fact_rows(self, rows: List[Dict[str, Any]]) -> int:
        """
        Batch loading optimized:
         - single batch dimension lookups (customers/products/dates)
         - bulk insert missing dim rows via pg_insert(...).values(list_of_dicts) + ON CONFLICT
         - re-query to obtain surrogate keys
         - bulk insert fact rows with session.add_all and single commit
        """
        if not rows:
            return 0

        inserted = 0
        rejected = 0

        try:
            # Prepare distinct naturals for the batch
            customer_ids = list({str(r.get("customer_id") or "GUEST") for r in rows if r.get("customer_id") is not None})
            stock_codes = list({str(r.get("stock_code") or "") for r in rows if r.get("stock_code")})
            # compute date_keys for date dim
            dates = []
            for r in rows:
                tx = r.get("transaction_datetime") or r.get("transaction_date")
                if isinstance(tx, datetime):
                    d = tx.date()
                    dates.append(d)
                elif isinstance(tx, date):
                    dates.append(tx)
                # else ignore missing/invalid

            # maps natural -> surrogate
            customer_map: Dict[str, int] = {}
            product_map: Dict[str, int] = {}
            date_map: Dict[date, int] = {}

            with get_db_session() as session:
                # 1) Query existing customers/products/dates in single statements
                if customer_ids:
                    q = text("SELECT customer_id, customer_key, country FROM retail_dw.dim_customer WHERE customer_id = ANY(:ids) AND is_current = true")
                    for r in session.execute(q, {'ids': customer_ids}).mappings():
                        customer_map[str(r['customer_id'])] = r['customer_key']

                if stock_codes:
                    q = text("SELECT stock_code, product_key FROM retail_dw.dim_product WHERE stock_code = ANY(:codes)")
                    for r in session.execute(q, {'codes': stock_codes}).mappings():
                        product_map[str(r['stock_code'])] = r['product_key']

                if dates:
                    # date_value stored as date in dim_date; convert list to ISO strings if needed
                    date_values = [d for d in dates]
                    q = text("SELECT date_value, date_key FROM retail_dw.dim_date WHERE date_value = ANY(:dates)")
                    # SQLAlchemy accepts python date objects for parameter binding
                    for r in session.execute(q, {'dates': date_values}).mappings():
                        date_map[r['date_value']] = r['date_key']

                # 2) Build missing lists to insert
                missing_customers = []
                for cid in customer_ids:
                    if cid not in customer_map:
                        missing_customers.append({
                            "customer_id": cid,
                            "country": "Unknown",
                            "effective_date": datetime.utcnow(),
                            "is_current": True,
                            "created_at": datetime.utcnow(),
                            "updated_at": datetime.utcnow(),
                            "data_source": "CSV"
                        })

                prod_attrs: Dict[str, Dict[str, Any]] = {}
                for r in rows:
                    sc = r.get("stock_code")
                    if not sc:
                        continue
                    cur = prod_attrs.get(sc, {"description": "", "category": None, "subcategory": None, "is_gift": False})
                    desc = (r.get("description") or "").strip()
                    # prefer longer description
                    if len(desc) > len(cur["description"]):
                        cur["description"] = desc
                    # prefer non-empty category/subcategory
                    cat = r.get("category")
                    sub = r.get("subcategory")
                    if cat:
                        cur["category"] = cat
                    if sub:
                        cur["subcategory"] = sub
                    if bool(r.get("is_gift")):
                        cur["is_gift"] = True
                    prod_attrs[sc] = cur


                missing_products = []
                now = datetime.utcnow()
                for sc in stock_codes:
                    if sc not in product_map:
                        attr = prod_attrs.get(sc, {})
                        missing_products.append({
                            "stock_code": sc,
                            "description": attr.get("description", "Unknown"),
                            "category": attr.get("category"),
                            "subcategory": attr.get("subcategory"),
                            "is_active": True,
                            "is_gift": bool(attr.get("is_gift", False)),
                            "created_at": now,
                            "updated_at": now,
                            "data_source": "CSV"
                        })

                missing_dates = []
                for d in set(dates):
                    if d not in date_map:
                        fields = self._compute_date_fields(d)
                        missing_dates.append(fields)

                # 3) Bulk insert missing dims (single statements)
                try:
                    if missing_customers:
                        stmt = pg_insert(DimCustomer).values(missing_customers)
                        stmt = stmt.on_conflict_do_nothing(index_elements=['customer_id'])
                        session.execute(stmt)

                    if missing_products:
                        stmt = pg_insert(DimProduct).values(missing_products)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['stock_code'],
                            set_={
                                'description': stmt.excluded.description,
                                'category': stmt.excluded.category,
                                'subcategory': stmt.excluded.subcategory,
                                'is_gift': stmt.excluded.is_gift,
                                'updated_at': stmt.excluded.updated_at,
                                'data_source': stmt.excluded.data_source
                            }
                        )
                        session.execute(stmt)


                    if missing_dates:
                        stmt = pg_insert(DimDate).values(missing_dates)
                        stmt = stmt.on_conflict_do_nothing(index_elements=['date_key'])
                        session.execute(stmt)

                    # commit dimension inserts once
                    session.commit()
                except Exception as e:
                    try:
                        session.rollback()
                    except Exception:
                        pass
                    self.logger.warning(f"Bulk dim insert failed; falling back to per-row: {e}", exc_info=True)
                    # fallback: use existing per-row methods for remaining rows
                    return self._fallback_load(rows)

                # 4) Re-query to refresh mapping after inserts
                if customer_ids:
                    q = text("SELECT customer_id, customer_key FROM retail_dw.dim_customer WHERE customer_id = ANY(:ids)")
                    for r in session.execute(q, {'ids': customer_ids}).mappings():
                        customer_map[str(r['customer_id'])] = r['customer_key']

                if stock_codes:
                    q = text("SELECT stock_code, product_key FROM retail_dw.dim_product WHERE stock_code = ANY(:codes)")
                    for r in session.execute(q, {'codes': stock_codes}).mappings():
                        product_map[str(r['stock_code'])] = r['product_key']

                if missing_dates:
                    keys = [int(d.strftime('%Y%m%d')) for d in set(dates) if hasattr(d, "strftime")]
                    if keys:
                        q = text("SELECT date_key, date_value FROM retail_dw.dim_date WHERE date_key = ANY(:keys)")
                        for r in session.execute(q, {'keys': keys}).mappings():
                            date_map[r['date_value']] = r['date_key']

                # 5) Build fact objects and bulk insert
                fact_objects = []
                tx_datetimes = []
                for r in rows:
                    tx = r.get("transaction_datetime") or r.get("transaction_date")
                    tx_dt = None
                    if isinstance(tx, datetime):
                        tx_dt = tx
                    elif isinstance(tx, date):
                        # convert to datetime at midnight
                        tx_dt = datetime.combine(tx, datetime.min.time())
                    else:
                        tx_dt = None

                    prod_key = product_map.get(str(r.get("stock_code")))
                    cust_key = customer_map.get(str(r.get("customer_id") or "GUEST"))
                    date_key = None
                    if tx_dt:
                        date_key = date_map.get(tx_dt.date())

                    if prod_key is None or date_key is None:
                        rejected += 1
                        self.logger.info(f"Row rejected (missing keys): invoice={r.get('invoice_no')} product_key={prod_key} date_key={date_key} customer_key={cust_key}")
                        continue

                    fact = FactSales(
                        date_key=date_key,
                        customer_key=cust_key,
                        product_key=prod_key,
                        invoice_no=r.get("invoice_no"),
                        transaction_type=r.get("transaction_type"),
                        quantity=r.get("quantity"),
                        unit_price=r.get("unit_price"),
                        line_total=r.get("line_total"),
                        transaction_datetime=tx_dt,
                        created_at=r.get("created_at") or datetime.utcnow(),
                        batch_id=r.get("batch_id"),
                        data_source=r.get("data_source")
                    )
                    fact_objects.append(fact)
                    if tx_dt:
                        tx_datetimes.append(tx_dt)

                # ensure partitions for date range BEFORE inserting facts
                if fact_objects and tx_datetimes:
                    min_dt = min(tx_datetimes)
                    max_dt = max(tx_datetimes)
                    try:
                        schema_manager.ensure_partitions_for_range(min_dt, max_dt)
                    except Exception as e:
                        self.logger.warning(f"Failed to ensure partitions for range {min_dt} - {max_dt}: {e}")

                if fact_objects:
                    try:
                        session.add_all(fact_objects)
                        session.commit()
                        inserted = len(fact_objects)
                    except Exception as e:
                        try:
                            session.rollback()
                        except Exception:
                            pass
                        self.logger.info(f"Failed to load fact rows: {e}")
                        # fallback to per-row add/commit loop
                        for fact in fact_objects:
                            try:
                                with get_db_session() as s2:
                                    s2.add(fact)
                                    s2.commit()
                                    inserted += 1
                            except Exception:
                                try:
                                    s2.rollback()
                                except Exception:
                                    pass
                # done with session
        except Exception as e:
            self.logger.info(f"Failed to load fact rows (outer): {e}", exc_info=True)
            # if anything major fails fallback
            return self._fallback_load(rows)

        if rejected:
            self.logger.info(f"Rejected {rejected} rows due to missing required dimension keys")
        return inserted

    def _fallback_load(self, rows: List[Dict[str, Any]]) -> int:
        """
        Safe fallback: original per-row logic (slower) used when bulk path fails.
        Keeps existing behavior but should rarely execute after bulk implementation.
        """
        inserted = 0
        try:
            with get_db_session() as session:
                fact_objects = []
                tx_datetimes = []
                for r in rows:
                    tx = r.get("transaction_datetime") or r.get("transaction_date") or r.get("transaction_datetime_str")
                    tx_dt = None
                    if isinstance(tx, datetime):
                        tx_dt = tx
                    elif isinstance(tx, str) and tx.strip():
                        try:
                            tx_dt = datetime.fromisoformat(tx.strip())
                        except Exception:
                            try:
                                tx_dt = datetime.strptime(tx.strip(), "%Y-%m-%d %H:%M:%S")
                            except Exception:
                                tx_dt = None

                    product_key = self.get_or_create_product_key(
                        r.get("stock_code"),
                        r.get("description"),
                        r.get("category"),
                        r.get("subcategory"),
                        bool(r.get("is_gift")),
                        r.get("data_source") or "CSV"
                    )
                    customer_key = self.get_or_create_customer_key(r.get("customer_id"), r.get("country"))
                    date_key = self.get_or_create_date_key(tx_dt.date()) if tx_dt else None

                    if date_key is None or product_key is None or customer_key is None:
                        self.logger.info(f"Row rejected (missing keys): invoice={r.get('invoice_no')} product_key={product_key} date_key={date_key} customer_key={customer_key}")
                        continue

                    fact = FactSales(
                        date_key=date_key,
                        customer_key=customer_key,
                        product_key=product_key,
                        invoice_no=r.get("invoice_no"),
                        transaction_type=r.get("transaction_type"),
                        quantity=r.get("quantity"),
                        unit_price=r.get("unit_price"),
                        line_total=r.get("line_total"),
                        transaction_datetime=tx_dt,
                        created_at=r.get("created_at") or datetime.utcnow(),
                        batch_id=r.get("batch_id"),
                        data_source=r.get("data_source")
                    )
                    fact_objects.append(fact)
                    if tx_dt:
                        tx_datetimes.append(tx_dt)

                if fact_objects and tx_datetimes:
                    try:
                        min_dt = min(tx_datetimes)
                        max_dt = max(tx_datetimes)
                        try:
                            schema_manager.ensure_partitions_for_range(min_dt, max_dt)
                        except Exception:
                            pass
                    except Exception:
                        pass

                if fact_objects:
                    try:
                        session.add_all(fact_objects)
                        session.commit()
                        inserted = len(fact_objects)
                    except Exception as e:
                        try:
                            session.rollback()
                        except Exception:
                            pass
                        self.logger.info(f"Failed to load fact rows in fallback: {e}")
        except Exception as e:
            self.logger.info(f"Fallback load failed: {e}", exc_info=True)
        return inserted


_loader_service_singleton = LoaderService()


def get_loader() -> LoaderService:
    return _loader_service_singleton


def load_fact_rows(rows: List[Dict[str, Any]]) -> int:
    return _loader_service_singleton.load_fact_rows(rows)

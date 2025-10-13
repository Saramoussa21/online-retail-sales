import csv
import datetime
import pytest

# SimpleCache is present in performance.cache and is lightweight for unit tests
from retail_data_platform.performance.cache import SimpleCache


def test_cache_set_get_customer_and_product():
    c = SimpleCache(default_ttl=60)
    c.set("product:SKU123", 111)
    assert c.get("product:SKU123") == 111

    c.set("customer:CUST1|UK", 222)
    assert c.get("customer:CUST1|UK") == 222

    d = datetime.date(2020, 1, 2)
    c.set(f"date:{d.isoformat()}", 20200102)
    assert c.get(f"date:{d.isoformat()}") == 20200102


def test_pipeline_calls_loader(monkeypatch, tmp_path):
    # prepare tiny CSV
    csv_path = tmp_path / "mini.csv"
    rows = [
        ["InvoiceNo","StockCode","Description","Quantity","InvoiceDate","UnitPrice","CustomerID","Country"],
        ["536365","85123A","WHITE HANGING HEART T-LIGHT HOLDER","2","2010-12-01 08:26:00","3.50","17850","United Kingdom"],
    ]
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)

    called = {"count": 0}

    def fake_loader(records):
        called["count"] = len(records)
        return len(records)

    # patch the LoaderService.load_fact_rows instance method used by the pipeline
    monkeypatch.setattr("retail_data_platform.etl.loader.LoaderService.load_fact_rows", lambda self, rows: fake_loader(rows))

    # Avoid DB calls in pipeline: patch lineage/version and quality monitor
    from retail_data_platform.etl.pipeline import ETLPipeline, run_retail_csv_etl

    monkeypatch.setattr(ETLPipeline, "_start_lineage_tracking", lambda self: "fake_lineage")
    monkeypatch.setattr(ETLPipeline, "_create_version_for_job", lambda self: None)
    monkeypatch.setattr(ETLPipeline, "_complete_lineage_tracking", lambda self, lineage_id, status, error_message=None: None)

    class FakeQM:
        def __init__(self, batch_id):
            pass
        def check_data_quality(self, records, table):
            return {}
        def persist_quality_metrics(self):
            return None
        def get_quality_summary(self):
            return {'total_checks': 0, 'passed_checks': 0, 'failed_checks': 0, 'success_rate': 100, 'overall_score': 100}
        def detect_quality_anomalies(self):
            return []

    monkeypatch.setattr("retail_data_platform.etl.pipeline.create_quality_monitor", lambda batch_id: FakeQM(batch_id))

    metrics = run_retail_csv_etl(str(csv_path), "test_job")
    assert called["count"] > 0
    assert metrics is not None

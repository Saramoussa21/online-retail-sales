import pytest
from retail_data_platform.etl.transformation import RetailDataTransformer


def make_sample_record():
    return {
        "InvoiceNo": "536365",
        "StockCode": "85123A",
        "Description": "WHITE HANGING HEART T-LIGHT HOLDER",
        "Quantity": "2",
        "InvoiceDate": "2010-12-01 08:26:00",
        "UnitPrice": "3.50",
        "CustomerID": "17850",
        "Country": "United Kingdom",
        "_ingestion_batch_id": "test-batch"
    }


def test_transform_basic_sale():
    t = RetailDataTransformer()
    rec = make_sample_record()
    out = t.transform(rec)
    assert out is not None
    assert out.get("transaction_type") == "SALE"
    assert out.get("invoice_no") == 536365
    assert out.get("quantity") == 2
    assert float(out.get("unit_price", 0)) == pytest.approx(3.5)
    assert "customer_id" in out

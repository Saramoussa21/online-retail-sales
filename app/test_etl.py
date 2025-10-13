#!/usr/bin/env python3
"""
Simple ETL test script
"""

import sys
import os

try:
    print("Starting ETL test...")
    from retail_data_platform.etl.pipeline import run_retail_csv_etl
    
    print("Running ETL on sample data...")
    result = run_retail_csv_etl('data/online_retail.csv', 'test_job')
    
    print(f"ETL completed with status: {result.status if hasattr(result, 'status') else 'Unknown'}")
    print(f"Records processed: {result.total_records if hasattr(result, 'total_records') else 'Unknown'}")
    
except Exception as e:
    print(f"ETL failed with error: {str(e)}")
    import traceback
    traceback.print_exc()
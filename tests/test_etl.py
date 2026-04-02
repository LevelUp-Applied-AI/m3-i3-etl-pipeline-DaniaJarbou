"""Tests for the ETL pipeline.

Write at least 3 tests:
1. test_transform_filters_cancelled — cancelled orders excluded after transform
2. test_transform_filters_suspicious_quantity — quantities > 100 excluded
3. test_validate_catches_nulls — validate() raises ValueError on null customer_id
"""
import pytest
import pandas as pd
import numpy as np
import sys
import os

# Adjusting system path to ensure the test runner can find etl_pipeline.py
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from etl_pipeline import transform, validate
except ImportError:
    # Fallback import for local execution without complex folder structures
    from etl_pipeline import transform, validate


def test_transform_filters_cancelled():
    """Create test DataFrames with a cancelled order. Confirm it's excluded."""
    customers = pd.DataFrame({
        'customer_id': [1, 2], 'customer_name': ['Sara Ahmad', 'Tala Ali'],
        'email': ['Sars@example.com', 'Tala@example.com'],
        'registration_date':['2023-07-10','2025-02-15'],
        'city': ['Irbid', 'Amman']})
    products = pd.DataFrame({
        'product_id': [10, 20], 
        'product_name': ['Gaming Laptop Pro', 'Espresso Coffee Maker'], 
        'category': ['Electronics', 'Home Appliances'],
        'unit_price': [850.00, 120.50]
    })
    orders = pd.DataFrame({
        'order_id': [101, 102], 
        'customer_id': [1, 2], 
        'status': ['completed', 'cancelled'] 
    })
    items = pd.DataFrame({
        'order_id': [101, 102], 
        'product_id': [10, 20], 
        'quantity': [1, 1]
    })
    
    data_dict = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": items
    }
    
    # Run transformation
    summary = transform(data_dict)
    
    # Assert: The result should only contain the completed order (customer 1)
    assert len(summary) == 1
    assert summary.iloc[0]['customer_id'] == 1
    assert 2 not in summary['customer_id'].values


    #-------------


def test_transform_filters_suspicious_quantity():
    """Create test DataFrames with quantity > 100. Confirm it's excluded."""
    customers = pd.DataFrame({
        'customer_id': [1, 2], 
        'customer_name': ['Omar Ali', 'Aya Mohammad'], 
        'email': ['Omer@gmail.com', 'aya@gmail.com'],
        'city': ['Amman', 'Aqaba'],
        'registration_date': ['2022-01-01', '2023-11-05']
    })
    
    products = pd.DataFrame({
        'product_id': [10, 20], 
        'product_name': ['Wireless Headphones', 'Mechanical Keyboard'], 
        'category': ['Electronics', 'Accessories'],
        'unit_price': [45.00, 60.00]
    })
    
    orders = pd.DataFrame({
        'order_id': [101, 102], 
        'customer_id': [1, 2], 
        'status': ['completed', 'completed']
    })
    
    # Setup a normal quantity and a suspicious quantity (100)
    items = pd.DataFrame({
        'order_id': [101, 102], 
        'product_id': [10, 20], 
        'quantity': [1, 101] 
    })
    
    data_dict = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": items
    }
    
    summary = transform(data_dict)
    
    # Assert: Confirm the record with quantity 100 is excluded
    assert len(summary) == 1
    assert summary.iloc[0]['customer_id'] == 1


#-------------
def test_validate_catches_nulls():
    """Create a DataFrame with null customer_id. Confirm validate() raises ValueError."""
   # Setup a summary DataFrame where one customer_name is missing (None/NaN)
    df_invalid = pd.DataFrame({
        'customer_id': [1, 2],
        'customer_name': ['Dania Mohammad', None], # Null name triggers the error
        'total_orders': [1, 1],
        'total_revenue': [800, 100],
        'avg_order_value': [800, 100]
    })
    
    # Assert: Calling validate should raise a ValueError
    with pytest.raises(ValueError):
        validate(df_invalid)

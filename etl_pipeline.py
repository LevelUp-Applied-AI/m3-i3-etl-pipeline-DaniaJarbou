"""ETL Pipeline — Amman Digital Market Customer Analytics

Extracts data from PostgreSQL, transforms it into customer-level summaries,
validates data quality, and loads results to a database table and CSV file.
"""
from sqlalchemy import create_engine
import pandas as pd
import os


def extract(engine):
    """Extract all source tables from PostgreSQL into DataFrames.

    Args:
        engine: SQLAlchemy engine connected to the amman_market database

    Returns:
        dict: {"customers": df, "products": df, "orders": df, "order_items": df}
    """
    print("--- Phase 1: Extracting Data ---")
    df_customers = pd.read_sql("SELECT * FROM customers", engine)
    df_products = pd.read_sql("SELECT * FROM products", engine)
    df_orders = pd.read_sql("SELECT * FROM orders", engine)
    df_order_items = pd.read_sql("SELECT * FROM order_items", engine)



    data_dict = {
        "customers": df_customers,
        "products": df_products,
        "orders": df_orders,
        "order_items": df_order_items

    }
    for df, name in data_dict.items():
        print(f"Extracted {len(df)} rows from {name}.")
    
    return data_dict


def transform(data_dict):
    """Transform raw data into customer-level analytics summary.

    Steps:
    1. Join orders with order_items and products
    2. Compute line_total (quantity * unit_price)
    3. Filter out cancelled orders (status = 'cancelled')
    4. Filter out suspicious quantities (quantity > 100)
    5. Aggregate to customer level: total_orders, total_revenue,
       avg_order_value, top_category

    Args:
        data_dict: dict of DataFrames from extract()

    Returns:
        DataFrame: customer-level summary with columns:
            customer_id, customer_name, city, total_orders,
            total_revenue, avg_order_value, top_category
    """
    df_customers = data_dict["customers"]
    df_products = data_dict["products"]
    df_orders = data_dict["orders"]
    df_items = data_dict["order_items"]
    # Join items with orders on 'order_id' 
    merged_df = pd.merge(df_items, df_orders, on="order_id")
    #Join the result with products on 'product_id'
    merged_df = pd.merge(merged_df, df_products, on="product_id")
    # Join wit customers on customer_id
    merged_df = pd.merge(merged_df,df_customers,on="customer_id")

    merged_df['line_total'] =merged_df['quantity']* merged_df['unit_price']
    cleaned_df = merged_df[
        (merged_df['status'] != 'cancelled') &(merged_df['quantity'] <=100 )
    ]
    # grouping:
    customer_summary = cleaned_df.groupby(['customer_id', 'customer_name']).agg({
         'order_id': 'nunique',
         'line_total': 'sum'
    }).reset_index()
    # rename columns to match the requirements
    customer_summary.rename(columns={'order_id': 'total_orders', 'line_total': 'total_revenue'}, inplace=True)
    #calculate avg
    customer_summary['avg_order_value'] = customer_summary['total_revenue'] / customer_summary['total_orders']
    return customer_summary


def validate(df):
    """Run data quality checks on the transformed DataFrame.

    Checks:
    - No nulls in customer_id or customer_name
    - total_revenue > 0 for all customers
    - No duplicate customer_ids
    - total_orders > 0 for all customers

    Args:
        df: transformed customer summary DataFrame

    Returns:
        dict: {check_name: bool} for each check

    Raises:
        ValueError: if any critical check fails
    """
    #Check for null
    null_check = df['customer_name'].isnull().sum() == 0
    #Check Positive Revenue Check
    rev_check = (df['total_revenue'] > 0).all()
    #Formatting the output
    print(f"Check - No Null Names: {'PASS' if null_check else 'FAIL'}")
    print(f"Check - Revenue > 0: {'PASS' if rev_check else 'FAIL'}")

    df['customer_id'].is_unique
    unique_check = df['customer_id'].is_unique

    # Check if all customers have at least 1 order
    order_check = (df['total_orders'] > 0).all()

    print(f"Check - Unique IDs: {'PASS' if unique_check else 'FAIL'}")
    print(f"Check - Orders > 0: {'PASS' if order_check else 'FAIL'}")

    # If any check is False, stop the pipeline!
    if not (null_check and rev_check and unique_check and order_check):
        raise ValueError("Data Quality Check Failed! Transformation results are invalid.")
    return True
def load(df, engine, csv_path):
    """Load customer summary to PostgreSQL table and CSV file.

    Args:
        df: validated customer summary DataFrame
        engine: SQLAlchemy engine
        csv_path: path for CSV output
    """
    # 1. Create the output directory if it doesn't exist
    output_dir = os.path.dirname(csv_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    # 2. Save to CSV
    df.to_csv(csv_path, index=False)
    print(f"Successfully saved results to {csv_path}")

    # 3. Load to PostgreSQL table
    # Using if_exists='replace' to overwrite old data with fresh results
    df.to_sql("customer_summary", engine, if_exists="replace", index=False)
    print("Successfully loaded results to 'customer_summary' table in PostgreSQL")





def main():

    """Orchestrate the ETL pipeline: extract -> transform -> validate -> load."""
    # : Implement main orchestration
    # 1. Create engine from DATABASE_URL env var (or default)
    # 2. Extract
    # 3. Transform
    # 4. Validate
    # 5. Load to customer_summary table and output/customer_analytics.csv

 #Settings   
# Settings
    DATABASE_URL = "postgresql+psycopg://postgres:postgres@localhost:5433/amman_market"
    csv_path = "output/customer_analytics.csv"
    
    engine = create_engine(DATABASE_URL)

    # 1. Extract
    extracted_data = extract(engine)

    # 2. Transform
    customer_summary = transform(extracted_data)

    # 3. Validate
    if validate(customer_summary):
        print("Validation successful. Ready for loading.")

        # 4. Load
        load(customer_summary, engine, csv_path)
        print("\n--- ETL Pipeline Finished Successfully! ---")

if __name__ == "__main__":
    main()

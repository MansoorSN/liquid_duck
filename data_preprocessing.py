# %%
import pandas as pd
import duckdb as db

# Connect to the DuckDB database
connection = db.connect(database='liquid_duck.db', read_only=False)

# %%
# Create PRODUCTS table from the CSV file
connection.execute('''
    CREATE OR REPLACE TABLE products AS
    SELECT
        "Product_id"::INTEGER AS product_id,
        "Supplier"::VARCHAR(255) AS supplier,
        "Brand"::VARCHAR(255) AS brand,
        "Family"::VARCHAR(255) AS family,
        "Product_name"::VARCHAR(255) AS product_name,
        "Product_cost"::DECIMAL(10,2) AS product_cost,
        "Inventory_volume"::INTEGER AS inventory_volume
    FROM read_csv_auto('datasets/products.csv', header=True);
''')

# %%
# Create CUSTOMERS table from the CSV file
connection.execute('''
    CREATE OR REPLACE TABLE customers AS
    SELECT
        "Customer_id"::INTEGER AS customer_id,
        "Customer_name"::VARCHAR(255) AS customer_name,
        "Customer_address"::VARCHAR(255) AS customer_address,
        "Customer_phone"::VARCHAR(255) AS customer_phone
    FROM read_csv_auto('datasets/customers.csv', header=True);
''')

# %%
# Create SALES table from the CSV file
connection.execute('''
    CREATE OR REPLACE TABLE sales AS
    SELECT
        "sale_id"::INTEGER AS sale_id,
        "Product_id"::INTEGER AS product_id,
        "Customer_id"::INTEGER AS customer_id,
        "Sale_date"::DATE AS sale_date,
        "Sale_volume"::INTEGER AS sale_volume,
        "Sale_revenue"::DECIMAL(10,2) AS sale_revenue
    FROM read_csv_auto('datasets/sales.csv', header=True);
''')

# %%
# Create DENORMALIZED_SALES table by joining SALES, CUSTOMERS, and PRODUCTS tables
connection.execute('''
    CREATE OR REPLACE TABLE denormalized_sales AS
    SELECT *
    FROM sales s
    LEFT OUTER JOIN customers c ON s.customer_id = c.customer_id
    FULL OUTER JOIN products p ON s.product_id = p.product_id;
''')

# %%
# Export the DENORMALIZED_SALES table to a CSV file
connection.execute(
    "COPY (FROM denormalized_sales) TO 'datasets/denormalized_sales.csv' (HEADER, DELIMITER ',')"
)

# %%
# Close the DuckDB connection
connection.close()
# %%

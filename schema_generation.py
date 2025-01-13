# %%
import duckdb

# %%
# Connect to the DuckDB database
connection = duckdb.connect(database='liquid_duck.db', read_only=False)

# %%
# Create products table
connection.execute('''
    CREATE OR REPLACE TABLE products (
        product_id INTEGER,
        supplier VARCHAR(255),
        brand VARCHAR(255),
        family VARCHAR(255),
        product_name VARCHAR(255),
        product_cost DECIMAL(10,2),
        inventory_volume INTEGER
    );
''')

# %%
# Create customers table
connection.execute('''
    CREATE OR REPLACE TABLE customers (
        customer_id INTEGER,
        customer_name VARCHAR(255),
        customer_address VARCHAR(255),
        customer_phone VARCHAR(255)
    );
''')

# %%
# Create sales table
connection.execute('''
    CREATE OR REPLACE TABLE sales (
        sale_id INTEGER,
        product_id INTEGER,
        customer_id INTEGER,
        sale_date DATE,
        sale_volume INTEGER,
        sale_revenue DECIMAL(10,2)
    );
''')

# %%
# Create a view for denormalized sales
connection.execute('''
    CREATE OR REPLACE VIEW denormalized_sales_view AS
    SELECT *
    FROM sales s
    LEFT OUTER JOIN customers c ON s.customer_id = c.customer_id
    LEFT OUTER JOIN products p ON s.product_id = p.product_id;
''')

# %%
# Create a view for grouping sets of sales revenue and volume
connection.execute('''
    CREATE OR REPLACE VIEW gs_sales_view AS
    SELECT 
        supplier,
        brand,
        family,
        EXTRACT(YEAR FROM sale_date)::INTEGER AS year,
        SUM(sale_volume) AS total_sales,
        SUM(sale_revenue) AS total_revenue
    FROM denormalized_sales_view
    GROUP BY GROUPING SETS (
        (),
        (supplier, year),
        (supplier, brand, year),
        (supplier, brand, family, year),
        (year)
    )
    ORDER BY 
        supplier NULLS FIRST,
        brand NULLS FIRST,
        family NULLS FIRST,
        year NULLS FIRST;
''')

# %%
# Close the DuckDB connection
connection.close()

# %%
# Reopen connection for data insertion
connection = duckdb.connect(database='liquid_duck.db', read_only=False)

# %%
# Insert data into customers table from CSV
connection.execute('''
    INSERT INTO customers
    SELECT *
    FROM read_csv_auto('datasets/customers.csv', header=True);
''')

# %%
# Verify inserted data
df = connection.execute('SELECT * FROM customers').df()
print(df.head(10))

# %%
# Close the DuckDB connection
connection.close()
# %%

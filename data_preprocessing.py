#%%
import pandas as pd
import duckdb as db

con=db.connect(database='liquid_duck.db', read_only=False)

# %%
#Create Products, Customers, Sales tables in duck db from the CSV files. 

con.execute('''CREATE OR REPLACE TABLE PRODUCTS AS
            Select
            "Product_id"::INTEGER           AS Product_id,
            "Supplier"::VARCHAR(255)        AS Supplier,
            "Brand"::VARCHAR(255)           AS Brand,
            "Family"::VARCHAR(255)          AS Family,
            "Product_name"::VARCHAR(255)    AS Product_name,
            "Product_cost"::DECIMAL(10,2)   AS Product_cost,
            "Inventory_volume"::INTEGER     AS Inventory_volume

            FROM read_csv_auto('datasets/products.csv', header=True); 
            ''')


# %%
con.execute('''
            CREATE OR REPLACE TABLE CUSTOMERS AS
            SELECT 
                "Customer_id"::INTEGER              AS Customer_id 
            , "Customer_name"::VARCHAR(255)         AS Customer_name
            , "Customer_address"::VARCHAR(255)      AS Customer_address
            , "Customer_phone"::VARCHAR(255)        AS Customer_phone

            FROM read_csv_auto('datasets/customers.csv', header=True);
            ''')
# %%

con.execute('''
            CREATE OR REPLACE TABLE SALES AS
            SELECT
            "sale_id"::INTEGER              AS Sale_id
            , "Product_id"::INTEGER           AS Product_id
            , "Customer_id"::INTEGER          AS Customer_id
            , "Sale_date"::DATE               AS Sale_date
            , "Sale_volume"::INTEGER          AS Sale_volume
            , "Sale_revenue"::DECIMAL(10,2)   AS Sale_revenue
            FROM read_csv_auto('datasets/sales.csv', header=True);
''')


# %%
con.execute('''
            CREATE OR REPLACE TABLE DENORMALIZED_SALES AS
            SELECT * FROM
           SALES S LEFT OUTER JOIN CUSTOMERS C
            ON S.Customer_id=C.Customer_id 
            FULL OUTER JOIN PRODUCTS P 
            ON S.Product_id=P.Product_id   
            ''')

# %%
con.execute("COPY (FROM DENORMALIZED_SALES) TO 'datasets/denormalized_sales.csv' (HEADER, DELIMITER ',')")

# %%

df=con.execute('''
            SELECT * 
            FROM DENORMALIZED_SALES   
            
            ''').df()

print(df.columns)
print(df.head(30))

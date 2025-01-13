# %%
import duckdb as db
# %%

con = db.connect(database='liquid_duck.db', read_only=False)
# %%
con.execute('''
            CREATE OR replace TABLE products
                        (
                            "Product_id"       integer ,
                            "Supplier"         varchar(255) ,
                            "Brand"            varchar(255) ,
                            "Family"           varchar(255) ,
                            "Product_name"     varchar(255) ,
                            "Product_cost"     decimal(10,2) ,
                            "Inventory_volume" integer
                        ) ;
            ''')

# %%
con.execute('''
            CREATE OR REPLACE TABLE CUSTOMERS
            ( 
                "Customer_id" INTEGER                  
            , "Customer_name"  VARCHAR(255)       
            , "Customer_address" VARCHAR(255)     
            , "Customer_phone" VARCHAR(255)       
            
            );
            ''')

# %%
con.execute('''
            CREATE OR REPLACE TABLE SALES 
            (
            
            "sale_id"   INTEGER              
            , "Product_id"  INTEGER           
            , "Customer_id"  INTEGER          
            , "Sale_date"   DATE              
            , "Sale_volume"  INTEGER          
            , "Sale_revenue" DECIMAL(10,2)   
            );
''')
# %%
# create a view of denormalized sales

con.execute('''
            CREATE OR REPLACE VIEW DENORMALIZED_SALES_VIEW AS
            SELECT * FROM
           SALES S LEFT OUTER JOIN CUSTOMERS C
            ON S.Customer_id=C.Customer_id 
            LEFT OUTER JOIN PRODUCTS P 
            ON S.Product_id=P.Product_id   
            ''')


# %%
# create a view of Grouping_SETS for SALES REVENUE AND SALES VOLUME
con.execute('''
                CREATE OR REPLACE VIEW GS_SALES_VIEW AS
                    SELECT 
                    Supplier
                    , Brand
                                , Family
                                , (EXTRACT(YEAR FROM Sale_date))::INTEGER    AS Year 
                                , sum(Sale_volume)  AS Total_sales
                                , sum(Sale_revenue) AS Total_revenue
                            FROM DENORMALIZED_SALES_VIEW
                            GROUP BY GROUPING SETS
                                ( ()
                                ,  (Supplier, Year)
                                , (Supplier, Brand, Year)
                                , (Supplier, Brand, Family, Year)
                                , (Year)
                                ) 
                                Order BY Supplier NULLS FIRST, Brand NULLS FIRST, Family NULLS FIRST, Year NULLS FIRST;
                                ''')
# %%
con.close()


# ---------------------------------------------------------------------------------------
# %%

con.execute('''
            INSERT INTO CUSTOMERS 
            SELECT 
                *
            FROM read_csv_auto('datasets/customers.csv', header=True);
            ''')
# %%

df = con.execute('SELECT * FROM CUSTOMERS').df()
print(df.head(10))
# %%

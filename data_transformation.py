# %%
import duckdb
# %%

con = duckdb.connect(database='liquid_duck.db', read_only=False)

# %%

denormalized_sales = con.execute('SELECT * FROM DENORMALIZED_SALES').df()
print(denormalized_sales.columns)
# %%
# creating a grouping sets by  (Supplier, Year), (Supplier, Brand, Year), (Supplier, Brand, Family, Year), (Year)

con.execute('''
                CREATE OR REPLACE TABLE SALES_GS AS 
                    SELECT 
                    Supplier, 
                    Brand, 
                    Family, 
                    (
                        EXTRACT(
                        YEAR 
                        FROM 
                            Sale_date
                        )
                    ):: INTEGER AS Year, 
                    sum(Sale_volume) AS total_sales, 
                    sum(Sale_revenue) AS total_revenue 
                    FROM 
                    DENORMALIZED_SALES 
                    GROUP BY 
                    GROUPING SETS(
                        (), 
                        (Supplier, Year), 
                        (Supplier, Brand, Year), 
                        (Supplier, Brand, Family, Year), 
                        (Year)
                    ) 
                    Order BY 
                    Supplier NULLS FIRST, 
                    Brand NULLS FIRST, 
                    Family NULLS FIRST, 
                    Year NULLS FIRST

                                ''')

df_grouping_sets = con.execute('SELECT * FROM SALES_GS').df()
print(df_grouping_sets.head(40))
# %%
# Pivot Sales volume by Brand and year
sales_pivot = con.execute('''
                        SELECT  Brand, "2020","2021","2022","2023","2024"
                        FROM (SELECT  Brand, Year,  SUM(Total_sales) AS Total_sales 
                        FROM SALES_GS GROUP BY Brand, Year, order by Brand, Year)
                        PIVOT( SUM(Total_sales) FOR Year IN ('2020','2021','2022','2023','2024')) AS PIVOTTABLE
                        order by Brand
                        ;
                        
                        ''').df()
print(sales_pivot.head(20))


# %%
# pivot Total_sales by Supplier, Brand, Family, Year
sales_volume_pivot = con.execute('''
                        SELECT  Supplier, Brand, Family, "2020","2021","2022","2023","2024"
                        FROM (SELECT  Supplier, Brand, Family, Year,  SUM(Total_sales) AS Total_sales 
                        FROM SALES_GS GROUP BY Supplier, Brand, Family, Year, order by Supplier,Brand, Family, Year)
                        PIVOT( SUM(Total_sales) FOR Year IN ('2020','2021','2022','2023','2024')) AS PIVOTTABLE
                        order by Supplier NULLS FIRST, Brand NULLS FIRST, Family NULLS FIRST
                        ;
                        
                        ''').df()
print(sales_volume_pivot.head(20))
# %%
# unpivot the above pivot.
sales_volume_unpivot = con.execute('''
                        SELECT  Supplier, Brand, Family, Year, Total_sales
                        FROM (SELECT  Supplier, Brand, Family,  "2020","2021","2022","2023","2024"
                        FROM sales_volume_pivot ) AS Source
                        UNPIVOT( Total_sales FOR Year IN ('2020','2021','2022','2023','2024')) AS UNPIVOTTABLE
                        order by Supplier NULLS FIRST, Brand NULLS FIRST, Family NULLS FIRST
                        ;
                        
                        ''').df()
print(sales_volume_unpivot.head(20))
# %%
# pivot Total_sales by Supplier, Brand, Family, Year
sales_revenue_pivot = con.execute('''
                        SELECT  Supplier, Brand, Family, "2020","2021","2022","2023","2024"
                        FROM (SELECT  Supplier, Brand, Family, Year,  SUM(total_revenue) AS Total_revenue 
                        FROM SALES_GS GROUP BY Supplier, Brand, Family, Year, order by Supplier,Brand, Family, Year)
                        PIVOT( SUM(Total_revenue) FOR Year IN ('2020','2021','2022','2023','2024')) AS PIVOTTABLE
                        order by Supplier NULLS FIRST, Brand NULLS FIRST, Family NULLS FIRST
                        ;
                        
                        ''').df()
print(sales_revenue_pivot.head(20))

# %%
sales_revenue_unpivot = con.execute('''
                        SELECT  Supplier, Brand, Family, Year, Total_revenue
                        FROM (SELECT  Supplier, Brand, Family,  "2020","2021","2022","2023","2024"
                        FROM sales_revenue_pivot ) AS Source
                        UNPIVOT( Total_revenue FOR Year IN ('2020','2021','2022','2023','2024')) AS UNPIVOTTABLE
                        order by Supplier NULLS FIRST, Brand NULLS FIRST, Family NULLS FIRST
                        ;
                        
                        ''').df()
print(sales_revenue_unpivot.head(20))
# %%
con.close()
# %%

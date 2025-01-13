# %%
import duckdb

# %%
# Connect to the DuckDB database
connection = duckdb.connect(database='liquid_duck.db', read_only=False)

# %%
# Retrieve denormalized sales data
denormalized_sales = connection.execute('SELECT * FROM denormalized_sales_view').df()
print(denormalized_sales.columns)

# %%
# Retrieve grouping sets by various combinations
grouping_sets_df = connection.execute('SELECT * FROM gs_sales_view').df()
print(grouping_sets_df.head(40))

# %%
# Pivot sales volume by brand and year
brand_sales_pivot = connection.execute('''
    SELECT Brand, "2020", "2021", "2022", "2023", "2024"
    FROM (
        SELECT Brand, Year, SUM(total_sales) AS total_sales
        FROM gs_sales_view
        GROUP BY Brand, Year
        ORDER BY Brand, Year
    )
    PIVOT(
        SUM(total_sales) FOR Year IN ('2020', '2021', '2022', '2023', '2024')
    ) AS pivot_table
    ORDER BY Brand;
''').df()
print(brand_sales_pivot.head(20))

# %%
# Pivot total sales volume by supplier, brand, family, and year
connection.execute('''
    CREATE OR REPLACE VIEW sales_volume_pivot AS
    SELECT Supplier, Brand, Family, "2020", "2021", "2022", "2023", "2024"
    FROM (
        SELECT Supplier, Brand, Family, Year, SUM(total_sales) AS total_sales
        FROM gs_sales_view
        GROUP BY Supplier, Brand, Family, Year
        ORDER BY Supplier, Brand, Family, Year
    )
    PIVOT(
        SUM(total_sales) FOR Year IN ('2020', '2021', '2022', '2023', '2024')
    ) AS pivot_table
    ORDER BY Supplier NULLS FIRST, Brand NULLS FIRST, Family NULLS FIRST;
''')
sales_volume_pivot = connection.execute('SELECT * FROM sales_volume_pivot').df()
print(sales_volume_pivot.head(20))

# %%
# Unpivot the above pivot table
sales_volume_unpivot = connection.execute('''
    SELECT Supplier, Brand, Family, Year, total_sales
    FROM (
        SELECT Supplier, Brand, Family, "2020", "2021", "2022", "2023", "2024"
        FROM sales_volume_pivot
    ) AS source
    UNPIVOT(
        total_sales FOR Year IN ('2020', '2021', '2022', '2023', '2024')
    ) AS unpivot_table
    ORDER BY Supplier NULLS FIRST, Brand NULLS FIRST, Family NULLS FIRST;
''').df()
print(sales_volume_unpivot.head(20))

# %%
# Pivot total sales revenue by supplier, brand, family, and year
connection.execute('''
    CREATE OR REPLACE VIEW sales_revenue_pivot AS
    SELECT Supplier, Brand, Family, "2020", "2021", "2022", "2023", "2024"
    FROM (
        SELECT Supplier, Brand, Family, Year, SUM(total_revenue) AS total_revenue
        FROM gs_sales_view
        GROUP BY Supplier, Brand, Family, Year
        ORDER BY Supplier, Brand, Family, Year
    )
    PIVOT(
        SUM(total_revenue) FOR Year IN ('2020', '2021', '2022', '2023', '2024')
    ) AS pivot_table
    ORDER BY Supplier NULLS FIRST, Brand NULLS FIRST, Family NULLS FIRST;
''')
sales_revenue_pivot = connection.execute('SELECT * FROM sales_revenue_pivot').df()
print(sales_revenue_pivot.head(20))

# %%
# Unpivot the sales revenue pivot table
sales_revenue_unpivot = connection.execute('''
    SELECT Supplier, Brand, Family, Year, total_revenue
    FROM (
        SELECT Supplier, Brand, Family, "2020", "2021", "2022", "2023", "2024"
        FROM sales_revenue_pivot
    ) AS source
    UNPIVOT(
        total_revenue FOR Year IN ('2020', '2021', '2022', '2023', '2024')
    ) AS unpivot_table
    ORDER BY Supplier NULLS FIRST, Brand NULLS FIRST, Family NULLS FIRST;
''').df()
print(sales_revenue_unpivot.head(20))

# %%
# Close the DuckDB connection
connection.close()

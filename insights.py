# %%
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
# %%
con = duckdb.connect(database='liquid_duck.db', read_only=True)
# %%
# Total sales Revenue of each year
total_revenue_by_year = con.execute('''
            SELECT "2020","2021","2022","2023","2024"
            FROM sales_revenue_pivot
            WHERE Supplier IS NULL
            AND Brand IS NULL
            AND Family IS NULL
            ;
''').df()

print(total_revenue_by_year.head(10))
# plot a bar chart
total_revenue_by_year.plot(kind='bar')

plt.title('Total Sales Revenue by Year')
plt.xlabel('Year')
plt.ylabel('Revenue')
plt.show()
# %%
# Total sales volume of each year


# %%
# brands with highest sales volume each year

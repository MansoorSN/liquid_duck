# %%
import pandas as pd
import random
from faker import Faker
from datetime import date

# %%
fake = Faker()
fake.seed_instance(2025)


def generate_products(num=50):
    """
    Generate a DataFrame of product data.

    Args:
        num (int): Number of products to generate. Default is 50.

    Returns:
        pd.DataFrame: DataFrame containing product data.
    """
    suppliers = ["A", "B", "C", "D", "E"]
    brands = ["Coca-Cola", "Pepsi", "Nestle", "DrPepper", "Monster"]
    families = ["Soda", "Water", "Juice", "Energy_drink", "Tea"]
    product_list = []

    for i in range(1, num + 1):
        product_id = i
        supplier = random.choice(suppliers)
        brand = random.choice(brands)
        family = random.choice(families)
        product_name = f"{brand} {family}"
        product_cost = random.randint(1, 10)
        inventory_volume = random.randint(0, 1000)

        product_list.append({
            "product_id": product_id,
            "supplier": supplier,
            "brand": brand,
            "family": family,
            "product_name": product_name,
            "product_cost": product_cost,
            "inventory_volume": inventory_volume
        })

    return pd.DataFrame(product_list)


# %%
def generate_customers(num=20):
    """
    Generate a DataFrame of customer data.

    Args:
        num (int): Number of customers to generate. Default is 20.

    Returns:
        pd.DataFrame: DataFrame containing customer data.
    """
    customer_list = []

    for i in range(1, num + 1):
        customer_name = fake.company()
        customer_address = fake.address()
        customer_phone = fake.phone_number()

        customer_list.append({
            "customer_id": i,
            "customer_name": customer_name,
            "customer_address": customer_address,
            "customer_phone": customer_phone
        })

    return pd.DataFrame(customer_list)


# %%
def generate_sales(products_df, customers_df, num=100):
    """
    Generate a DataFrame of sales data.

    Args:
        products_df (pd.DataFrame): DataFrame containing product data.
        customers_df (pd.DataFrame): DataFrame containing customer data.
        num (int): Number of sales records to generate. Default is 100.

    Returns:
        pd.DataFrame: DataFrame containing sales data.
    """
    sales_list = []

    start_date = date(2020, 1, 1)
    end_date = date(2024, 12, 31)

    for i in range(1, num + 1):
        product = products_df.sample(1).iloc[0]
        customer = customers_df.sample(1).iloc[0]

        sale_volume = random.randint(10, 500)
        sale_revenue = round(sale_volume * random.uniform(2, 30), 2)

        sales_list.append({
            "sale_id": i,
            "product_id": product["product_id"],
            "customer_id": customer["customer_id"],
            "sale_date": fake.date_between(start_date=start_date, end_date=end_date),
            "sale_volume": sale_volume,
            "sale_revenue": sale_revenue
        })

    return pd.DataFrame(sales_list)


# %%
# Generate data
products_df = generate_products(num=20)
customers_df = generate_customers(num=20)
sales_df = generate_sales(products_df, customers_df, num=100)

# Display sample sales data
print(sales_df.head())

# %%
# Save data to CSV files
products_df.to_csv(r"datasets/products.csv", header=True, index=False)
customers_df.to_csv(r"datasets/customers.csv", header=True, index=False)
sales_df.to_csv(r"datasets/sales.csv", header=True, index=False)

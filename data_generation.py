#%%
import pandas as pd
import random
from faker import Faker
from datetime import date

# %%
fake = Faker()
Faker.seed(2025)
def generate_products(num=50):
    suppliers = ["A", "B", "C", "D", "E"]
    brands = ["Coca-Cola", "Pepsi", "Nestle", "DrPepper", "Monster"]
    families = ["Soda", "Water", "Juice", "Energy_drink", "Tea"]
    product_list=[]
    for i in range(1, num+1):
        Product_id=i
        Supplier=random.choices(suppliers)
        Brand=random.choices(brands)
        Family=random.choices(families)
        Product_name=f"{Brand} {Family}"
        Product_cost=random.randint(1,10)
        Inventory_volume=random.randint(0,1000)

        product_list.append({
            "Product_id":Product_id,
            "Supplier":Supplier,
            "Brand":Brand,
            "Family":Family,
            "Product_name":Product_name,
            "Product_cost":Product_cost,
            "Inventory_volume":Inventory_volume})

    return pd.DataFrame(product_list)
    

# %%
fake = Faker()
Faker.seed(2025)

def generate_customers(num=50):
    customer_list=[]

    for i in range(1, num+1):
        company_name=fake.company()
        company_address=fake.address()
        company_phone=fake.phone_number()
        customer_list.append({
        "Customer_id":i,
        "Customer_name":company_name,
        "Customer_address":company_address,
        "Customer_phone":company_phone
        })

    return pd.DataFrame(customer_list)  


# %%
def generate_sales(products_df, customers_df, num=5000):
    sales_list=[]

    start_date = date(2020, 1, 1)  
    end_date = date(2024, 12, 31)


    for i in range(1,num+1):
        product=products_df.sample(1).iloc[0]
        customer=customers_df.sample(1).iloc[0]

        volume=random.randint(10,500)
        revenue=round(volume*random.uniform(2,30),2)

        sales_list.append({
            "sale_id":i,
            "Product_id":product["Product_id"],
            "Customer_id":customer["Customer_id"],
            "Sale_date":fake.date_between(start_date=start_date, end_date=end_date),
            "Sale_volume":volume,
            "Sale_revenue":revenue
        })

    return pd.DataFrame(sales_list)



# %%
products_df=generate_products(num=50)
customers_df=generate_customers(num=50)
sales_df=generate_sales(products_df, customers_df, num=1000)
print(sales_df.head())
# %%
products_df.to_csv(r"datasets/products.csv")
customers_df.to_csv(r"datasets/customers.csv")
sales_df.to_csv(r"datasets/sales.csv")
# %%

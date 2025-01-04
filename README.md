# liquid_duck
Liquid Duck Project

The assignment can be divided into the following tasks:
- Data set Creation
- Data Preprocessing loading
- Data transformation in duckdb using grouping sets, pivot, unpivot
- Redis stream implementation


  Schema of the initial source tables:
  Schema:

Sales:
	- Sale_id
	- Product_id
	- Customer_id
	- Sale_date
	- Sale_volume
	- Sale_revenue
	
Product:
	- Product_id
	- Supplier
	- Brand
	- Family
	- Product_name
	- Product_cost
	- Inventory_volume
	
Customer:
	- Customer_id
	- Customer_name
	- Customer_address
	- Customer_phone

![image](https://github.com/user-attachments/assets/5d9c6585-37df-4550-9b1e-8e2fc1783db6)


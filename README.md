# Problem Statement : Process real time sales data to obtain latest results of sales data across Suppliers, Brands and Family of products

We have a realtime data of Products, Customers and Sales coming from multiple sources. We need to ingest this data, Transform the data to obtain the insights and load it to a Target table.
To extract the data from the data sources in real time we create a websocket server that esatblishes a websocket connection and writes the data to a Redis server. The redis server acts as message queue decoupling the source from the consumer that consumes the source data. The consumer consumes from the redis queue and writes the data to duckdb tables.
The data obtained is transformed by first creating a denormalized view and then applying Grouping Sets and pivot operations to transform the data to obtain the desired performance indicators.

Performance Indicators : Sales Volume and Sales Revenue of Suppliers, Brands and Families of Products by Year.



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

Workflow:
![Alt text](workflow.jpg)

**Schema Generation:**
Sales, Products, Customers table are created and a denormalized and Grouping Sets views are created.
We have used views instead of tables, to get the latest data every time we run a query.

**Data Generation:**
I have used Faker library for creating dataset. 3 datatsets representing Sales, Products and Customers were created and saved in csv files.

**Real time data simulation using Tornado Websocket Server and Redis:**
To simulate streaming data from a websocket client, the websocket client takes data from the csv files and sends it to the websocket-server.
The websocket-server receives the data and pushes the data to the Redis stream. Redis-consumer consumes from the redis queue and inserts the data to the appropriate tables.

**Data Transformation:**
The Grouping set and Pivot views are created for Total revenue and Total sales.


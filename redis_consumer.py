import redis
import duckdb
import json


# DuckDB Configuration
duckdb_db = 'liquid_duck.db'

# Redis streams and consumer group
streams = {
    "products_stream": "PRODUCTS",
    "customers_stream": "CUSTOMERS",
    "sales_stream": "SALES",
}
group_name = "consumer_group"
consumer_name = "consumer_1"

# Connect to Redis
r = redis.Redis(host='localhost', port=6380, db=0)

# Connect to DuckDB and set up tables if they don't exist
conn = duckdb.connect(duckdb_db, read_only=False)

# Ensure the consumer group exists
for stream in streams:
    try:
        r.xgroup_create(stream, group_name, id='0', mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            print(
                f"Consumer group '{group_name}' already exists for stream '{stream}'.")
        else:
            raise


def consume_from_redis():
    """
    Reads messages from Redis streams and writes them to the appropriate DuckDB tables.
    """
    print(f"Starting consumer '{consumer_name}'...")
    while True:
        try:
            # Read from all streams
            stream_queries = {stream: '>' for stream in streams.keys()}
            messages = r.xreadgroup(
                group_name, consumer_name, stream_queries, count=10, block=5000)

            # print(stream_queries)

            for stream, entries in messages:
                for message_id, data in entries:
                    # Parse the message
                    record = {k.decode('utf-8'): v.decode('utf-8')
                              for k, v in data.items()}
                    stream = stream.decode('utf-8')
                    print(f"Received message from {stream}: {record}")

                    # Insert data into the appropriate DuckDB table
                    if stream == "products_stream":
                        conn.execute(f"""
                            INSERT INTO PRODUCTS (Product_id, Supplier, Brand, Family, Product_name, Product_cost, Inventory_volume)   
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (
                            int(record.get('Product_id')),
                            record.get('Supplier'),
                            record.get('Brand'),
                            record.get('Family'),
                            record.get('Product_name'),
                            float(record.get('Product_cost', 0.00)),
                            int(record.get('Inventory_volume', 0))
                        ))
                        print("Inserted into PRODUCTS:", conn.execute(
                            "SELECT * FROM PRODUCTS").fetchdf())

                    elif stream == "customers_stream":
                        conn.execute(f"""
                            INSERT INTO CUSTOMERS (Customer_id, Customer_name, Customer_address, Customer_phone)
                            VALUES (?, ?, ?, ?)
                        """, (
                            int(record.get('Customer_id')),
                            record.get('Customer_name'),
                            record.get('Customer_address'),
                            record.get('Customer_phone')
                        ))
                        print("Inserted into CUSTOMERS:", conn.execute(
                            "SELECT * FROM CUSTOMERS").fetchdf())

                    elif stream == "sales_stream":

                        conn.execute(f"""
                            INSERT INTO SALES (sale_id, Product_id, Customer_id, Sale_date, Sale_volume, Sale_revenue)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            int(record.get('sale_id')),
                            int(record.get('Product_id')),
                            int(record.get('Customer_id')),
                            record.get('Sale_date'),
                            int(record.get('Sale_volume', 0)),
                            float(record.get('Sale_revenue', 0.00))
                        ))
                        print("Inserted into SALES:", conn.execute(
                            "SELECT * FROM SALES").fetchdf())

                    # Acknowledge the message in Redis
                    r.xack(stream, group_name, message_id)

                    print("consumers table:", conn.execute(
                        'select * from CUSTOMERS').df().head(10))

        except Exception as e:
            print(f"Error while consuming messages: {e}")


# Run the consumer
if __name__ == "__main__":
    consume_from_redis()

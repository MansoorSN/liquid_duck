import redis
import duckdb

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
redis_client = redis.Redis(host='localhost', port=6380, db=0)

# Connect to DuckDB and set up tables if they don't exist
duckdb_connection = duckdb.connect(duckdb_db, read_only=False)

# Ensure the consumer group exists
for stream in streams:
    try:
        redis_client.xgroup_create(stream, group_name, id='0', mkstream=True)
    except redis.exceptions.ResponseError as error:
        if "BUSYGROUP" in str(error):
            print(f"Consumer group '{group_name}' already exists for stream '{stream}'.")
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
            messages = redis_client.xreadgroup(
                group_name, consumer_name, stream_queries, count=10, block=5000
            )

            for stream, entries in messages:
                stream_name = stream.decode('utf-8')

                for message_id, data in entries:
                    # Parse the message
                    record = {k.decode('utf-8'): v.decode('utf-8') for k, v in data.items()}
                    print(f"Received message from {stream_name}: {record}")

                    # Insert data into the appropriate DuckDB table
                    if stream_name == "products_stream":
                        duckdb_connection.execute(
                            """
                            INSERT INTO PRODUCTS (Product_id, Supplier, Brand, Family, Product_name, Product_cost, Inventory_volume)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                int(record.get('Product_id')),
                                record.get('Supplier'),
                                record.get('Brand'),
                                record.get('Family'),
                                record.get('Product_name'),
                                float(record.get('Product_cost', 0.00)),
                                int(record.get('Inventory_volume', 0))
                            )
                        )
                        print("Inserted into PRODUCTS")

                    elif stream_name == "customers_stream":
                        duckdb_connection.execute(
                            """
                            INSERT INTO CUSTOMERS (Customer_id, Customer_name, Customer_address, Customer_phone)
                            VALUES (?, ?, ?, ?)
                            """,
                            (
                                int(record.get('Customer_id')),
                                record.get('Customer_name'),
                                record.get('Customer_address'),
                                record.get('Customer_phone')
                            )
                        )
                        print("Inserted into CUSTOMERS")

                    elif stream_name == "sales_stream":
                        duckdb_connection.execute(
                            """
                            INSERT INTO SALES (sale_id, Product_id, Customer_id, Sale_date, Sale_volume, Sale_revenue)
                            VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            (
                                int(record.get('sale_id')),
                                int(record.get('Product_id')),
                                int(record.get('Customer_id')),
                                record.get('Sale_date'),
                                int(record.get('Sale_volume', 0)),
                                float(record.get('Sale_revenue', 0.00))
                            )
                        )
                        print("Inserted into SALES")

                    # Acknowledge the message in Redis
                    redis_client.xack(stream_name, group_name, message_id)

        except Exception as error:
            print(f"Error while consuming messages: {error}")

if __name__ == "__main__":
    consume_from_redis()

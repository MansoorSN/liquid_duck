import asyncio
import websockets
import json
import pandas as pd


async def websocket_client():
    """
    Connects to a WebSocket server and sends data from CSV files.

    The function reads data from products, customers, and sales CSV files,
    then sends the data to the WebSocket server in JSON format.
    """
    uri = "ws://localhost:8888/ws"  # WebSocket server URL

    # Read CSV files into pandas DataFrames
    products_df = pd.read_csv(r"datasets/products.csv",
                              header=0, index_col=None)
    customers_df = pd.read_csv(
        r"datasets/customers.csv", header=0, index_col=None)
    sales_df = pd.read_csv(r"datasets/sales.csv", header=0, index_col=None)

    try:
        # Connect to the WebSocket server
        async with websockets.connect(uri) as websocket:
            print("Connected to WebSocket server")

            # Send data from products CSV
            for _, row in products_df.iterrows():
                message = {"type": "products", **row.to_dict()}
                await websocket.send(json.dumps(message))
                print(f"Sent: {message}")
                response = await websocket.recv()
                print(f"Received: {response}")
                await asyncio.sleep(1)

            # Send data from customers CSV
            for _, row in customers_df.iterrows():
                message = {"type": "customers", **row.to_dict()}
                await websocket.send(json.dumps(message))
                print(f"Sent: {message}")
                response = await websocket.recv()
                print(f"Received: {response}")
                await asyncio.sleep(1)

            # Send data from sales CSV
            for _, row in sales_df.iterrows():
                message = {"type": "sales", **row.to_dict()}
                await websocket.send(json.dumps(message))
                print(f"Sent: {message}")
                response = await websocket.recv()
                print(f"Received: {response}")
                await asyncio.sleep(1)

            print("All messages sent. Closing connection.")
    except Exception as error:
        print(f"Error connecting to WebSocket server: {error}")


# Run the client
if __name__ == "__main__":
    asyncio.run(websocket_client())

# --
import redis
import tornado.ioloop
import tornado.web
import tornado.websocket
import json
from faker import Faker

# %%
r = redis.Redis(host='localhost', port=6380, db=0)


# %%

class MyWebSocket(tornado.websocket.WebSocketHandler):

    def open(self):
        print("WebSocket connection opened")

    def on_message(self, message):
        """
        Called when a client sends a message to this endpoint.
        We expect `message` to be a JSON string.
        """
        try:
            data = json.loads(message)

           # check for the appropriate message type and process it
            message_type = data.get("type")
            if message_type == "products":
                self.process_product_data(data)
            elif message_type == "customers":
                self.process_order_data(data)
            elif message_type == "sales":
                self.process_inventory_data(data)
            else:
                self.write_message("Unknown data type")
                return

            self.write_message(f"Processed {message_type} data successfully.")
        except Exception as e:
            print(f"Error processing message: {e}")
            self.write_message(f"Error: {str(e)}")

    def process_product_data(self, data):
        print(f"Processing products data: {data}")
        r.xadd("products_stream", {k: str(v) for k, v in data.items()})

    def process_order_data(self, data):

        print(f"Processing Customers data: {data}")
        r.xadd("customers_stream", {k: str(v) for k, v in data.items()})

    def process_inventory_data(self, data):

        print(f"Processing sales data: {data}")
        r.xadd("sales_stream", {k: str(v) for k, v in data.items()})

    def on_close(self):
        print("WebSocket connection closed")

# %%


def make_app():
    return tornado.web.Application([
        (r"/ws", MyWebSocket),  # WebSocket route
    ])


# %%
if __name__ == "__main__":
    app = make_app()
    app.listen(8888)  # start Tornado on localhost:8888
    print("Tornado WebSocket server running on http://localhost:8888/ws")
    print("redis is running on localhost:6380", r.ping())
    tornado.ioloop.IOLoop.current().start()
# %%

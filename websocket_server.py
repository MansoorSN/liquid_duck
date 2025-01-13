import redis
import tornado.ioloop
import tornado.web
import tornado.websocket
import json

# Initialize Redis connection
redis_client = redis.Redis(host='localhost', port=6380, db=0)


class WebSocketHandler(tornado.websocket.WebSocketHandler):
    """
    A WebSocket handler to process and route incoming messages to Redis streams
    based on their type.
    """

    def open(self):
        """Called when a WebSocket connection is opened."""
        print("WebSocket connection opened")

    def on_message(self, message):
        """
        Called when a client sends a message to this endpoint.

        Args:
            message (str): JSON string containing the message data.
        """
        try:
            data = json.loads(message)

            # Determine message type and process accordingly
            message_type = data.get("type")
            if message_type == "products":
                self.process_product_data(data)
            elif message_type == "customers":
                self.process_customer_data(data)
            elif message_type == "sales":
                self.process_sales_data(data)
            else:
                self.write_message("Unknown data type")
                return

            self.write_message(f"Processed {message_type} data successfully.")
        except Exception as error:
            print(f"Error processing message: {error}")
            self.write_message(f"Error: {str(error)}")

    def process_product_data(self, data):
        """Process and add product data to the Redis stream."""
        print(f"Processing product data: {data}")
        redis_client.xadd("products_stream", {
                          k: str(v) for k, v in data.items()})

    def process_customer_data(self, data):
        """Process and add customer data to the Redis stream."""
        print(f"Processing customer data: {data}")
        redis_client.xadd("customers_stream", {
                          k: str(v) for k, v in data.items()})

    def process_sales_data(self, data):
        """Process and add sales data to the Redis stream."""
        print(f"Processing sales data: {data}")
        redis_client.xadd("sales_stream", {k: str(v) for k, v in data.items()})

    def on_close(self):
        """Called when the WebSocket connection is closed."""
        print("WebSocket connection closed")


def make_app():
    """
    Create and return a Tornado web application.

    Returns:
        tornado.web.Application: Tornado application instance.
    """
    return tornado.web.Application([
        (r"/ws", WebSocketHandler),
    ])


if __name__ == "__main__":
    app = make_app()
    app.listen(8888)  # Start Tornado on localhost:8888
    print("Tornado WebSocket server running on http://localhost:8888/ws")
    print("Redis is running on localhost:6380", redis_client.ping())
    tornado.ioloop.IOLoop.current().start()

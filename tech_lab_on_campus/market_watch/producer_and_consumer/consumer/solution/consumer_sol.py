# Create a class mqConsumer
# Below are bullet points of the criteria:
# - In the `solution` directory, create a file named `consumer_sol.py`
# - Write your code in the `consumer_sol.py` file
# - Create a class named `mqConsumer`
# - Your class should [inherit](../../Resources/Python-Basics.md#creating-an-interface) from our mqConsumerInterface.
# - Constructor:[Save the three variables](../../Resources/Python-Basics.md#saving-a-instance-variable-and-calling-the-variable) needed to instantiate the class.
# - Constructor: Call the setupRMQConnection function.
# - setupRMQConnection Function: Establish connection to the RabbitMQ service, declare a queue and exchange, bind the binding key to the queue on the exchange and finally set up a callback function for receiving messages
# - onMessageCallback: Print the UTF-8 string message and then close the connection.
# - startConsuming:  Consumer should start listening for messages from the queue.
# - Del: Close Connection and Channel.

import pika
import os

from consumer_interface import mqConsumerInterface

class mqConsumer(mqConsumerInterface):

    def __init__(self, binding_key, queue_name, exchange_name) -> None:

        self.binding_key = binding_key
        self.queue_name = queue_name
        self.exchange_name  = exchange_name

        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:

        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)

        # Establish Channel
        self.channel = self.connection.channel()

        # Create Queue if not already present
        self.channel.queue_declare(queue_name=self.queue_name)

        # Create the exchange if not already present
        self.channel.exchange_declare(exchange=self.exchange_name)

        # Bind Binding Key to Queue on the exchange
        self.channel.queue_bind(queue_name=self.queue_name, exchange_name=self.exchange_name, binding_key=self.binding_key)

        # Set-up Callback function for receiving messages
        self.channel.basic_consume(queue_name=self.queue_name, on_message_callback=self.on_message_callback)
    
    def on_message_callback(
        self, channel, method_frame, header_frame, body
    ) -> None:

        # Acknowledge message
        self.channel.basic_ack(delivery_tag=method_frame.delivery_tag, multiple=False)

        # Print message (The message is contained in the body parameter variable)
        print(body.decode("utf-8"))
    
    def startConsuming(self) -> None:

        # Print " [*] Waiting for messages. To exit press CTRL+C"
        print("[*] Waiting for messages. To exit press CTRL+C")

        # Start consuming messages
        self.channel.start_consuming()
    
    def __del__(self) -> None:

        # Print "Closing RMQ connection on destruction"
        print("Closing RMQ connection on destruction")

        # Close Channel
        if hasattr(self, 'channel'):
            self.channel.close()

        # Close Connection
        if hasattr(self, 'connection'):
            self.connection.close()
    


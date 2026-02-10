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

    def __init__(self, v1, v2, v3) -> None:

        self.binding = v1
        self.queue = v2
        self.exchange = v3

        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:

        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)

        # Establish Channel
        self.channel = self.connection.channel()

        # Create Queue if not already present
        self.channel.queue_declare(queue=self.queue)

        # Create the exchange if not already present
        self.channel.exchange_declare(exchange=self.exchange)

        # Bind Binding Key to Queue on the exchange
        self.channel.queue_bind(queue=self.queue, exchange=self.exchange, routing_key=self.binding)

        # Set-up Callback function for receiving messages
        self.channel.basic_consume(queue=self.queue, on_message_callback=self.on_message_callback)

        pass
    
    def on_message_callback(
        self, channel, method_frame, header_frame, body
    ) -> None:

        # Acknowledge message
        self.channel.basic_ack(delivery_tag=method_frame.delivery_tag, False)

        # Print message (The message is contained in the body parameter variable)
        print(body.decode("utf-8"))

        pass
    
    def startConsuming(self) -> None:

        # Print " [*] Waiting for messages. To exit press CTRL+C"
        print("[*] Waiting for messages. To exit press CTRL+C")

        # Start consuming messages
        self.channel.start_consuming()
        pass
    
    def __del__(self) -> None:

        # Print "Closing RMQ connection on destruction"
        print("Closing RMQ connection on destruction")

        # Close Channel
        self.channel.close()

        # Close Connection
        self.connection.close()

        pass
    


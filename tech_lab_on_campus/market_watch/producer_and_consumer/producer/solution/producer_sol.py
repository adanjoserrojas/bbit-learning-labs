'''
Below are bullet points of the criteria:

In the solution directory, create a file named producer_sol.py
Write your code in the producer_sol.py file
Create a class named mqProducer
Your class should inherit from our mqProducerInterface.
Constructor: Save the two variables needed to instantiate the class.
Constructor: Call the setupRMQConnection function.
setupRMQConnection Function: Establish connection to the RabbitMQ service.
publishOrder: Publish a simple UTF-8 string message from the parameter.
publishOrder: Close Channel and Connection.
'''
import os
import pika
from producer_interface import mqProducerInterface

class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.connection = None
        self.channel = None
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        cons_params = pika.URLParameters('amqp://guest:guest@localhost:5672/%2F')
        self.connection = pika.BlockingConnection(cons_params)
        # Establish Channel
        self.channel = self.connection.channel()
        # Create the exchange if not already present
        self.exchange = self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct', durable=True)

    def publishOrder(self, message: str) -> None:
        # Basic Publish to Exchange
        self.channel.basic_publish(exchange=self.exchange_name,
                                   routing_key=self.routing_key,
                                   body=message.encode('utf-8'))
        # Close Channel
        self.channel.close()
        # Close Connection
        self.connection.close()
    


from consumer_interface import mqConsumerInterface

import pika
import os


class mqConsumer(mqConsumerInterface):
    def __init__(self, exchange_name) -> None:
        # Save parameters to class variables
        # self.binding_key = binding_key
        self.exchange_name = exchange_name
        # self.queue_name = queue_name
        self.setupRMQConnection()

    def setupRMQConnection(self):
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        self.channel = self.connection.channel()
        # exchange = channel.exchange_declare(exchange="Exchange Name")

        # self.channel.queue_declare(queue=self.queue_name)

        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type="topic")

        # self.channel.queue_bind(
        #     queue= self.queue_name,
        #     routing_key= self.binding_key,
        #     exchange= self.exchange_name,
        # )
        # self.channel.basic_consume(
        #     self.queue_name, self.on_message_callback , auto_ack=False
        # )
        # pass

    def bindQueueToExchange(self, queueName: str, topic: str) -> None:
        self.channel.queue_bind(
            queue= queueName,
            routing_key=topic,
            exchange=self.exchange_name,
        )

    def createQueue(self, queueName: str) -> None:
        self.channel.queue_declare(queue=queueName)
        self.channel.basic_consume(
            queueName, self.on_message_callback , auto_ack=False
        )

    def on_message_callback(
        self, channel, method_frame, header_frame, body
    ) -> None:
        # Acknowledge message
        channel.basic_ack(method_frame.delivery_tag, False)
        #Print message (The message is contained in the body parameter variable)
        print(body)
        pass

    def startConsuming(self) -> None:
        # Print " [*] Waiting for messages. To exit press CTRL+C"
        print(" [*] Waiting for messages. To exit press CTRL+C")
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


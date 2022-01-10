import json
import pika
from dotenv import load_dotenv, find_dotenv
import os

class RabbitServer:
    def __init__(self):
        self.host_mq = os.environ.get('RABBITMQ_HOST')
        self.port_mq = int(os.environ.get('RABBITMQ_PORT'))
        self.user_mq = os.environ.get('RABBITMQ_USER')
        self.password_mq = os.environ.get('RABBITMQ_PASSWORD')
        self.task_queue = os.environ.get('RABBITMQ_TASK_QUEUE')

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.host_mq, port=self.port_mq, credentials=pika.PlainCredentials(self.user_mq, self.password_mq))
        )

        self.channel = self.connection.channel()
        self.channel.queue_declare(self.task_queue)


    def handle_message(self, ch, method, properties, body):
        print('Hello, I am handle you')
        
        message = json.loads(body)
        print(message)
      
        flask_url = os.environ.get('Flask_URL')
        print(flask_url)

        self.channel.basic_ack(method.delivery_tag)
        return

        #запрос по html к апи фласка.
        response = requests.get(url).json

        if respones.status_code == requests.codes.ok:
            print('Ура, задача выполнена, ее нужно удалить из очереди')
            self.channel.basic_ack(method.delivery_tag)
        else:
            print('Что то пошло не так, задача не выполнена')


    def run_server(self):
        self.channel.basic_consume(
            queue=self.task_queue, on_message_callback=self.handle_message, auto_ack=False
        )

        print(f"RabbitMQ Server is waiting for messages on {self.host_mq}:{self.port_mq}")
        self.channel.start_consuming()


def run():
    server = RabbitServer()
    server.run_server()

# load .env file
load_dotenv(find_dotenv())

#run server
run()
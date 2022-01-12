import json
import pika
from dotenv import load_dotenv, find_dotenv
import os
import requests
import time

class RabbitServer:
    def __init__(self):
        self.host_mq = os.environ.get('RABBITMQ_HOST')
        self.port_mq = int(os.environ.get('RABBITMQ_PORT'))
        self.user_mq = os.environ.get('RABBITMQ_USER')
        self.password_mq = os.environ.get('RABBITMQ_PASSWORD')
        self.task_queue = os.environ.get('RABBITMQ_TASK_QUEUE')

        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.host_mq, port=self.port_mq, credentials=pika.PlainCredentials(self.user_mq, self.password_mq))
            )

            self.channel = self.connection.channel()
            self.channel.queue_declare(self.task_queue)

        except Exception as e:
            print("не удалось подключится к очереди сообщений")
            print(e)


    def handle_message(self, ch, method, properties, body):         
        message = json.loads(body)
        print('RabbitMQ handle message with params', message)

        if 'handle' not in message:
            print("message doesn't have handle")
            self.channel.basic_ack(method.delivery_tag)
            return

        if 'sid' not in message:
            print("message doesn't have sid")
            self.channel.basic_ack(method.delivery_tag)
            return

        flask_url = os.environ.get('FLASK_URL')
        flask_url = f'http://{flask_url}/' + "make_task"

        while True:
            try:
                #запрос по html к апи фласка.
                response = requests.post(flask_url, json = {"handle" : message['handle'], "sid" : message['sid']})

                if response.status_code == requests.codes.ok:
                    print('Ура, задача выполнена и удалена из очереди')
                    self.channel.basic_ack(method.delivery_tag)
                else:
                    print(f'Что то пошло не так, задача не выполнена. Ответ сервера: {response.status_code}')
                    self.channel.basic_ack(method.delivery_tag)

                return
            except Exception as e:
                print("Flask server is not available")
                print(f"Код ошибки : {e}")
            time.sleep(3)
        
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
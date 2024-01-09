# consumer
from functools import reduce
import json
import pika

from celery_consumer import add


def listenOnFromHamiltonQueue():
    credentials = pika.PlainCredentials("guest", "guest")

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host="dev-aics-tfp-002", port=80, credentials=credentials
        )
    )
    channel = connection.channel()
    channel.exchange_declare("test", durable=True, exchange_type="topic")
    channel.queue_declare(queue="C")
    channel.queue_declare(queue="D")
    channel.basic_consume(
        queue="C",
        on_message_callback=consumeFileListFromVenus,
        auto_ack=True,
    )
    # this will be command for starting the consumer session
    channel.start_consuming()


def consumeFileListFromVenus(ch, method, properties, body):
    # Recieve the message from Venus fileQueue, and deserialize it
    print(f"Got a message from Queue C: {body}")
    file_list = json.loads(body)
    responses = []

    # Send each file to the celery queue, where this will be processed by the worker(s) concurrently
    for file in file_list:
        print(f"processing file {file}")
        responses.append(add.delay(1, 1))

    # Wait for all the responses to come back, and then reduce them to a single result
    print("Waiting for responses")
    results = map(lambda r: r.get(), responses)
    print("Got all responses")
    mock_cell_count = reduce(lambda x, y: x + y, results)

    print(f"Got a cell count of {mock_cell_count}")
    produceCellCountforVenus(mock_cell_count)


def produceCellCountforVenus(cell_count):
    credentials = pika.PlainCredentials("guest", "guest")

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host="dev-aics-tfp-002", port=80, credentials=credentials
        )
    )
    channel = connection.channel()
    channel.exchange_declare("hamilton_local", durable=True, exchange_type="topic")
    channel.queue_declare(queue="test")
    channel.queue_bind(exchange="test", queue="D", routing_key="D")

    channel.basic_publish(
        exchange="test", routing_key="D", body=json.dumps({"cell_count": cell_count})
    )
    channel.close()


def main():
    listenOnFromHamiltonQueue()


if __name__ == "__main__":
    main()

# consumer
from functools import reduce
import json
import pika

from celery_worker import add
from hamilton_queue import EXCHANGE_HAMILTON, QUEUE_CELL_COUNT, QUEUE_FILE_LIST


#########################################
# Consumer of the message from Hamilton
def consumeFileListFromHamilton(ch, method, properties, body):
    print(f"Got a message from Queue C: {body}")
    file_list = json.loads(body)
    mock_cell_count = produceFilePathsForCelery(file_list)
    print(f"Got a cell count of {mock_cell_count}")
    produceCellCountforHamilton(mock_cell_count)


def listenOnFromHamiltonQueue():
    credentials = pika.PlainCredentials("guest", "guest")

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host="dev-aics-tfp-002", port=80, credentials=credentials
        )
    )
    channel = connection.channel()
    channel.exchange_declare(
        exchange=EXCHANGE_HAMILTON, durable=True, exchange_type="direct"
    )
    channel.queue_declare(queue=QUEUE_FILE_LIST, durable=True)
    channel.queue_bind(
        exchange=EXCHANGE_HAMILTON, queue=QUEUE_FILE_LIST, routing_key=QUEUE_FILE_LIST
    )
    channel.basic_consume(
        queue=QUEUE_FILE_LIST,
        on_message_callback=consumeFileListFromHamilton,
        auto_ack=True,
    )
    # this will be command for starting the consumer session
    channel.start_consuming()


#########################################
# Producer of the message to Celery workers
def produceFilePathsForCelery(file_list):
    responses = []
    # Send each file to the celery queue, where this will be processed by the worker(s) concurrently
    for file in file_list:
        print(f"processing file {file}")
        responses.append(add.delay(1, 1))

    # Wait for all the responses to come back, and then reduce them to a single result
    print("Waiting for responses")
    results = map(lambda r: r.get(), responses)
    print("Got all responses")
    return reduce(lambda x, y: x + y, list(results))


#########################################
# Producer of the message to Hamilton (sends back cell count computed by celery workers)
def produceCellCountforHamilton(cell_count):
    credentials = pika.PlainCredentials("guest", "guest")

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host="dev-aics-tfp-002", port=80, credentials=credentials
        )
    )
    channel = connection.channel()
    channel.basic_publish(
        exchange=EXCHANGE_HAMILTON,
        routing_key=QUEUE_CELL_COUNT,
        body=json.dumps({"cell_count": cell_count}),
    )
    channel.close()


def main():
    listenOnFromHamiltonQueue()


if __name__ == "__main__":
    main()

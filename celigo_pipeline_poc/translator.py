# consumer
from functools import reduce
import json

from celery_worker import add
from hamilton_queue_connection import HamiltonQueueConnection, EXCHANGE_HAMILTON, QUEUE_CELL_COUNT, QUEUE_FILE_LIST



#########################################
# Producer of the message to Celery workers
def produceCeleryWork(file_list):
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
def produceCellCount(cell_count):
    connection = HamiltonQueueConnection()
    channel = connection.getChannel()
    channel.basic_publish(
        exchange=EXCHANGE_HAMILTON,
        routing_key=QUEUE_CELL_COUNT,
        body=json.dumps({"cell_count": cell_count}),
    )
    channel.close()

    
#########################################
# Consumer of the message from Hamilton
def handleFileList(ch, method, properties, body):
    print(f"Got a message from Queue C: {body}")
    file_list = json.loads(body)
    mock_cell_count = produceCeleryWork(file_list)
    print(f"Got a cell count of {mock_cell_count}")
    produceCellCount(mock_cell_count)


def consumeFileList():
    connection = HamiltonQueueConnection()
    channel = connection.getChannel()
    channel.basic_consume(
        queue=QUEUE_FILE_LIST,
        on_message_callback=handleFileList,
        auto_ack=True,
    )
    # this will be command for starting the consumer session
    channel.start_consuming()


def main():
    consumeFileList()


if __name__ == "__main__":
    main()

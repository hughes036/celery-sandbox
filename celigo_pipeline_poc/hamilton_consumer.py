import json

from hamilton_queue_connection import QUEUE_CELL_COUNT, HamiltonQueueConnection


def consumeCellCountFromSlum(ch, method, properties, body):
    print(f"Hamilton got cell count message: {json.loads(body)}")


def listenOnToHamiltonQueue():

    connection = HamiltonQueueConnection()
    channel = connection.getChannel()
    channel.basic_consume(
        queue=QUEUE_CELL_COUNT,
        on_message_callback=consumeCellCountFromSlum,
        auto_ack=True,
    )
    # this will be command for starting the consumer session
    channel.start_consuming()


def main():
    listenOnToHamiltonQueue()


if __name__ == "__main__":
    main()

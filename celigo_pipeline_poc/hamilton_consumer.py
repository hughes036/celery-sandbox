import json
import pika

from hamilton_queue import EXCHANGE_HAMILTON, QUEUE_CELL_COUNT


def consumeCellCountFromSlum(ch, method, properties, body):
    print(f"Hamilton got cell count message: {json.loads(body)}")


def listenOnToHamiltonQueue():
    credentials = pika.PlainCredentials("guest", "guest")

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host="dev-aics-tfp-002", port=80, credentials=credentials
        )
    )
    channel = connection.channel()
    channel.exchange_declare(EXCHANGE_HAMILTON, durable=True, exchange_type="direct")
    channel.queue_declare(queue=QUEUE_CELL_COUNT, durable=True)
    # TODO works in mailman but not here?
    channel.queue_bind(
        exchange=EXCHANGE_HAMILTON, queue=QUEUE_CELL_COUNT, routing_key=QUEUE_CELL_COUNT
    )
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

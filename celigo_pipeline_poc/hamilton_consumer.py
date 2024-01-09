import json
import pika


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
    channel.exchange_declare("test", durable=True, exchange_type="topic")
    channel.basic_consume(
        queue="D",
        on_message_callback=consumeCellCountFromSlum,
        auto_ack=True,
    )
    # this will be command for starting the consumer session
    channel.start_consuming()


def main():
    listenOnToHamiltonQueue()


if __name__ == "__main__":
    main()

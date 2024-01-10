import pika


EXCHANGE_HAMILTON = "hamilton_local"
QUEUE_FILE_LIST = "file_list"
QUEUE_CELL_COUNT = "cell_count"

class HamiltonQueueConnection:

    def __init__(self):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host="dev-aics-tfp-002", port=80, credentials=pika.PlainCredentials("guest", "guest")
            )
        )
        self.channel = connection.channel()
        self.channel.exchange_declare(EXCHANGE_HAMILTON, durable=True, exchange_type="direct")
        self.channel.queue_declare(queue=QUEUE_CELL_COUNT, durable=True)
        # TODO works in mailman but not here?
        self.channel.queue_bind(
            exchange=EXCHANGE_HAMILTON, queue=QUEUE_CELL_COUNT, routing_key=QUEUE_CELL_COUNT
        )

    def getChannel(self):
        return self.channel
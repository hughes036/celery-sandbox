# producer
import json

from hamilton_queue_connection import EXCHANGE_HAMILTON, QUEUE_FILE_LIST, HamiltonQueueConnection


def produceFileList():
    # This simulates the Venus process putting the inital file list message on a queue
    connection = HamiltonQueueConnection()
    channel = connection.getChannel()

    file_list = [
        "/foo/bar/1.tiff",
        "/foo/bar/2.tiff",
        "/foo/bar/3.tiff",
        "/foo/bar/4.tiff",
        "/foo/bar/5.tiff",
    ]
    channel.basic_publish(
        exchange=EXCHANGE_HAMILTON,
        routing_key=QUEUE_FILE_LIST,
        body=json.dumps(file_list),
    )
    channel.close()


def main():
    produceFileList()


if __name__ == "__main__":
    main()

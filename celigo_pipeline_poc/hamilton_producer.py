# producer
import json
import pika


def produceFileListForSlurm():
    # This simulates the Venus process putting the inital file list message on a queue
    credentials = pika.PlainCredentials("guest", "guest")

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host="dev-aics-tfp-002", port=80, credentials=credentials
        )
    )
    channel = connection.channel()
    channel.exchange_declare("test", durable=True, exchange_type="topic")
    channel.queue_declare(queue="C")
    channel.queue_bind(exchange="test", 
                       queue="C", 
                       routing_key="C")
    # messaging to queue named C
    file_list = [
        "/foo/bar/1.tiff",
        "/foo/bar/2.tiff",
        "/foo/bar/3.tiff",
        "/foo/bar/4.tiff",
        "/foo/bar/5.tiff",
    ]
    channel.basic_publish(exchange="test", routing_key="C", body=json.dumps(file_list))
    channel.close()


def main():
    produceFileListForSlurm()


if __name__ == "__main__":
    main()

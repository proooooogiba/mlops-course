"""Create missing Kafka topics; an already initialized broker is a no-op."""
import os

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


def main():
    client = AdminClient({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")})
    existing = client.list_topics(timeout=30).topics
    missing = [NewTopic(name, num_partitions=1, replication_factor=1)
               for name in ("transactions", "scores", "dead_letters") if name not in existing]
    if not missing:
        print("All Kafka topics already exist")
        return
    for name, future in client.create_topics(missing).items():
        try:
            future.result()
        except KafkaException as exc:
            # Another initializer may have created the topic since list_topics.
            if exc.args[0].name() != "TOPIC_ALREADY_EXISTS":
                raise
        print("Ready:", name)


if __name__ == '__main__':
    main()

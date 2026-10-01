"""Acknowledged writes and explicit per-message offset commits (at least once)."""
import json
import logging
import signal
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer, TopicPartition, OffsetAndMetadata

from src.adapters.controllers import MessageSource
from src.adapters.messages import score_message, transaction_message
from src.domain.entities import InvalidData, ScoreResult, Transaction


def make_producer(bootstrap_servers):
    return KafkaProducer(bootstrap_servers=bootstrap_servers, acks="all", retries=10,
                         max_in_flight_requests_per_connection=1,
                         value_serializer=lambda v: json.dumps(v, ensure_ascii=False, allow_nan=False).encode(),
                         key_serializer=lambda k: str(k).encode())


def publish(producer, topic, record):
    producer.send(topic, key=record.get("transaction_id", "invalid"), value=record).get(timeout=30)


def commit_message(consumer, message):
    consumer.commit({TopicPartition(message.topic, message.partition): OffsetAndMetadata(message.offset + 1, "", -1)})


class Worker:
    def __init__(self, bootstrap_servers, topic, group, dead_letters_topic):
        self.running = True
        self.dead_letters_topic = dead_letters_topic
        self.consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, group_id=group,
                                      auto_offset_reset="earliest", enable_auto_commit=False,
                                      max_poll_records=1, max_poll_interval_ms=300000)
        self.producer = make_producer(bootstrap_servers)
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: setattr(self, "running", False))

    def run(self, handler, stage):
        logging.info("%s worker ready", stage)
        try:
            while self.running:
                Path("/tmp/worker-heartbeat").touch()
                for messages in self.consumer.poll(timeout_ms=1000, max_records=1).values():
                    for message in messages:
                        try:
                            record = json.loads(message.value)
                            handler(record, MessageSource(message.topic, message.partition, message.offset))
                        except (InvalidData, json.JSONDecodeError, UnicodeDecodeError) as exc:
                            error = {"stage": stage, "source_topic": message.topic,
                                     "partition": message.partition, "offset": message.offset,
                                     "error": str(exc)}
                            publish(self.producer, self.dead_letters_topic, error)
                            logging.warning("Rejected %s:%s:%s: %s", message.topic, message.partition, message.offset, exc)
                        commit_message(self.consumer, message)
        finally:
            self.consumer.close(autocommit=False)
            self.producer.close(timeout=10)


def setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)


class KafkaScorePublisher:
    def __init__(self, producer, topic):
        self.producer = producer
        self.topic = topic

    def publish(self, result: ScoreResult) -> None:
        publish(self.producer, self.topic, score_message(result))


class KafkaTransactionPublisher:
    def __init__(self, producer, topic):
        self.producer = producer
        self.topic = topic

    def publish(self, transaction: Transaction) -> None:
        publish(self.producer, self.topic, transaction_message(transaction))

import json
from types import SimpleNamespace

import pytest

from src.domain.entities import InvalidData
from src.infrastructure.kafka import Worker


def worker_with_one_message(payload):
    events = []
    worker = Worker.__new__(Worker)
    worker.running = True
    worker.dead_letters_topic = "dead_letters"
    message = SimpleNamespace(topic="transactions", partition=0, offset=7, value=payload)

    class Consumer:
        def poll(self, **kwargs):
            worker.running = False
            return {0: [message]}
        def commit(self, offsets):
            events.append(("commit", offsets))
        def close(self, autocommit):
            assert autocommit is False

    class Producer:
        def send(self, topic, key, value):
            class Future:
                def get(self, timeout):
                    events.append(("publish", topic, value))
            return Future()
        def close(self, timeout):
            pass

    worker.consumer = Consumer()
    worker.producer = Producer()
    return worker, events


def test_offset_is_committed_after_successful_handler():
    worker, events = worker_with_one_message(b'{"valid": true}')
    def handle(record, source):
        assert source.identity == "transactions:0:7"
        events.append(("processed", record))
    worker.run(handle, "scorer")
    assert [event[0] for event in events] == ["processed", "commit"]
    offsets = events[-1][1]
    assert next(iter(offsets.values())).offset == 8


@pytest.mark.parametrize("error", [ConnectionError("broker unavailable"), RuntimeError("unexpected bug"), TypeError("unexpected type bug")])
def test_infrastructure_and_programming_failures_do_not_commit(error):
    worker, events = worker_with_one_message(b'{"valid": true}')
    def handle(record, source):
        raise error
    with pytest.raises(type(error)):
        worker.run(handle, "scorer")
    assert events == []


def test_invalid_data_is_acknowledged_only_after_dead_letter_publication():
    worker, events = worker_with_one_message(b'{"invalid": true}')
    def handle(record, source):
        raise InvalidData("Missing fields")
    worker.run(handle, "scorer")
    assert [event[0] for event in events] == ["publish", "commit"]
    assert events[0][1] == "dead_letters"
    assert events[0][2] == {"stage": "scorer", "source_topic": "transactions", "partition": 0, "offset": 7, "error": "Missing fields"}


def test_malformed_json_never_reaches_handler():
    worker, events = worker_with_one_message(b'{bad-json')
    def handle(record, source):
        pytest.fail("malformed JSON must not reach a use case")
    worker.run(handle, "scorer")
    assert [event[0] for event in events] == ["publish", "commit"]

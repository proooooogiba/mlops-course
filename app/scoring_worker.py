import logging

from app import bootstrap, settings
from src.adapters.controllers import TransactionController
from src.infrastructure.kafka import setup_logging


def main():
    setup_logging()
    worker = bootstrap.build_worker(settings.TRANSACTIONS_TOPIC, "fraud-scorer-v1")
    controller = TransactionController(bootstrap.build_process_transaction(worker.producer))

    def handle(record, source):
        result = controller.handle(record, source)
        logging.info("Scored transaction %s", result.transaction_id)

    worker.run(handle, "scorer")


if __name__ == "__main__":
    main()

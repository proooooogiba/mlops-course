from app import bootstrap, settings
from src.adapters.controllers import ScoreController
from src.infrastructure.kafka import setup_logging


def main():
    setup_logging()
    worker = bootstrap.build_worker(settings.SCORES_TOPIC, "scores-postgres-v1")
    with bootstrap.build_store_score() as use_case:
        controller = ScoreController(use_case)
        worker.run(controller.handle, "postgres-sink")


if __name__ == "__main__":
    main()

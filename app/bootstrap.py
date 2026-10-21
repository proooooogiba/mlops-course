"""First startup loads labels and publishes a real trained initial model."""
from pipeline.seed import seed
from pipeline.storage import root
from pipeline.retrain import prepare_window, train_candidate, evaluate_candidate, publish_model
from pipeline.quality import inspect_dataset, render_report, assert_quality


def main():
    seed()
    if not (root() / "models/latest.json").exists():
        plan = prepare_window("bootstrap")
        if plan is None:
            raise ValueError("need three consecutive labeled months")
        publish_model(evaluate_candidate(train_candidate(plan)))
    assert_quality(render_report(inspect_dataset("bootstrap")))


if __name__ == "__main__":
    main()

"""Retrain the current window, keep the quality gate and publish a new version."""
import json
import uuid

from pipeline.retrain import prepare_window, train_candidate, evaluate_candidate, publish_model


def main():
    run_key = 'hw5-demo-' + uuid.uuid4().hex
    plan = prepare_window(run_key, replay_current=True)
    if plan is None:
        raise RuntimeError('No complete training window')
    version = publish_model(evaluate_candidate(train_candidate(plan)))
    if version is None:
        raise RuntimeError('Quality gate rejected candidate; current model kept')
    print(json.dumps({'run_key': run_key, 'published_version': version}))


if __name__ == '__main__':
    main()

"""Always render HTML before failing on a violated dataset requirement."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.quality import inspect_dataset, render_report, assert_quality

with DAG("dataset_quality", start_date=datetime(2024, 1, 1), schedule="*/5 * * * *",
         catchup=False, max_active_runs=1, is_paused_upon_creation=False,
         default_args={"retries": 1, "retry_delay": timedelta(seconds=15)}, tags=["hw3", "dq"]) as dag:
    inspect = PythonOperator(task_id="inspect_dataset", python_callable=inspect_dataset, op_kwargs={"run_key": "{{ run_id }}"})
    render = PythonOperator(task_id="render_report", python_callable=render_report, op_args=[inspect.output])
    validate = PythonOperator(task_id="assert_quality", python_callable=assert_quality, op_args=[render.output])
    inspect >> render >> validate

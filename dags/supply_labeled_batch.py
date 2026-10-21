"""Monthly supply and retention are separate tasks in a separate scheduled DAG."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline.supply import append_month, prune_history

with DAG("supply_labeled_batch", start_date=datetime(2024, 1, 1), schedule="*/5 * * * *",
         catchup=False, max_active_runs=1, is_paused_upon_creation=False,
         default_args={"retries": 2, "retry_delay": timedelta(seconds=15)}, tags=["hw3"]) as dag:
    append = PythonOperator(task_id="append_month", python_callable=append_month, op_kwargs={"run_key": "{{ run_id }}"})
    prune = PythonOperator(task_id="prune_history", python_callable=prune_history, op_args=[append.output])
    append >> prune

import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

default_args_dict = {
    "start_date": datetime.datetime(2022, 11, 8, 0, 0, 0),
    "concurrency": 1,
    "schedule_interval": "0 4 * * *",  # Every day at 4am
    "retries": 1,
    "retry_delay": datetime.timedelta(seconds=15),
}

production_dag = DAG(
    dag_id="production_dag",
    default_args=default_args_dict,
    catchup=False,
)

start_node = EmptyOperator(
    task_id="start_task", dag=production_dag, trigger_rule="all_success"
)

end_node = EmptyOperator(
    task_id="end_task", dag=production_dag, trigger_rule="all_success"
)

start_node >> end_node
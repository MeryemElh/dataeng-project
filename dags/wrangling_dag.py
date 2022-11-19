import datetime
import pprint

from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient


default_args_dict = {
    "start_date": datetime.datetime(2022, 11, 8, 0, 0, 0),
    "concurrency": 1,
    "schedule_interval": "0 0 * * *",  # Every day at midnight
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

wrangling_dag = DAG(
    dag_id="wrangling_dag",
    default_args=default_args_dict,
    catchup=False,
)

def _mongodb_reader_test(host: str, port: str, database: str):
    client = MongoClient(f"mongodb://{host}:{port}/")
    db = client[database]
    col = db["wikidata_disstracks"]
    cpt = 0
    for post in col.find():
        pprint.pprint(post)
        cpt += 1
    if cpt == 0:
        raise Exception("Empty wikidata collection")
    
    col = db["dbpedia_disstracks"]
    cpt = 0
    for post in col.find():
        pprint.pprint(post)
        cpt += 1
    if cpt == 0:
        raise Exception("Empty dbpedia collection")

fourth_node_a = PythonOperator(
    task_id="mongodb_reader_test",
    dag=wrangling_dag,
    trigger_rule="none_failed",
    python_callable=_mongodb_reader_test,
    op_kwargs={
        "host": "mongo",
        "port": "27017",
        "database": "data",
        "collection": "wikidata",
    },
)
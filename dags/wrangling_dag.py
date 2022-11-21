import datetime
import pprint

from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import pandas as pd
import redis
from redis.commands.json.path import Path


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

def _cleansing_dbpedia_data(redis_output_key: str, redis_host: str, redis_port: str, redis_db: str,host: str, port: str, database: str):
    client = MongoClient(f"mongodb://{host}:{port}/")
    db = client[database]
    dbpedia_data = db["dbpedia_disstracks"]
    dbpedia_df = pd.DataFrame(dbpedia_data)

    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    client.json().set(redis_output_key, Path.root_path(), dbpedia_df)

first_node = PythonOperator(
    task_id="mongodb_reader_test",
    dag=wrangling_dag,
    trigger_rule="none_failed",
    python_callable=_cleansing_dbpedia_data,
    op_kwargs={
        "redis_input_key": "dbpedia_results",
        "redis_host": "rejson",
        "redis_port": 6379,
        "redis_db": 0,
        "host": "mongo",
        "port": "27017",
        "database": "data",
        "collection": "wikidata",
    },
)

def _cleansing_wikidata_data(redis_output_key: str, redis_host: str, redis_port: str, redis_db: str,host: str, port: str, database: str):
    client = MongoClient(f"mongodb://{host}:{port}/")
    db = client[database]
    wikidata_data = db["wikidata_disstracks"]
    wikidata_df = pd.DataFrame(wikidata_data)
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    client.json().set(redis_output_key, Path.root_path(), wikidata_df)

second_node = PythonOperator(
    task_id="_cleansing_wikidata_data",
    dag=wrangling_dag,
    trigger_rule="none_failed",
    python_callable=_cleansing_wikidata_data,
    op_kwargs={
        "redis_input_key": "wikidata_results",
        "redis_host": "rejson",
        "redis_port": 6379,
        "redis_db": 0,
        "host": "mongo",
        "port": "27017",
        "database": "data",
        "collection": "wikidata",

    },
)

def merging_data(redis_input_key_1: str, redis_input_key_2: str, redis_host: str, redis_port: str, redis_db: str):
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    dbpedia_data = client.json().get(redis_input_key_1)
    wikidata_data =client.json().get(redis_input_key_2)




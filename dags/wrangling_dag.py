import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd

default_args_dict = {
    "start_date": datetime.datetime(2022, 11, 8, 0, 0, 0),
    "concurrency": 1,
    "schedule_interval": "0 0 * * *",  # Every day at midnight
    "retries": 1,
    "retry_delay": datetime.timedelta(seconds=15),
}

wrangling_dag = DAG(
    dag_id="wrangling_dag",
    default_args=default_args_dict,
    catchup=False,
)

<<<<<<< HEAD
def _cleansing_dbpedia_data(redis_output_key: str, redis_host: str, redis_port: str, redis_db: str,host: str, port: str, database: str):
    client = MongoClient(f"mongodb://{host}:{port}/")
    db = client[database]
    dbpedia_data = db["dbpedia_disstracks"]
    dbpedia_df = pd.DataFrame(dbpedia_data)
=======

def _cleansing_dbpedia_data(
    redis_output_key: str,
    redis_host: str,
    redis_port: str,
    redis_db: str,
    host: str,
    port: str,
    database: str,
):
    # from pymongo import MongoClient
    # import redis
    # from redis.commands.json.path import Path
>>>>>>> 1897a43a7667864aa5a7bf8c279c6ccb5f628fc5

    # client = MongoClient(f"mongodb://{host}:{port}/")
    # db = client[database]
    # dbpedia_data = db["dbpedia_disstracks"]
    # dbpedia_df = pd.DataFrame(dbpedia_data)

    # client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    # client.json().set(redis_output_key, Path.root_path(), dbpedia_df)
    pass

<<<<<<< HEAD
first_node = PythonOperator(
    task_id="mongodb_reader_test",
=======

cleansing_dbpedia_node = PythonOperator(
    task_id="cleansing_dbpedia_data",
>>>>>>> 1897a43a7667864aa5a7bf8c279c6ccb5f628fc5
    dag=wrangling_dag,
    trigger_rule="none_failed",
    python_callable=_cleansing_dbpedia_data,
    op_kwargs={
        "redis_output_key": "dbpedia_results",
        "redis_host": "rejson",
        "redis_port": 6379,
        "redis_db": 0,
        "host": "mongo",
        "port": "27017",
        "database": "data",
        "collection": "wikidata",
    },
)

<<<<<<< HEAD
def _cleansing_wikidata_data(redis_output_key: str, redis_host: str, redis_port: str, redis_db: str,host: str, port: str, database: str):
    client = MongoClient(f"mongodb://{host}:{port}/")
    db = client[database]
    wikidata_data = db["wikidata_disstracks"]
    wikidata_df = pd.DataFrame(wikidata_data)
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    client.json().set(redis_output_key, Path.root_path(), wikidata_df)

second_node = PythonOperator(
    task_id="_cleansing_wikidata_data",
=======

def _cleansing_wikidata_data(
    redis_output_key: str,
    redis_host: str,
    redis_port: str,
    redis_db: str,
    host: str,
    port: str,
    database: str,
):
    # from pymongo import MongoClient
    # import redis
    # from redis.commands.json.path import Path

    # client = MongoClient(f"mongodb://{host}:{port}/")
    # db = client[database]
    # wikidata_data = db["wikidata_disstracks"]
    # wikidata_df = pd.DataFrame(wikidata_data)
    # client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    # client.json().set(redis_output_key, Path.root_path(), wikidata_df)
    pass


cleansing_wikidata_node = PythonOperator(
    task_id="cleansing_wikidata_data",
>>>>>>> 1897a43a7667864aa5a7bf8c279c6ccb5f628fc5
    dag=wrangling_dag,
    trigger_rule="none_failed",
    python_callable=_cleansing_wikidata_data,
    op_kwargs={
        "redis_output_key": "wikidata_results",
        "redis_host": "rejson",
        "redis_port": 6379,
        "redis_db": 0,
        "host": "mongo",
        "port": "27017",
        "database": "data",
        "collection": "wikidata",

    },
)

<<<<<<< HEAD
def merging_data(redis_input_key_1: str, redis_input_key_2: str, redis_host: str, redis_port: str, redis_db: str):
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    dbpedia_data = client.json().get(redis_input_key_1)
    wikidata_data =client.json().get(redis_input_key_2)



=======

def _merging_data(
    redis_wikidata_key: str,
    redis_dbpedia_key: str,
    redis_host: str,
    redis_port: int,
    redis_db: int,
):
    import redis
    import pandas as pd
    from sqlalchemy import create_engine

    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    wikidata_data = client.json().get(redis_wikidata_key)
    dbpedia_data = client.json().get(redis_dbpedia_key)

    # wiki_df = pd.DataFrame(wikidata_data)
    # print("Merging into one DF...")
    # print(wiki_df)


merging_node = PythonOperator(
    task_id="merging_data",
    dag=wrangling_dag,
    trigger_rule="none_failed",
    python_callable=_merging_data,
    op_kwargs={
        "redis_wikidata_key": "wikidata_results",
        "redis_dbpedia_key": "dbpedia_results",
        "redis_host": "rejson",
        "redis_port": 6379,
        "redis_db": 0,
    },
)


def _saving_to_postgres(
    redis_wikidata_key: str,
    redis_dbpedia_key: str,
    redis_host: str,
    redis_port: int,
    redis_db: int,
    postgres_host: str,
    postgres_port: int,
    postgres_db: str,
    postgres_user: str,
    postgres_pswd: str,
):
    import redis
    import pandas as pd
    from sqlalchemy import create_engine

    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    wikidata_data = client.json().get(redis_wikidata_key)
    dbpedia_data = client.json().get(redis_dbpedia_key)

    sql_engine = create_engine(
        f"postgresql://{postgres_user}:{postgres_pswd}@{postgres_host}:{postgres_port}/{postgres_db}"
    )

    print(wikidata_data)
    wiki_df = pd.DataFrame(wikidata_data)
    print(wiki_df)
    wiki_df.drop(columns="wikidata_metadata", inplace=True)
    print(wiki_df)
    wiki_df.to_sql("wiki_df", sql_engine, if_exists="replace")


saving_node = PythonOperator(
    task_id="saving_to_postgres",
    dag=wrangling_dag,
    trigger_rule="none_failed",
    python_callable=_saving_to_postgres,
    op_kwargs={
        "redis_wikidata_key": "wikidata_results",
        "redis_dbpedia_key": "dbpedia_results",
        "redis_host": "rejson",
        "redis_port": 6379,
        "redis_db": 0,
        "postgres_host": "postgres",
        "postgres_port": 5432,
        "postgres_db": "postgres",
        "postgres_user": "airflow",
        "postgres_pswd": "airflow",
    },
)

cleansing_wikidata_node >> merging_node >> saving_node
>>>>>>> 1897a43a7667864aa5a7bf8c279c6ccb5f628fc5

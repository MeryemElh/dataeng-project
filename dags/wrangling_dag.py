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


def _get_dbpedia_data(
    redis_output_key: str,
    redis_host: str,
    redis_port: str,
    redis_db: str,
    host: str,
    port: str,
    database: str,
):
    from pymongo import MongoClient
    import redis
    from redis.commands.json.path import Path
    import pyarrow as pa

    mongo_client = MongoClient(f"mongodb://{host}:{port}/")
    db = mongo_client[database]
    # format data
    dbpedia_data = db["dbpedia_disstracks"]
    precleaned_db = [
        {
            "url": x["diss"]["value"],
            "Song Title": x["name"]["value"],
            "genre": x["genre"]["value"],
            "recorded": x["recorded"]["value"],
            "released": x["released"]["value"],
            "recordLabel": x["recordLabel"]["value"],
        }
        for x in list(dbpedia_data.find())
    ]
    # storing in redis
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    context = pa.default_serialization_context()
    redis_client.set(
        "dbpedia_df", context.serialize(precleaned_db).to_buffer().to_pybytes()
    )


get_dbpedia_node = PythonOperator(
    task_id="get_dbpedia_data",
    dag=wrangling_dag,
    trigger_rule="none_failed",
    python_callable=_get_dbpedia_data,
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

dbpedia_cleansing = EmptyOperator(
    task_id="dbpedia_cleansing", dag=wrangling_dag, trigger_rule="all_success"
)


def _get_wikidata_data(
    redis_output_key: str,
    redis_host: str,
    redis_port: str,
    redis_db: str,
    host: str,
    port: str,
    database: str,
):
    from pymongo import MongoClient
    import redis
    from redis.commands.json.path import Path
    import pyarrow as pa

    mongo_client = MongoClient(f"mongodb://{host}:{port}/")
    db = mongo_client[database]
    wikidata_data = db["wikidata_disstracks"]
    wikidata_df = pd.DataFrame(list(wikidata_data.find()))
    # storing in redis
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    context = pa.default_serialization_context()
    redis_client.set(
        "wikidata_df", context.serialize(wikidata_df).to_buffer().to_pybytes()
    )


get_wikidata_node = PythonOperator(
    task_id="get_wikidata_data",
    dag=wrangling_dag,
    trigger_rule="none_failed",
    python_callable=_get_wikidata_data,
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

wikidata_cleansing = EmptyOperator(
    task_id="wikidata_cleansing", dag=wrangling_dag, trigger_rule="all_success"
)


def _merging_data(
    redis_host: str,
    redis_port: int,
    redis_db: int,
):
    import redis
    import pandas as pd
    import pyarrow as pa

    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    context = pa.default_serialization_context()
    wikidata_data = context.deserialize(redis_client.get("wikidata_df"))
    dbpedia_data = context.deserialize(redis_client.get("dbpedia_df"))
    wikidata_df = pd.DataFrame(wikidata_data)
    dbpedia_df = pd.DataFrame(dbpedia_data)
    wikidata_df["Song Title"] = wikidata_df["Song Title"].str[1:-1]
    merged_df = pd.merge(wikidata_df, dbpedia_df, how="outer", on=["Song Title"])

    # saving result to redis
    redis_client.set("merged_df", context.serialize(merged_df).to_buffer().to_pybytes())


merging_node = PythonOperator(
    task_id="merging_data",
    dag=wrangling_dag,
    trigger_rule="none_failed",
    python_callable=_merging_data,
    op_kwargs={
        "redis_host": "rejson",
        "redis_port": 6379,
        "redis_db": 0,
    },
)


def _cleansing_data(
    redis_host: str,
    redis_port: int,
    redis_db: int,
):
    import redis
    import pandas as pd
    import pyarrow as pa

    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    context = pa.default_serialization_context()
    data = context.deserialize(redis_client.get("merged_df"))
    df = pd.DataFrame(data)
    df.drop(["url","recordLabel","_id","Ref(s)","Wikipedia endpoint"],axis=1)
    #formating and adding two cols from metadata
    target_type = []
    artists = []
    for elem in df["wikidata_metadata"]:
        if(type(elem)!=dict):
            target_type.append("")
            artists.append("")
        if(type(elem)==dict):
            target_type.append(elem.get("target", "") and elem["target"][0]["type_label"]["value"])
            artists.append(elem.get("artists", "") and elem["artists"][0]["author_label"]["value"])
    df['Target Type'] = target_type
    df['Song Artist'] = artists
    #storing in redis
    redis_client.set("df", context.serialize(df).to_buffer().to_pybytes())


cleansing_node = PythonOperator(
    task_id="cleansing_data",
    dag=wrangling_dag,
    trigger_rule="none_failed",
    python_callable=_cleansing_data,
    op_kwargs={
        "redis_host": "rejson",
        "redis_port": 6379,
        "redis_db": 0,
    },
)


def _saving_to_postgres(
    redis_songs_key: str,
    redis_groups_key: str,
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
    import pyarrow as pa

    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    context = pa.default_serialization_context()
    songs = context.deserialize(redis_client.get(redis_songs_key))
    # groups = context.deserialize(redis_client.get(redis_groups_key))

    sql_engine = create_engine(
        f"postgresql://{postgres_user}:{postgres_pswd}@{postgres_host}:{postgres_port}/{postgres_db}"
    )

    songs_df = pd.DataFrame(songs, dtype=str)
    songs_df.drop(columns="wikidata_metadata", inplace=True)
    songs_df.to_sql("songs", sql_engine, if_exists="replace")


saving_node = PythonOperator(
    task_id="saving_to_postgres",
    dag=wrangling_dag,
    trigger_rule="none_failed",
    python_callable=_saving_to_postgres,
    op_kwargs={
        "redis_songs_key": "merged_df",
        "redis_groups_key": "groups_df",
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

get_wikidata_node >> get_dbpedia_node >> merging_node >> cleansing_node >> saving_node

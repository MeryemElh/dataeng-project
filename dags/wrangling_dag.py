import datetime
import json
import urllib.parse
import requests

from airflow import DAG
from time import sleep
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
    #droping unimportant columns
    df = df.drop(["url","recordLabel","_id","Ref(s)","Wikipedia endpoint"],axis=1)
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

def _person_request(target_id: str, endpoint: str, url: str):

    # Wikidata query to get target information 
    sparql_query = (
    "SELECT DISTINCT ?occupation_label ?first_name ?last_name ?birth_place "
        "WHERE { "
        f"wd:{target_id} wdt:P106 ?occupation_id. "
        "?occupation_id rdfs:label ?occupation_label. "
        f"wd:{target_id} wdt:P735|wdt:P1477|wdt:P1559 ?first_name_id. "
        "?first_name_id rdfs:label ?first_name. "
        f"wd:{target_id} wdt:P734 ?last_name_id. "
        "?last_name_id rdfs:label ?last_name. "
        f"wd:{target_id} wdt:P19 ?birth_place_id. "
        "?birth_place_id rdfs:label ?birth_place. "
        "filter(lang(?occupation_label) = 'en') "
        "filter(lang(?first_name) = 'en') "
        "filter(lang(?last_name) = 'en') "
        "filter(lang(?birth_place) = 'en') "
        "}"
    )
    r = requests.get(f"{url}{endpoint}", params={"format": "json", "query": sparql_query})
    if not r.ok:
        # Probable too many requests, so timeout and retry
        sleep(1)
        r = requests.get(
            f"{url}{endpoint}", params={"format": "json", "query": sparql_query}
        )
    return r.json()


def _group_request(target_id: str, endpoint: str, url: str):

    # Wikidata query to get target information 
    sparql_query = (
        "SELECT DISTINCT (sample(?name) as ?name) ?inception ?origin_country_label (count(?nominations) as ?nb_nominations) "
        "WHERE "
        "{ "
            "OPTIONAL{ "
                f"wd:{target_id} rdfs:label ?name. "
                "filter(lang(?name) = 'en') "
            "} "
            "OPTIONAL{ "
                f"wd:{target_id} wdt:P571 ?inception. "
            "} "
            "OPTIONAL{ "
                f"wd:{target_id} wdt:P495 ?origin_country. "
                "?origin_country rdfs:label ?origin_country_label. "
                "filter(lang(?origin_country_label) = 'en') "
            "} "
            "OPTIONAL{ "
                f"wd:{target_id} wdt:P1411 ?nominations. "
            "} "
        "} "
        "GROUP BY ?inception ?origin_country_label "
    )
    r = requests.get(f"{url}{endpoint}", params={"format": "json", "query": sparql_query})
    if not r.ok:
        # Probable too many requests, so timeout and retry
        sleep(1)
        r = requests.get(
            f"{url}{endpoint}", params={"format": "json", "query": sparql_query}
        )
    return r.json()


def _data_enrichment(
    redis_host: str,
    redis_port: int,
    redis_db: int,
    endpoint: str,
    url: str,
):
    import redis
    import pandas as pd
    import pyarrow as pa
    import requests
    import json
    from time import sleep


    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    context = pa.default_serialization_context()
    df = context.deserialize(redis_client.get("df"))

    persons_data = []
    for row in df.iterrows(): 
        if(row[1]["Target Type"] == "human"):
            target_id = row[1]["Wikidata target id"] 
            person_data = _person_request(target_id,endpoint,url)
            if person_data["results"]["bindings"]:
                    persons_data.append(person_data["results"]["bindings"])

    persons_fdata = [
        {
            "Occupation Label": x["occupation_label"]["value"],
            "First Name": x["first_name"]["value"],
            "Last Name": x["last_name"]["value"],
            "Birth Place": x["birth_place"]["value"],
        }
        for x in persons_data[0]
    ]
    persons_df = pd.DataFrame(persons_fdata)

    groups_data = []
    for row in df.iterrows(): 
        if("group" in row[1]["Target Type"].lower() or "duo" in row[1]["Target Type"].lower()):
            target_id = row[1]["Wikidata target id"] 
            group_data = _person_request(target_id,endpoint,url)
            if group_data["results"]["bindings"]:
                    groups_data.append(group_data["results"]["bindings"])

    groups_fdata = [
        {
            "Name": x["name"]["value"],
            "Inception": x["inception"]["value"],
            "Country": x["origin_country_label"]["value"],
            "Number of Nominations": x["nb_nominations"]["value"],
        }
        for x in groups_data[0]
    ]
    groups_df = pd.DataFrame(groups_fdata)

    #storing in redis
    redis_client.set("persons_df", context.serialize(persons_df).to_buffer().to_pybytes())
    redis_client.set("groups_df", context.serialize(groups_df).to_buffer().to_pybytes())

enrichment_node = PythonOperator(
    task_id="data_enrichment",
    dag=wrangling_dag,
    trigger_rule="none_failed",
    python_callable=_data_enrichment,
    op_kwargs={
        "redis_host": "rejson",
        "redis_port": 6379,
        "redis_db": 0,
        "endpoint": "/sparql",
        "url": "https://query.wikidata.org",
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

get_wikidata_node >> get_dbpedia_node >> merging_node >> cleansing_node >> enrichment_node >> saving_node

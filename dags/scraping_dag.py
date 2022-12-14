import json
from typing import Any
import urllib.parse
import datetime
from time import sleep

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
import redis
from redis.commands.json.path import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator


default_args_dict = {
    "start_date": datetime.datetime(2022, 11, 8, 0, 0, 0),
    "concurrency": 1,
    "schedule_interval": "0 0 * * *",  # Every day at midnight
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

data_collection_dag = DAG(
    dag_id="data_collection_dag",
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=["/opt/airflow/data/"],
)


offline_source_node = EmptyOperator(
    task_id="offline_source", dag=data_collection_dag, trigger_rule="all_success"
)


start_scrapper_node = EmptyOperator(
    task_id="scrapper_departure", dag=data_collection_dag, trigger_rule="all_success"
)


def _check_connection():

    try:
        requests.get("https://google.com")
    except requests.exceptions.ConnectionError:
        return "offline_source"

    return "scrapper_departure"
        

connection_check_node = BranchPythonOperator(
    task_id='connection_check',
    dag=data_collection_dag,
    python_callable= _check_connection,
    op_kwargs={},
    trigger_rule='all_success',
)


def _scrap_disstrack_dbpedia(
    filepath_input: str,
    redis_output_key: str,
    redis_host: str,
    redis_port: str,
    redis_db: str,
) -> None:

    # Fetched url to get our results from dbpedia in a json format
    with open(filepath_input, "r+", encoding="utf-8") as f:
        data = json.load(f)

    # Save the result to redis db (to speed up the steps as it uses cache)
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    client.json().set(redis_output_key, Path.root_path(), data)


dbpedia_offline_node = PythonOperator(
    task_id="dbpedia_offline_list",
    dag=data_collection_dag,
    trigger_rule="all_success",
    python_callable=_scrap_disstrack_dbpedia,
    op_kwargs={
        "filepath_input": "/opt/airflow/data/offline_dbpedia_data.json",
        "redis_output_key": "dbpedia_results",
        "redis_host": "rejson",
        "redis_port": 6379,
        "redis_db": 0,
    },
    depends_on_past=False,
)


def _scrap_all_disstracks_wikidata_metadata(
    filepath_input: str,
    redis_output_key: str,
    redis_host: str,
    redis_port: str,
    redis_db: str,
):

    with open(filepath_input, "r+", encoding="utf-8") as f:
        disstracks_list = json.load(f)

    # Save the result to redis db (to speed up the steps as it uses cache)
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    client.json().set(redis_output_key, Path.root_path(), disstracks_list)


wikidata_offline_node = PythonOperator(
    task_id="wikidata_offline_metadata",
    dag=data_collection_dag,
    trigger_rule="all_success",
    python_callable=_scrap_all_disstracks_wikidata_metadata,
    op_kwargs={
        "filepath_input": "/opt/airflow/data/offline_wikidata_data.json",
        "redis_output_key": "wikidata_results",
        "redis_host": "rejson",
        "redis_port": 6379,
        "redis_db": 0,
    },
    depends_on_past=False,
)


def _scrap_disstrack_list(table: Tag, fixed_properties: dict, url: str):
    # Reads all the rows of the table and separate the first one containing the headers
    rows = table.find_all("tr")
    headers = rows[0]

    # Separate all the headers and only keep the text (inner html)
    headers = headers.find_all("th")
    headers = [header.text.strip() for header in headers]

    # Goes through all the rows except the headers and add the elements in a list
    disstracks = []
    for row in rows[1:]:
        # Gets all the elements of the row
        elements = row.find_all("td")

        # Puts together the elements in a dictionnary mapped to their headers
        disstrack_infos = {}
        for index, elem in enumerate(elements):
            disstrack_infos[headers[index]] = elem.text.strip()

        # Gets the link to the targets and song wikipedia page if exists
        song_target_index = headers.index("Target(s)")
        try:
            _add_wikidata_id(
                elements[song_target_index], disstrack_infos, url, "Wikidata target id"
            )
        except IndexError:
            print(f"A disstrack doesn't have any target : {disstrack_infos}")
            disstrack_infos["Wikipedia endpoint"] = ""
            disstrack_infos["Wikidata target id"] = ""

        song_title_index = headers.index("Song Title")
        try:
            _add_wikidata_id(
                elements[song_title_index], disstrack_infos, url, "Wikidata song id"
            )
        except IndexError:
            print(f"A disstrack doesn't have any title : {disstrack_infos}")

        # Mixing the song infos with the fixed infos and adding it to the list
        disstracks.append(dict(disstrack_infos, **fixed_properties))

    return disstracks


def _add_wikidata_id(element: Any, dissTrackInfos: dict, url: str, name: str):
    if not element.a:
        dissTrackInfos["Wikipedia endpoint"] = ""
        dissTrackInfos[name] = ""

    else:
        dissTrackInfos["Wikipedia endpoint"] = element.a["href"]
        # Gets the song Wikidata id if exists
        page = requests.get(f"{url}{dissTrackInfos['Wikipedia endpoint']}")
        soup = BeautifulSoup(page.content, "html.parser")
        wikidata_tag = soup.find_all("span", string="Wikidata item")
        if not wikidata_tag:
            dissTrackInfos[name] = ""
        else:
            # From the full wikidata url, only take the part at the right of the last slash '/'
            dissTrackInfos[name] = wikidata_tag[0].parent["href"].rsplit("/", 1)[1]
    return dissTrackInfos


def _scrap_disstrack_wikipage(
    redis_output_key: str,
    redis_host: str,
    redis_port: str,
    redis_db: str,
    endpoint: str,
    url: str,
) -> None:

    page = requests.get(f"{url}{endpoint}")
    soup = BeautifulSoup(page.content, "html.parser")

    # Gets two list of disstracks (tables), the first one representing the traditionally recorded
    #   and the second one for Youtube
    disstrack_lists = soup.find_all("table", {"class": "wikitable sortable"})
    traditional_list = disstrack_lists[0]
    youtube_list = disstrack_lists[1]

    # Scraps both lists to get all the disstracks infos (with the "traditional" or "YouTube" info)
    disstracks_infos = _scrap_disstrack_list(
        traditional_list, {"origin": "traditionally recorded"}, url
    )
    disstracks_infos.extend(
        _scrap_disstrack_list(youtube_list, {"origin": "youtube"}, url)
    )

    # Save the result to redis db (to speed up the steps as it uses cache)
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    client.json().set(redis_output_key, Path.root_path(), disstracks_infos)


wikipedia_node = PythonOperator(
    task_id="wikipedia_list",
    dag=data_collection_dag,
    trigger_rule="all_success",
    python_callable=_scrap_disstrack_wikipage,
    op_kwargs={
        "redis_output_key": "wikipedia_results",
        "redis_host": "rejson",
        "redis_port": 6379,
        "redis_db": 0,
        "endpoint": "/wiki/List_of_diss_tracks",
        "url": "https://en.wikipedia.org",
    },
    depends_on_past=False,
)


def _scrap_disstrack_dbpedia(
    filepath_output: str,
    redis_output_key: str,
    redis_host: str,
    redis_port: str,
    redis_db: str,
    endpoint: str,
    url: str,
) -> None:

    # DBPedia query to get infos of all the disstracks present on DBPedia
    sparql_query = (
        "SELECT DISTINCT ?diss,?name,?genre,?recorded,?released,?recordLabel WHERE"
        "{?diss rdf:type dbo:Song;"
        "dbp:name ?name;"
        "dct:subject dbc:Diss_tracks;"
        "dbp:genre ?genre;"
        "dbp:recorded ?recorded;"
        "dbp:released ?released;"
        "dbo:recordLabel ?recordLabel}"
    )

    # Fetched url to get our results from dbpedia in a json format
    api_request = (
        f"{url}{endpoint}?query={urllib.parse.quote_plus(sparql_query)}&format=json"
    )
    json_data = json.loads(requests.get(api_request).text)

    # Persist the data in a json in case no connection is available next time
    with open(filepath_output, "w+", encoding="utf-8") as f:
        json.dump(json_data, f)

    # Save the result to redis db (to speed up the steps as it uses cache)
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    client.json().set(
        redis_output_key, Path.root_path(), json_data
    )


dbpedia_node = PythonOperator(
    task_id="dbpedia_list",
    dag=data_collection_dag,
    trigger_rule="all_success",
    python_callable=_scrap_disstrack_dbpedia,
    op_kwargs={
        "filepath_output": "/opt/airflow/data/offline_dbpedia_data.json",
        "redis_output_key": "dbpedia_results",
        "redis_host": "rejson",
        "redis_port": 6379,
        "redis_db": 0,
        "endpoint": "/sparql",
        "url": "http://dbpedia.org",
    },
    depends_on_past=False,
)


def _scrap_disstrack_wikidata_metadata_artists(diss_id: str, endpoint: str, url: str):

    # Wikidata query to get metadata from disstracks
    sparql_query = (
        "SELECT DISTINCT ?author_id ?author_label "
        "WHERE { "
        f"wd:{diss_id} wdt:P50|wdt:P175|wdt:P86 ?author_id. "
        "?author_id rdfs:label ?author_label. "
        "filter(lang(?author_label) = 'en') "
        "}"
    )
    r = requests.get(
        f"{url}{endpoint}", params={"format": "json", "query": sparql_query}
    )
    if not r.ok:
        # Probable too many requests, so sleep and retry
        sleep(1)
        r = requests.get(
            f"{url}{endpoint}", params={"format": "json", "query": sparql_query}
        )
    return r.json()


def _scrap_disstrack_wikidata_metadata_target(target_id: str, endpoint: str, url: str):

    # Wikidata query to get metadata from disstracks
    sparql_query = (
        "SELECT DISTINCT ?type_id ?type_label "
        "WHERE { "
        f"wd:{target_id} wdt:P31 ?type_id. "
        "?type_id rdfs:label ?type_label. "
        "filter(lang(?type_label) = 'en') "
        "}"
    )
    r = requests.get(
        f"{url}{endpoint}", params={"format": "json", "query": sparql_query}
    )
    if not r.ok:
        # Probable too many requests, so timeout and retry
        sleep(1)
        r = requests.get(
            f"{url}{endpoint}", params={"format": "json", "query": sparql_query}
        )
    return r.json()


def _scrap_all_disstracks_wikidata_metadata(
    redis_input_key: str,
    redis_output_key: str,
    filepath_output: str,
    redis_host: str,
    redis_port: str,
    redis_db: str,
    endpoint: str,
    url: str,
):

    # Gets the precedent wikipedia list saved in redis
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    disstracks_list = client.json().get(redis_input_key)

    cpt = 0
    for diss in disstracks_list:
        cpt += 1

        # If the diss has a Wikidata song id, we try to complete some metadata, else we add a blank json
        diss["wikidata_metadata"] = {}
        if diss["Wikidata song id"]:
            # If found artists, add them to the metadata
            raw_wikidata_metadata_artists = _scrap_disstrack_wikidata_metadata_artists(
                diss["Wikidata song id"], endpoint, url
            )
            if raw_wikidata_metadata_artists["results"]["bindings"]:
                diss["wikidata_metadata"] = {
                    "artists": raw_wikidata_metadata_artists["results"]["bindings"]
                }

        # If the diss has a Wikidata target id, we try to complete its type
        if diss["Wikidata target id"]:
            # If found subjects, add them to the metadata
            raw_wikidata_metadata_target = _scrap_disstrack_wikidata_metadata_target(
                diss["Wikidata target id"], endpoint, url
            )
            if raw_wikidata_metadata_target["results"]["bindings"]:
                    diss["wikidata_metadata"]["target"] = raw_wikidata_metadata_target["results"]["bindings"]

    # Persist the data in a json in case no connection is available next time
    with open(filepath_output, "w+", encoding="utf-8") as f:
        json.dump(disstracks_list, f)

    # Save the result to redis db (to speed up the steps as it uses cache)
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    client.json().set(redis_output_key, Path.root_path(), disstracks_list)


wikidata_node = PythonOperator(
    task_id="wikidata_metadata",
    dag=data_collection_dag,
    trigger_rule="all_success",
    python_callable=_scrap_all_disstracks_wikidata_metadata,
    op_kwargs={
        "redis_input_key": "wikipedia_results",
        "redis_output_key": "wikidata_results",
        "filepath_output": "/opt/airflow/data/offline_wikidata_data.json",
        "redis_host": "rejson",
        "redis_port": 6379,
        "redis_db": 0,
        "endpoint": "/sparql",
        "url": "https://query.wikidata.org",
    },
    depends_on_past=False,
)


def _mongodb_saver(
    redis_input_key: str,
    redis_host: str,
    redis_port: str,
    redis_db: str,
    mongo_host: str,
    mongo_port: str,
    mongo_database: str,
    mongo_collection: str,
):

    # Gets the input data saved in redis to save them in mongo
    client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    json_data = client.json().get(redis_input_key)

    client = MongoClient(f"mongodb://{mongo_host}:{mongo_port}/")
    db = client[mongo_database]
    col = db[mongo_collection]

    if mongo_collection == "wikidata_disstracks":
        col.create_index(
            [
                ("Date Released", ASCENDING),
                ("Song Title", ASCENDING),
                ("Artist(s)", ASCENDING),
            ],
            unique=True,
        )
        for doc in json_data:
            try:
                col.insert_one(doc)
                print("inserted")
            except DuplicateKeyError:
                # If song already exists in db, skip the insertion
                pass
    elif mongo_collection == "dbpedia_disstracks":
        col.create_index("diss", unique=True)
        for doc in json_data["results"]["bindings"]:
            try:
                col.insert_one(doc)
            except DuplicateKeyError:
                print("not inserted")
                # If song already exists in db, skip the insertion
                pass


mongo_wiki_node = PythonOperator(
    task_id="mongodb_saver_wikidata",
    dag=data_collection_dag,
    trigger_rule="one_success",
    python_callable=_mongodb_saver,
    op_kwargs={
        "redis_input_key": "wikidata_results",
        "redis_host": "rejson",
        "redis_port": 6379,
        "redis_db": 0,
        "mongo_host": "mongo",
        "mongo_port": "27017",
        "mongo_database": "data",
        "mongo_collection": "wikidata_disstracks",
    },
)


mongo_dbpedia_node = PythonOperator(
    task_id="mongodb_saver_dbpedia",
    dag=data_collection_dag,
    trigger_rule="one_success",
    python_callable=_mongodb_saver,
    op_kwargs={
        "redis_input_key": "dbpedia_results",
        "redis_host": "rejson",
        "redis_port": 6379,
        "redis_db": 0,
        "mongo_host": "mongo",
        "mongo_port": "27017",
        "mongo_database": "data",
        "mongo_collection": "dbpedia_disstracks",
    },
)


last_node = EmptyOperator(
    task_id="finale", dag=data_collection_dag, trigger_rule="all_success"
)



connection_check_node >> [start_scrapper_node, offline_source_node]

start_scrapper_node >> wikipedia_node >> wikidata_node >> mongo_wiki_node >> last_node
start_scrapper_node >> dbpedia_node >> mongo_dbpedia_node >> last_node

offline_source_node >> wikidata_offline_node >> mongo_wiki_node >> last_node
offline_source_node >> dbpedia_offline_node >> mongo_dbpedia_node >> last_node

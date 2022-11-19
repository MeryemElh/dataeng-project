import json
import urllib.parse
import datetime
from time import sleep

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from pymongo import MongoClient

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# from airflow.operators.bash import BashOperator


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

        # Gets the link to the song wikipedia page if exists
        song_title_index = headers.index("Song Title")
        if not elements[song_title_index].a:
            disstrack_infos["Wikipedia endpoint"] = ""
            disstrack_infos["Wikidata id"] = ""
        else:
            disstrack_infos["Wikipedia endpoint"] = elements[song_title_index].a["href"]
            # Gets the song wikidata id if exists
            page = requests.get(f"{url}{disstrack_infos['Wikipedia endpoint']}")
            soup = BeautifulSoup(page.content, "html.parser")
            wikidata_tag = soup.find_all("span", string="Wikidata item")
            if not wikidata_tag:
                disstrack_infos["Wikidata id"] = ""
            else:
                # From the full wikidata url, only take the part at the right of the last slash '/'
                disstrack_infos["Wikidata id"] = (
                    wikidata_tag[0].parent["href"].rsplit("/", 1)[1]
                )

        # Mixing the song infos with the fixed infos and adding it to the list
        disstracks.append(dict(disstrack_infos, **fixed_properties))

    return disstracks


def _scrap_disstrack_wikipage(output_folder: str, endpoint: str, url: str) -> None:
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

    # Save the result to a file (temporary that will be deleted if docker stopped)
    with open(f"{output_folder}/node1_wikipedia_disstrack_list.json", "w+") as f:
        json.dump(disstracks_infos, f)


first_node = PythonOperator(
    task_id="wikipedia_list",
    dag=data_collection_dag,
    trigger_rule="none_failed",
    python_callable=_scrap_disstrack_wikipage,
    op_kwargs={
        "output_folder": "/opt/airflow/data",
        "endpoint": "/wiki/List_of_diss_tracks",
        "url": "https://en.wikipedia.org",
    },
    depends_on_past=False,
)


def _scrap_disstrack_dbpedia(output_folder: str, endpoint: str, url: str) -> None:

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

    # Save the result to a file (temporary that will be deleted if docker stopped)
    with open(f"{output_folder}/node2_dbpedia_disstrack_list.json", "w+") as f:
        f.write(requests.get(api_request).text)


second_node = PythonOperator(
    task_id="dbpedia_list",
    dag=data_collection_dag,
    trigger_rule="none_failed",
    python_callable=_scrap_disstrack_dbpedia,
    op_kwargs={
        "output_folder": "/opt/airflow/data",
        "endpoint": "/sparql",
        "url": "http://dbpedia.org",
    },
    depends_on_past=False,
)


def _scrap_disstrack_wikidata_metadata_subject(diss_id: str, endpoint: str, url: str):

    # Wikidata query to get metadata from disstracks
    sparql_query = (
        "SELECT DISTINCT ?main_subject_id ?main_subject_label ?author_id ?author_label "
        "WHERE { "
        f"wd:{diss_id} wdt:P921 ?main_subject_id. "
        "?main_subject_id rdfs:label ?main_subject_label. "
        f"wd:{diss_id} wdt:P50 ?author_id. "
        "?author_id rdfs:label ?author_label. "
        "filter(lang(?main_subject_label) = 'en') "
        "filter(lang(?author_label) = 'en') "
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
    output_folder: str, endpoint: str, url: str
):

    with open(f"{output_folder}/node1_wikipedia_disstrack_list.json", "r") as f:
        disstracks_list = json.load(f)
    cpt = 0
    for diss in disstracks_list:
        cpt += 1
        print(cpt)
        # If the diss has a wikidata id, we try to complete some metadata, else we add a blank json
        diss["wikidata_metadata"] = {}
        if diss["Wikidata id"]:
            # If found subjects, add them to the metadata
            raw_wikidata_metadata_subject = _scrap_disstrack_wikidata_metadata_subject(
                diss["Wikidata id"], endpoint, url
            )
            if raw_wikidata_metadata_subject["results"]["bindings"]:
                diss["wikidata_metadata"] = {
                    "subject": raw_wikidata_metadata_subject["results"]["bindings"]
                }
            # TODO: Create other functions to retrieve metadata similarly to the subject in _scrap_disstrack_wikidata_metadata_subject

    with open(
        f"{output_folder}/node3_wikidata_disstrack_list_with_additional_metadata.json",
        "w+",
    ) as f:
        json.dump(disstracks_list, f)


third_node = PythonOperator(
    task_id="wikidata_metadata",
    dag=data_collection_dag,
    trigger_rule="all_success",
    python_callable=_scrap_all_disstracks_wikidata_metadata,
    op_kwargs={
        "output_folder": "/opt/airflow/data",
        "endpoint": "/sparql",
        "url": "https://query.wikidata.org",
    },
    depends_on_past=False,
)


def _mongodb_saver(
    output_folder: str, filename: str, host: str, port: str, database: str, collection: str
):
    client = MongoClient(f"mongodb://{host}:{port}/")
    db = client[database]
    col = db[collection]
    with open(output_folder + filename) as file:
        file_data = json.load(file)

    if isinstance(file_data, list):
        col.insert_many(file_data)
    else:
        col.insert_one(file_data)


fourth_node_a = PythonOperator(
    task_id="mongodb_saver_wikidata",
    dag=data_collection_dag,
    trigger_rule="all_success",
    python_callable=_mongodb_saver,
    op_kwargs={
        "output_folder": "/opt/airflow/data/",
        "filename": "node3_wikidata_disstrack_list_with_additional_metadata.json",
        "host": "mongo",
        "port": "27017",
        "database": "data",
        "collection": "wikidata_disstracks",
    },
)


fourth_node_b = PythonOperator(
    task_id="mongodb_saver_dbpedia",
    dag=data_collection_dag,
    trigger_rule="all_success",
    python_callable=_mongodb_saver,
    op_kwargs={
        "output_folder": "/opt/airflow/data/",
        "filename": "node2_dbpedia_disstrack_list.json",
        "host": "mongo",
        "port": "27017",
        "database": "data",
        "collection": "dbpedia_disstracks",
    },
)


fifth_node = EmptyOperator(
    task_id="finale", dag=data_collection_dag, trigger_rule="none_failed"
)


first_node >> third_node >> fourth_node_a >> fifth_node
second_node >> fourth_node_b >> fifth_node

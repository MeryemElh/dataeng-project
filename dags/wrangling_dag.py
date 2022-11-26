import datetime
import requests
from pymongo import MongoClient
import redis
import pyarrow as pa
from sqlalchemy import create_engine, Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import declarative_base, relationship, Session
from py2neo import Graph

from airflow import DAG
from time import sleep
from airflow.operators.python import PythonOperator
import pandas as pd

default_args_dict = {
    "start_date": datetime.datetime(2022, 11, 8, 0, 0, 0),
    "concurrency": 1,
    "schedule_interval": "0 2 * * *",  # Every day at 2am
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
        redis_output_key, context.serialize(precleaned_db).to_buffer().to_pybytes()
    )

get_dbpedia_node = PythonOperator(
    task_id="get_dbpedia_data",
    dag=wrangling_dag,
    trigger_rule="none_failed",
    python_callable=_get_dbpedia_data,
    op_kwargs={
        "redis_output_key": "dbpedia_df",
        "redis_host": "rejson",
        "redis_port": 6379,
        "redis_db": 0,
        "host": "mongo",
        "port": "27017",
        "database": "data",
        "collection": "wikidata",
    },
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
    mongo_client = MongoClient(f"mongodb://{host}:{port}/")
    db = mongo_client[database]
    wikidata_data = db["wikidata_disstracks"]
    wikidata_df = pd.DataFrame(list(wikidata_data.find()))
    # storing in redis
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    context = pa.default_serialization_context()
    redis_client.set(
        redis_output_key, context.serialize(wikidata_df).to_buffer().to_pybytes()
    )

get_wikidata_node = PythonOperator(
    task_id="get_wikidata_data",
    dag=wrangling_dag,
    trigger_rule="none_failed",
    python_callable=_get_wikidata_data,
    op_kwargs={
        "redis_output_key": "wikidata_df",
        "redis_host": "rejson",
        "redis_port": 6379,
        "redis_db": 0,
        "host": "mongo",
        "port": "27017",
        "database": "data",
        "collection": "wikidata",
    },
)

def _merging_data(
    redis_output_key: str,
    redis_host: str,
    redis_port: int,
    redis_db: int,
):
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    context = pa.default_serialization_context()
    wikidata_data = context.deserialize(redis_client.get("wikidata_df"))
    dbpedia_data = context.deserialize(redis_client.get("dbpedia_df"))
    wikidata_df = pd.DataFrame(wikidata_data)
    dbpedia_df = pd.DataFrame(dbpedia_data)
    wikidata_df["Song Title"] = wikidata_df["Song Title"].str[1:-1]
    merged_df = pd.merge(wikidata_df, dbpedia_df, how="outer", on=["Song Title"])
    # saving result to redis
    redis_client.set(redis_output_key, context.serialize(merged_df).to_buffer().to_pybytes())

merging_node = PythonOperator(
    task_id="merging_data",
    dag=wrangling_dag,
    trigger_rule="none_failed",
    python_callable=_merging_data,
    op_kwargs={
        "redis_output_key": "merged_df",
        "redis_host": "rejson",
        "redis_port": 6379,
        "redis_db": 0,
    },
)

def _cleansing_data(
    redis_output_key: str,
    redis_input_key:str,
    redis_host: str,
    redis_port: int,
    redis_db: int,
):
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    context = pa.default_serialization_context()
    data = context.deserialize(redis_client.get(redis_input_key))
    df = pd.DataFrame(data)
    #droping unimportant columns
    df = df.drop(["url","recordLabel","_id","Ref(s)","Wikipedia endpoint","Notes","origin"],axis=1)
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
    # drop wikidata table after formating its content in cols
    df = df.drop(["wikidata_metadata"],axis=1)
    # merging information to handle nan
    artists_df = df['Artist(s)'].combine_first(df['Song Artist'])
    released_df = df['released'].combine_first(df['Date Released'])
    df = df.join(artists_df, lsuffix='_caller', rsuffix='_song')
    df = df.join(released_df, lsuffix='_caller', rsuffix='_song')
    df = df.drop(["Artist(s)_caller","released_caller","Date Released","Song Artist"],axis=1)
    #storing in redis
    redis_client.set(redis_output_key, context.serialize(df).to_buffer().to_pybytes())

cleansing_node = PythonOperator(
    task_id="cleansing_data",
    dag=wrangling_dag,
    trigger_rule="none_failed",
    python_callable=_cleansing_data,
    op_kwargs={
        "redis_output_key": "df",
        "redis_input_key":"merged_df",
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

    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    context = pa.default_serialization_context()
    df = context.deserialize(redis_client.get("df"))

    groups_fdata = []
    persons_fdata = []
    for row in df.iterrows(): 
        if(row[1]["Target Type"] == "human"):
            target_id = row[1]["Wikidata target id"] 
            person_data = _person_request(target_id,endpoint,url)
            if person_data["results"]["bindings"]:
                x = person_data["results"]["bindings"][0]
                persons_fdata.append({
                    "Occupation Label": x["occupation_label"]["value"],
                    "First Name": x["first_name"]["value"],
                    "Last Name": x["last_name"]["value"],
                    "Birth Place": x["birth_place"]["value"],
                    "person id": target_id
                })
        elif("group" in row[1]["Target Type"].lower() or "duo" in row[1]["Target Type"].lower()):
            target_id = row[1]["Wikidata target id"] 
            group_data = _group_request(target_id,endpoint,url)
            if group_data["results"]["bindings"]:
                x = group_data["results"]["bindings"][0]
                groups_fdata.append({
                    "Name": x["name"]["value"],
                    "Inception": x["inception"]["value"],
                    "Country": x["origin_country_label"]["value"],
                    "Number of Nominations": x["nb_nominations"]["value"],
                    "group id": target_id
                })
                
    persons_df = pd.DataFrame(persons_fdata)
    groups_df = pd.DataFrame(groups_fdata)
    df = df.drop(["Target Type"],axis=1)
    print(df)

    #storing in redis
    redis_client.set("persons_df", context.serialize(persons_df).to_buffer().to_pybytes())
    redis_client.set("groups_df", context.serialize(groups_df).to_buffer().to_pybytes())
    redis_client.set("df", context.serialize(df).to_buffer().to_pybytes())

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
    redis_persons_key: str,
    redis_host: str,
    redis_port: int,
    redis_db: int,
    postgres_host: str,
    postgres_port: int,
    postgres_db: str,
    postgres_user: str,
    postgres_pswd: str,
):
    Base = declarative_base()
    class Song(Base):
        __tablename__ = "song"
        id = Column(Integer, primary_key=True)
        title = Column(String, nullable=False)
        release_date = Column(DateTime)
        record_date = Column(DateTime)
        genre = Column(String)
        wikidata_id = Column(String)
        artist_id = Column(Integer, ForeignKey("entity.id"), nullable=False)
        target_id = Column(Integer, ForeignKey("entity.id"), nullable=False)
      
        artist = relationship(
            "Entity", backref="produced_disses", foreign_keys=[artist_id]
        )
        target = relationship(
            "Entity", backref="targeted_disses", foreign_keys=[target_id]
        )
        def __repr__(self):
            return f"Song(id={self.id!r}, title={self.title!r}, release_date={self.release_date!r}, release_date={self.release_date!r})"
  
      
    class Entity(Base):
        __tablename__ = "entity"
        id = Column(Integer, primary_key=True)
        name = Column(String, nullable=False)
        type = Column(String(50))
        wikidata_id = Column(String)
        __mapper_args__ = {
            "polymorphic_identity": "entity",
            "polymorphic_on": type,
        }
      
        def __repr__(self):
            return f"Entity(id={self.id!r}, name={self.name!r}, produced_disses={self.produced_disses!r},  targeted_disses={self.targeted_disses!r})"
      
    class Human(Entity):
        __tablename__ = "human"
        id = Column(Integer, ForeignKey("entity.id"), primary_key=True)
        occupation = Column(String)
        first_name = Column(String)
        last_name = Column(String)
        birth_place = Column(String)
        __mapper_args__ = {
            "polymorphic_identity": "human",
        }
      
    class Group(Entity):
        __tablename__ = "group"
        id = Column(Integer, ForeignKey("entity.id"), primary_key=True)
        country = Column(String)
        nb_nominations = Column(Integer)
        inception = Column(DateTime)
        __mapper_args__ = {
            "polymorphic_identity": "group",
        }
      
    class Other(Entity):
        __tablename__ = "other"
        id = Column(Integer, ForeignKey("entity.id"), primary_key=True)
        __mapper_args__ = {
            "polymorphic_identity": "other",
        }
    engine = create_engine(
        f"postgresql://{postgres_user}:{postgres_pswd}@{postgres_host}:{postgres_port}/{postgres_db}"
    )
  
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    context = pa.default_serialization_context()
    songs = context.deserialize(redis_client.get(redis_songs_key))
    groups = context.deserialize(redis_client.get(redis_groups_key))
    persons = context.deserialize(redis_client.get(redis_persons_key))
    songs_df = pd.DataFrame(songs, dtype=str)
    groups_df = pd.DataFrame(groups, dtype=str)
    persons_df = pd.DataFrame(persons, dtype=str)

    def convert_date(str_date: str):
        try:
            return datetime.datetime.strptime(str_date, "%Y-%m-%d")
        except ValueError:
            try:
                return datetime.datetime.strptime(str_date, "%Y")
            except ValueError:
                return None
          
    with Session(engine) as session:
        available_entities = {}
      
        for row in groups_df.iterrows():
            name = row[1]["Name"]
            country = row[1]["Country"]
            nb_nominations = int(row[1]["Number of Nominations"])
            inception = convert_date(row[1]["Inception"])
            target_id = row[1]["group id"]
            group = Group(name = name, wikidata_id = target_id, country = country, nb_nominations = nb_nominations, inception = inception)
            available_entities[target_id] = group
            session.add(group)
      
        for row in persons_df.iterrows():
            name = f'{row[1]["Last Name"]} {row[1]["First Name"]}'
            occupation = row[1]["Occupation Label"]
            first_name = row[1]["First Name"]
            last_name = row[1]["Last Name"]
            birth_place = row[1]["Birth Place"]
            target_id = row[1]["person id"]
          
            person = Human(name = name, wikidata_id = target_id, occupation = occupation, first_name = first_name, last_name = last_name, birth_place = birth_place)
            available_entities[target_id] = person
            session.add(person)
        for row in songs_df.iterrows():
            recorded = convert_date(row[1]["recorded"])
            released = convert_date(row[1]["released_song"])
            artists_names = row[1]["Artist(s)_song"]
            song_wiki_id = row[1]["Wikidata song id"]
            target_wiki_id = row[1]["Wikidata target id"]
            Targets_names = row[1]["Target(s)"]
            genre = row[1]["genre"]
            song_title = row[1]["Song Title"]
            song = Song(title=song_title, release_date = released, genre=genre, record_date = recorded, wikidata_id = song_wiki_id, artist=Other(name=artists_names), target=available_entities.get(target_wiki_id, "") or Other(name=Targets_names))
            session.add(song)
        session.commit()

saving_node = PythonOperator(
    task_id="saving_to_postgres",
    dag=wrangling_dag,
    trigger_rule="none_failed",
    python_callable=_saving_to_postgres,
    op_kwargs={
        "redis_songs_key": "df",
        "redis_groups_key": "groups_df",
        "redis_persons_key": "persons_df",
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

def _saving_to_neo4j(
    pg_user: str,
    pg_pwd: str,
    pg_host: str,
    pg_port: str,
    pg_db: str,
    neo_host: str,
    neo_port: str,
):

    query = """
                SELECT artist_id, target_id, a.name AS artist_name, b.name AS target_name
                FROM song, entity as a, entity as b
                WHERE artist_id=a.id AND target_id=b.id
            """
    
    engine = create_engine(
        f'postgresql://{pg_user}:{pg_pwd}@{pg_host}:{pg_port}/{pg_db}'
    )
    df = pd.read_sql(query, con=engine)
    print(df.columns.values)
    engine.dispose()

    graph = Graph(f"bolt://{neo_host}:{neo_port}")

    graph.delete_all()
    tx = graph.begin()
    for _, row in df.iterrows():
        tx.evaluate('''
        MERGE (a:artist {wikidata_id:$artist_id, name:$artist_name})
        MERGE (b:target {wikidata_id:$target_id, name:$target_name})
        MERGE (a)-[r:diss]->(b)
        ''', parameters = {'artist_id': int(row['artist_id']), 'artist_name': row['artist_name'], 'target_id': int(row['target_id']), 'target_name': row['target_name']})
    tx.commit()

graph_node = PythonOperator(
    task_id="saving_to_neo4j",
    dag=wrangling_dag,
    trigger_rule="all_success",
    python_callable=_saving_to_neo4j,
    op_kwargs={
        "pg_user": "airflow",
        "pg_pwd": "airflow",
        "pg_host": "postgres",
        "pg_port": "5432",
        "pg_db": "postgres",
        "neo_host": "neo4j",
        "neo_port": "7687",
    },
)


get_wikidata_node >> get_dbpedia_node >> merging_node >> cleansing_node >> enrichment_node >> saving_node >> graph_node
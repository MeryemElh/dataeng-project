import datetime

import pandas as pd

from sqlalchemy import create_engine
from py2neo import Graph

from airflow import DAG
from airflow.operators.python import PythonOperator
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
        print(f"{row['artist_name']} dissed {row['target_name']}")
        tx.evaluate('''
        MERGE (a:Artist {wikidata_id:$artist_id, name:$artist_name})
        MERGE (b:Target {wikidata_id:$target_id, name:$target_name})
        MERGE (a)-[r:Dissed]->(b)
        ''', parameters = {'artist_id': int(row['artist_id']), 'artist_name': row['artist_name'], 'target_id': int(row['target_id']), 'target_name': row['target_name']})
    tx.commit()

graph_node = PythonOperator(
    task_id="saving_to_neo4j",
    dag=production_dag,
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

end_node = EmptyOperator(
    task_id="end_task", dag=production_dag, trigger_rule="all_success"
)

start_node >> graph_node >> end_node
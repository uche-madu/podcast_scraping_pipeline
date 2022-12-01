from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pendulum
from datetime import timedelta
import requests
import xmltodict
import os
from dotenv import load_dotenv
from google.cloud.sql.connector import Connector
from google.cloud import storage
import sqlalchemy
import pg8000
import pandas as pd

# Parse .env file and load all variables
load_dotenv()

bucket_name = os.getenv("CLOUD_STORAGE_BUCKET")

# Derived from the documentation: https://cloud.google.com/sql/docs/postgres/connect-connectors#python_1
def connect_with_connector() -> sqlalchemy.engine.base.Engine:
    instance_connection_name = os.environ["INSTANCE_CONNECTION_NAME"]
    db_user = os.environ["GCSQL_POSTGRES_USER"]
    db_pass = os.environ["GCSQL_POSTGRES_PASSWORD"]
    db_name = os.environ["GCSQL_POSTGRES_DATABASE_NAME"]

    # initialize Cloud SQL Python Connector object
    connector = Connector()

    def getconn() -> pg8000.dbapi.Connection:
        conn: pg8000.dbapi.Connection = connector.connect(
            instance_connection_name,
            "pg8000",
            user=db_user,
            password=db_pass,
            db=db_name
        )
        return conn

    pool = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
    )
    return pool

def write_io_to_gcs(bucket_name, filename, audio):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)

    with blob.open("wb") as f:
        f.write(audio.content)

def list_blobs(bucket_name):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)

    blob_list = []
    for blob in blobs:
        blob_list.append(blob.name)

    return blob_list

@dag(
    dag_id='podcast_summary',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022,7,11),
    default_args={
        'email_on_failure': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=1)
    },
    catchup=False
)
def podcast_summary():

    create_table_sql_query = """
    CREATE TABLE IF NOT EXISTS episodes(
        link TEXT PRIMARY KEY,
        title TEXT,
        filename TEXT,
        published TEXT,
        description TEXT
    )
    """
    create_database_postgres = PostgresOperator(
        task_id='create_table_task',
        sql=create_table_sql_query,
        postgres_conn_id='podcasts_conn'
    )

    @task()
    def create_table_sql_query_task():
        with connect_with_connector().connect() as db_conn:
            db_conn.execute(create_table_sql_query)
    
    @task()
    def get_episodes():
        data = requests.get("https://www.marketplace.org/feed/podcast/marketplace")
        feed = xmltodict.parse(data.text)
        episodes = feed["rss"]["channel"]["item"]
        print(f"Found {len(episodes)} episodes.")
        return episodes
    
    @task()
    def load_episodes(episodes):
        with connect_with_connector().connect() as db_conn:
            
            sql = "SELECT link FROM episodes;"
            existing_links = pd.read_sql_query(sql, db_conn)
            
            for episode in episodes:
                if episode["link"] not in existing_links.values:
                    filename = f'{episode["link"].split("/")[-1]}.mp3'
                    new_episodes = []
                    new_episodes.append((episode["link"], episode["title"], 
                        episode["pubDate"], episode["description"], filename))

                    insert_stmt = """INSERT INTO episodes 
                                    (link, title, published, description, filename)
                                    VALUES (%s, %s, %s, %s, %s)"""
                    db_conn.execute(insert_stmt, new_episodes)
    
    @task()
    def load_episodes_postgres(episodes):
        hook = PostgresHook(postgres_conn_id='podcasts_conn')

        stored_link = hook.get_pandas_df("SELECT link from episodes;")
        new_episodes = []
        for episode in episodes:
            if episode["link"] not in stored_link.values:
                filename = f'{episode["link"].split("/")[-1]}.mp3'
                new_episodes.append((episode["link"], episode["title"],
                    episode["pubDate"], episode["description"], filename
                ))
        hook.insert_rows(
            table="episodes", rows=new_episodes, 
            target_fields=["link", "title", "published", "description", "filename"]        
        )

    @task()
    def upload_episodes(episodes):
        for episode in episodes:
            filename = f'{episode["link"].split("/")[-1]}.mp3'

            existing_episodes = list_blobs(bucket_name)
            if filename not in existing_episodes:
                print(f"Uploading {filename}")
                audio = requests.get(episode["enclosure"]["@url"])
                write_io_to_gcs(bucket_name, filename, audio)
                print(f"Successfully uploaded {filename}")


    podcast_episodes = get_episodes()
    podcast_episodes_postgres = get_episodes()
    create_table_sql_query_task = create_table_sql_query_task()

    load_episodes_postgres(podcast_episodes_postgres)
    load_episodes(podcast_episodes)
    upload_episodes(podcast_episodes_postgres)
    
    create_table_sql_query_task >> podcast_episodes
    create_database_postgres >> podcast_episodes_postgres

summary = podcast_summary()

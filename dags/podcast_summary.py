from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLCreateInstanceDatabaseOperator
import pendulum
from datetime import timedelta
import requests
import xmltodict
import os
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

bucket_name = os.getenv("CLOUD_STORAGE_BUCKET")

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
        'retries': 3,
        'retry_delay': timedelta(minutes=2)
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
    # create_database = CloudSQLCreateInstanceDatabaseOperator(

    # )
    create_database = PostgresOperator(
        task_id='create_table_task',
        sql=create_table_sql_query,
        postgres_conn_id='podcasts_conn'
    )
    
    @task()
    def get_episodes():
        data = requests.get("https://www.marketplace.org/feed/podcast/marketplace")
        feed = xmltodict.parse(data.text)
        episodes = feed["rss"]["channel"]["item"]
        print(f"Found {len(episodes)} episodes.")
        return episodes
    
    @task()
    def load_episodes(episodes):
        hook = PostgresHook(postgres_conn_id='podcasts_conn')
        stored_link = hook.get_pandas_df("SELECT link FROM episodes;")
        new_episodes = []
        for episode in episodes:
            if episode["link"] not in stored_link.values:
                filename = f'{episode["link"].split("/")[-1]}.mp3'
                new_episodes.append((episode["link"], episode["title"], 
                    episode["pubDate"], episode["description"], filename))
        hook.insert_rows(table="episodes", rows=new_episodes, 
            target_fields=["link", "title", "published", "description", "filename"])

    @task()
    def download_episodes(episodes):
        for episode in episodes:
            filename = f'{episode["link"].split("/")[-1]}.mp3'

            existing_episodes = list_blobs(bucket_name)
            if filename not in existing_episodes:
                print(f"Uploading {filename}")
                audio = requests.get(episode["enclosure"]["@url"])
                write_io_to_gcs(bucket_name, filename, audio)
                print(f"Successfully uploaded {filename}")


    podcast_episodes = get_episodes()
    load_episodes(podcast_episodes)
    download_episodes(podcast_episodes)
    
    create_database >> podcast_episodes

summary = podcast_summary()

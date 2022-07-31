from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
from datetime import timedelta
import requests
import xmltodict
import os

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

    create_database = PostgresOperator(
        task_id='create_table_task',
        sql=create_table_sql_query,
        postgres_conn_id='podcasts'
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
        hook = PostgresHook(postgres_conn_id='podcasts')
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
            dirname = os.path.dirname(__file__)
            audio_path = os.path.join(dirname, "episodes", filename)
            if not os.path.exists(audio_path):
                print(f"Downloading {filename}")
                audio = requests.get(episode["enclosure"]["@url"])
                with open(audio_path, "wb+") as f:
                    f.write(audio.content)

    podcast_episodes = get_episodes()
    load_episodes(podcast_episodes)
    download_episodes(podcast_episodes)
    
    create_database >> podcast_episodes

summary = podcast_summary()

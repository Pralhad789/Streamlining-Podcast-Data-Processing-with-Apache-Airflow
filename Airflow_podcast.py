import os
import json
import requests
import xmltodict

from airflow.decorators import dag, task
import pendulum
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

from vosk import Model, KaldiRecognizer
from pydub import AudioSegment

PODCAST_URL = "https://www.marketplace.org/feed/podcast/marketplace/"
EPISODE_FOLDER = "episodes"
FRAME_RATE = 16000

# Define an Apache Airflow DAG with the ID 'airflow_podcast'.
@dag(
    dag_id='airflow_podcast',
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 5, 30),
    catchup=False,
)
def airflow_podcast():

    # SQL statement to create the 'episodes' table with specified columns.
    create_database = SqliteOperator(
        task_id='create_table_sqlite',
        sql=r"""
        CREATE TABLE IF NOT EXISTS episodes (
            link TEXT PRIMARY KEY,
            title TEXT,
            filename TEXT,
            published TEXT,
            description TEXT,
            transcript TEXT
        );
        """,
        sqlite_conn_id="podcasts"
    )

    @task()
    def get_episodes():

        # Fetch podcast data from the specified URL using the 'requests' library.
        data = requests.get(PODCAST_URL)
        
        # Parse the retrieved XML data into a Python dictionary using 'xmltodict'.
        feed = xmltodict.parse(data.text)
        
        # Extract the 'episodes' from the parsed XML data.
        episodes = feed["rss"]["channel"]["item"]
        
        print(f"Found {len(episodes)} episodes.")
        return episodes

    # Call the 'get_episodes' function to retrieve podcast episodes.
    podcast_episodes = get_episodes()
    
    # Set a downstream relationship
    create_database.set_downstream(podcast_episodes)

    @task()
    def load_episodes(episodes):
        
        # Create a SQLite database connection hook.
        hook = SqliteHook(sqlite_conn_id="podcasts")
        
        # Retrieve previously stored episodes from the 'episodes' table as a Pandas DataFrame.
        stored_episodes = hook.get_pandas_df("SELECT * from episodes;")
        
        new_episodes = []
        
        # Iterate through the retrieved episodes.
        for episode in episodes:
            if episode["link"] not in stored_episodes["link"].values:
                # Generate a filename for the episode based on its link.
                filename = f"{episode['link'].split('/')[-1]}.mp3"
                new_episodes.append([episode["link"], episode["title"], episode["pubDate"], episode["description"], filename])

        # Insert the new episode rows into the 'episodes' table.
        hook.insert_rows(table='episodes', rows=new_episodes, target_fields=["link", "title", "published", "description", "filename"])
        return new_episodes

    # Call the 'load_episodes' function to load and store new podcast episodes.
    new_episodes = load_episodes(podcast_episodes)

    @task()
    def download_episodes(episodes):
        audio_files = []
        for episode in episodes:

            # Extract the end of the episode link to generate a filename.
            name_end = episode["link"].split('/')[-1]
            filename = f"{name_end}.mp3"
            
            # Define the local path where the audio file will be saved.
            audio_path = os.path.join(EPISODE_FOLDER, filename)
            
            # Check if the audio file already exists locally.
            if not os.path.exists(audio_path):
                
                # If not, download the audio file.
                print(f"Downloading {filename}")                
                audio = requests.get(episode["enclosure"]["@url"])
                
                # Write the downloaded audio content to the local file.
                with open(audio_path, "wb+") as f:
                    f.write(audio.content)
            
            # Append information about the downloaded audio file to the list.
            audio_files.append({
                "link": episode["link"],
                "filename": filename
            })
        return audio_files

    # Call the 'download_episodes' function to download and store podcast audio files.
    audio_files = download_episodes(podcast_episodes)

    @task()
    def speech_to_text(audio_files, new_episodes):
        
        # Initialize a hook to interact with the SQLite database.
        hook = SqliteHook(sqlite_conn_id="podcasts")
        
        # Retrieve untranscribed episodes from the database.
        untranscribed_episodes = hook.get_pandas_df("SELECT * from episodes WHERE transcript IS NULL;")

        # Define the speech recognition model and settings.
        model = Model(model_name="vosk-model-en-us-0.22-lgraph")
        rec = KaldiRecognizer(model, FRAME_RATE)
        rec.SetWords(True)

        # Iterate through untranscribed episodes for transcription.
        for index, row in untranscribed_episodes.iterrows():
            
            print(f"Transcribing {row['filename']}")
            filepath = os.path.join(EPISODE_FOLDER, row["filename"])
            
            # Load and configure the audio segment for processing.
            mp3 = AudioSegment.from_mp3(filepath)
            mp3 = mp3.set_channels(1)
            mp3 = mp3.set_frame_rate(FRAME_RATE)

            step = 20000
            transcript = ""
            
            # Iterate through audio segments and perform speech recognition.
            for i in range(0, len(mp3), step):
                
                print(f"Progress: {i/len(mp3)}")
                segment = mp3[i:i+step]
                
                # Accept and process the waveform segment.
                rec.AcceptWaveform(segment.raw_data)
                result = rec.Result()
                text = json.loads(result)["text"]
                
                # Append the recognized text to the transcript.
                transcript += text
            
            # Insert the transcript into the database for the episode.
            hook.insert_rows(table='episodes', rows=[[row["link"], transcript]], target_fields=["link", "transcript"], replace=True)

    # Call the 'speech_to_text' function to transcribe audio files and update the database.
    speech_to_text(audio_files, new_episodes)

summary = airflow_podcast()
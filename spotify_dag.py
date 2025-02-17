from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import io

# Constants
S3_BUCKET = "aws-bucket-etl-spotify-veb"
S3_PREFIX_ALBUM = "transformed_data/album_data/"
S3_PREFIX_ARTIST = "transformed_data/artist_data/"
S3_PREFIX_SONGS = "transformed_data/songs_data/"

POSTGRES_CONN_ID = "postgres_conn"
TABLE_NAME_ALBUM = "jobs_data_album"
TABLE_NAME_SONGS = "jobs_data_songs"
TABLE_NAME_ARTIST = "jobs_data_artist"

def download_from_s3_album(**context):
    """
    Downloads data from S3 and converts it to a pandas DataFrame.
    The function automatically processes the file detected by the sensor.
    """
    
        
    try:
        s3_hook = S3Hook(aws_conn_id="aws_conn")
        objects = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=S3_PREFIX_ALBUM)

        if not objects:
            raise ValueError("No files found in the specified S3 prefix")


        detected_key = sorted(objects)[-1]  # Sort to get the latest file
        print(f"Processing file: {detected_key}")

        file_obj = s3_hook.get_key(key=detected_key, bucket_name=S3_BUCKET)
        if not file_obj:
            raise ValueError(f"File {detected_key} not found in {S3_BUCKET}")

        content = file_obj.get()['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(content))

        if df.empty:
            raise ValueError("Downloaded file contains no data")

        context['task_instance'].xcom_push(key="job_data", value={
            'data': df.to_dict(),
            'filename': detected_key
        })

    except Exception as e:
        print(f"Error in download_from_s3: {str(e)}")
        raise

def download_from_s3_artist(**context):
    """
    Downloads data from S3 and converts it to a pandas DataFrame.
    The function automatically processes the file detected by the sensor.
    """
    
        
    try:
        s3_hook = S3Hook(aws_conn_id="aws_conn")
        objects = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=S3_PREFIX_ARTIST)

        if not objects:
            raise ValueError("No files found in the specified S3 prefix")


        detected_key = sorted(objects)[-1]  # Sort to get the latest file
        print(f"Processing file: {detected_key}")

        file_obj = s3_hook.get_key(key=detected_key, bucket_name=S3_BUCKET)
        if not file_obj:
            raise ValueError(f"File {detected_key} not found in {S3_BUCKET}")

        content = file_obj.get()['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(content))

        if df.empty:
            raise ValueError("Downloaded file contains no data")

        context['task_instance'].xcom_push(key="job_data", value={
            'data': df.to_dict(),
            'filename': detected_key
        })

    except Exception as e:
        print(f"Error in download_from_s3: {str(e)}")
        raise

def download_from_s3_songs(**context):
    """
    Downloads data from S3 and converts it to a pandas DataFrame.
    The function automatically processes the file detected by the sensor.
    """
    
        
    try:
        s3_hook = S3Hook(aws_conn_id="aws_conn")
        objects = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=S3_PREFIX_SONGS)

        if not objects:
            raise ValueError("No files found in the specified S3 prefix")


        detected_key = sorted(objects)[-1]  # Sort to get the latest file
        print(f"Processing file: {detected_key}")

        file_obj = s3_hook.get_key(key=detected_key, bucket_name=S3_BUCKET)
        if not file_obj:
            raise ValueError(f"File {detected_key} not found in {S3_BUCKET}")

        content = file_obj.get()['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(content))

        if df.empty:
            raise ValueError("Downloaded file contains no data")

        context['task_instance'].xcom_push(key="job_data", value={
            'data': df.to_dict(),
            'filename': detected_key
        })

    except Exception as e:
        print(f"Error in download_from_s3: {str(e)}")
        raise

def insert_into_postgres_album(**context):
    """
    Retrieves data from XCom and inserts it into PostgreSQL.
    Includes error handling and basic data validation.
    """
    try:
        ti = context['task_instance']
        job_data = ti.xcom_pull(task_ids="download_from_s3_album", key="job_data")
        
        if not job_data:
            raise ValueError("No data received from previous task")
            
        df = pd.DataFrame(job_data['data'])
        filename = job_data['filename']
        
        # Additional data validation
        if df.empty:
            raise ValueError("DataFrame is empty")
            
        # Get PostgreSQL connection
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        
        # Insert into PostgreSQL with error handling
        df.to_sql(
            name=TABLE_NAME_ALBUM,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=1000  # Process in chunks for large datasets
        )
        
        print(f"Successfully inserted {len(df)} rows from {filename} into {TABLE_NAME_ALBUM}")
        
    except Exception as e:
        print(f"Error in insert_into_postgres: {str(e)}")
        raise

def insert_into_postgres_artist(**context):
    """
    Retrieves data from XCom and inserts it into PostgreSQL.
    Includes error handling and basic data validation.
    """
    try:
        ti = context['task_instance']
        job_data = ti.xcom_pull(task_ids="download_from_s3_artist", key="job_data")
        
        if not job_data:
            raise ValueError("No data received from previous task")
            
        df = pd.DataFrame(job_data['data'])
        filename = job_data['filename']
        
        # Additional data validation
        if df.empty:
            raise ValueError("DataFrame is empty")
            
        # Get PostgreSQL connection
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        
        # Insert into PostgreSQL with error handling
        df.to_sql(
            name=TABLE_NAME_ARTIST,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=1000  # Process in chunks for large datasets
        )
        
        print(f"Successfully inserted {len(df)} rows from {filename} into {TABLE_NAME_ARTIST}")
        
    except Exception as e:
        print(f"Error in insert_into_postgres: {str(e)}")
        raise

def insert_into_postgres_songs(**context):
    """
    Retrieves data from XCom and inserts it into PostgreSQL.
    Includes error handling and basic data validation.
    """
    try:
        ti = context['task_instance']
        job_data = ti.xcom_pull(task_ids="download_from_s3_songs", key="job_data")
        
        if not job_data:
            raise ValueError("No data received from previous task")
            
        df = pd.DataFrame(job_data['data'])
        filename = job_data['filename']
        
        # Additional data validation
        if df.empty:
            raise ValueError("DataFrame is empty")
            
        # Get PostgreSQL connection
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        
        # Insert into PostgreSQL with error handling
        df.to_sql(
            name=TABLE_NAME_SONGS,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=1000  # Process in chunks for large datasets
        )
        
        print(f"Successfully inserted {len(df)} rows from {filename} into {TABLE_NAME_SONGS}")
        
    except Exception as e:
        print(f"Error in insert_into_postgres: {str(e)}")
        raise

# Define Airflow DAG
default_args = {
    'owner': 'Vaibhav Gupta',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 17),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

dag = DAG(
    dag_id='s3_sensor_to_postgres',
    default_args=default_args,
    description='Monitor S3 for new data and load into PostgreSQL',
    schedule_interval=None,  # Runs only when file appears in S3
    catchup=False,
    max_active_runs=1,  # Prevent multiple concurrent runs
    tags=['s3', 'postgres', 'etl'],
)

# S3 Sensor Task - Waits for any new file in the specified prefix
sense_s3_file_album = S3KeySensor(
    task_id='sense_s3_file_album',
    bucket_name=S3_BUCKET,
    bucket_key=S3_PREFIX_ALBUM,  # Monitor the entire prefix instead of a specific file
    wildcard_match=True,
    aws_conn_id='aws_conn',
    timeout=600,  # Max wait time (10 mins)
    poke_interval=60,  # Checks every 60 seconds
    mode='reschedule',  # Can be changed to 'reschedule' for better resource usage
    soft_fail=False,  # Fail the task if timeout is reached
    dag=dag,
)

sense_s3_file_songs = S3KeySensor(
    task_id='sense_s3_file_songs',
    bucket_name=S3_BUCKET,
    bucket_key=S3_PREFIX_SONGS,  # Monitor the entire prefix instead of a specific file
    wildcard_match=True,
    aws_conn_id='aws_conn',
    timeout=600,  # Max wait time (10 mins)
    poke_interval=60,  # Checks every 60 seconds
    mode='reschedule',  # Can be changed to 'reschedule' for better resource usage
    soft_fail=False,  # Fail the task if timeout is reached
    dag=dag,
)

sense_s3_file_artists = S3KeySensor(
    task_id='sense_s3_file_artists',
    bucket_name=S3_BUCKET,
    bucket_key=S3_PREFIX_ARTIST,  # Monitor the entire prefix instead of a specific file
    wildcard_match=True,
    aws_conn_id='aws_conn',
    timeout=600,  # Max wait time (10 mins)
    poke_interval=60,  # Checks every 60 seconds
    mode='reschedule',  # Can be changed to 'reschedule' for better resource usage
    soft_fail=False,  # Fail the task if timeout is reached
    dag=dag,
)

start_task = DummyOperator(task_id='start_task', dag=dag)

# Download and Process Task
download_task_album = PythonOperator(
    task_id='download_from_s3_album',
    python_callable=download_from_s3_album,
    provide_context=True,
    dag=dag,
)

download_task_artist = PythonOperator(
    task_id='download_from_s3_artist',
    python_callable=download_from_s3_artist,
    provide_context=True,
    dag=dag,
)

download_task_songs = PythonOperator(
    task_id='download_from_s3_songs',
    python_callable=download_from_s3_songs,
    provide_context=True,
    dag=dag,
)

# Insert into PostgreSQL Task
insert_task_album = PythonOperator(
    task_id='insert_into_postgres_album',
    python_callable=insert_into_postgres_album,
    provide_context=True,
    dag=dag,
)

insert_task_artist = PythonOperator(
    task_id='insert_into_postgres_artist',
    python_callable=insert_into_postgres_artist,
    provide_context=True,
    dag=dag,
)

insert_task_songs = PythonOperator(
    task_id='insert_into_postgres_songs',
    python_callable=insert_into_postgres_songs,
    provide_context=True,
    dag=dag,
)

# Task Dependencies
start_task >> [sense_s3_file_album,sense_s3_file_artists,sense_s3_file_songs] 
sense_s3_file_album >> download_task_album >> insert_task_album
sense_s3_file_artists >> download_task_artist >> insert_task_artist
sense_s3_file_songs >> download_task_songs >> insert_task_songs
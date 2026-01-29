import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET = "bronze-transactions"
LOCAL_DIR = "/tmp/minio_downloads"  # use absolute path for Airflow

SNOWFLAKE_USER = "nikita19"
SNOWFLAKE_PASSWORD = "Nikitagaikwad@19"
SNOWFLAKE_ACCOUNT = "uy68072.ap-southeast-1"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DB = "STOCKS_MDS"
SNOWFLAKE_SCHEMA = "COMMON"

def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    #Gets list of files in the bucket
     #If empty → returns []
    objects = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    local_files = []
    #Loop through each file in MinIO
    for obj in objects:
        #File name inside bucket
        key = obj["Key"]
        #Creates local path:
        local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
        #Downloads file from MinIO to local folder
        s3.download_file(BUCKET, key, local_file)
        print(f"Downloaded {key} -> {local_file}")
        #Save file path for next task
        local_files.append(local_file)
    #Sends file list to next Airflow task 
    return local_files

#**kwargs used to:
#Access Airflow context
def load_to_snowflake(**kwargs):
    #Gets file list from previous task
    local_files = kwargs['ti'].xcom_pull(task_ids='download_minio')
    if not local_files:
        print("No files to load.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA
    )
    #Cursor is used to execute SQL queries
    cur = conn.cursor()

    for f in local_files:
        #Uploads local file to Snowflake internal stage
        #@%table_name
        cur.execute(f"PUT file://{f} @%bronze_stock_quotes_raw")
        print(f"Uploaded {f} to Snowflake stage")

    #Loads JSON files into Snowflake table
    cur.execute("""
        COPY INTO bronze_stock_quotes_raw
        FROM @%bronze_stock_quotes_raw
        FILE_FORMAT = (TYPE=JSON)
    """)
    print("COPY INTO executed")

    cur.close()
    conn.close()

#default dag setting
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 9),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),  #Retry once after 5 minutes if failed
}

with DAG(
    #DAG name (shown in Airflow UI)
    "minio_to_snowflake",
    default_args=default_args,
    #Runs every 1 minute
    schedule_interval="*/1 * * * *",  # every 1 minutes
    #Don’t run missed past executions
    catchup=False,
) as dag:
    #Downloads data from MinIO
    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )
     #Loads data into Snowflake
    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,
    )
    #Task1 must finish before Task2 starts
    task1 >> task2
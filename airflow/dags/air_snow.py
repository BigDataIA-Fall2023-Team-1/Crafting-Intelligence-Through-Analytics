from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from snowflake import connector
import botocore

# Define your Snowflake connection parameters
SNOWFLAKE_CONN_ID = 'snowflake_conn'
WAREHOUSE = 'HOL_WH1'
DATABASE = 'HOL_DB1'
SCHEMA = 'JOBS'
TABLE = 'DATA'

# Define your S3 connection parameters
S3_CONN_ID = 's3_conn'
BUCKET_NAME = 'a3-damg'
CSV_KEY = 's3://a3-damg/test1.csv'

# Define your Airflow DAG parameters
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'csv_to_snowflake',
    default_args=default_args,
    description='DAG to transfer CSV data to Snowflake',
    schedule_interval='@daily',
)

# Function to fetch CSV file from S3
def fetch_csv_from_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    try:
        file_content_bytes = s3_hook.read_key(key=CSV_KEY, bucket_name=BUCKET_NAME)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"File not found at {CSV_KEY}. Proceeding with an empty dataset.")
            return []
        
        # Handle other errors
        raise e

    # Check if the file content is not empty
    if file_content_bytes:
        file_content = file_content_bytes.decode('utf-8')
        # Split the file content into lines and skip the first line (header)
        csv_data = file_content.split('\n')[1:]
        return csv_data
    else:
        raise ValueError("File content is empty or could not be decoded.")

# Function to create table and insert data into Snowflake
def create_table_and_insert(**kwargs):
    snowflake_hook = connector.SnowflakeConnectorHook(conn_name_attr=SNOWFLAKE_CONN_ID)
    connection = snowflake_hook.get_conn()
    cursor = connection.cursor()

    # Create the Snowflake table if it doesn't exist
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA}.{TABLE} (
        COMPANY_NAME VARCHAR,
        JOB_TITLE VARCHAR,
        LOCATION VARCHAR,
        COMPANY_DOMAIN VARCHAR,
        JOB_URL VARCHAR,
        POSTED_ON DATE,
        JOB_ID INTEGER,
        CITY VARCHAR,
        STATE VARCHAR
    )
    """
    cursor.execute(create_table_sql)

    # Assuming your CSV file has a header, so skipping the first row
    csv_data = kwargs['task_instance'].xcom_pull(task_ids='fetch_csv_from_s3')[1:]
    
    # Construct the Snowflake SQL INSERT statement
    insert_sql = f"INSERT INTO {DATABASE}.{SCHEMA}.{TABLE} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"  

    # Execute the INSERT statement for each row in the CSV
    for row in csv_data:
        cursor.execute(insert_sql, row.split(','))  # Assuming CSV is comma-separated

    # Commit the changes and close the connection
    connection.commit()
    cursor.close()
    connection.close()

# Define Airflow tasks
fetch_csv_task = PythonOperator(
    task_id='fetch_csv_from_s3',
    python_callable=fetch_csv_from_s3,
    provide_context=True,
    dag=dag,
)

create_table_and_insert_task = PythonOperator(
    task_id='create_table_and_insert',
    python_callable=create_table_and_insert,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
fetch_csv_task >> create_table_and_insert_task

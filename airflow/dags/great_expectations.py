import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import snowflake.connector
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.types.base import (
    DataContextConfig,
    CheckpointConfig
)
from great_expectations.dataset import SqlAlchemyDataset
import pandas as pd
import great_expectations as ge
import boto3
import sys

load_dotenv()

# Snowflake connection details
SNOWFLAKE_USER = os.environ.get("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = os.environ.get("SNOWFLAKE_TABLE")

# S3 connection details
AIRFLOW_VAR_AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY')
AIRFLOW_VAR_AWS_SECRET_KEY = os.environ.get('AWS_SECRET_KEY')
AIRFLOW_VAR_AWS_REGION = os.environ.get('AWS_REGION')
S3_BUCKET = 'a3-damg'
S3_KEY = 'great_expectations/report.html'

# Snowflake connection
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
)

# Airflow DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'great_expectations_dag',
    default_args=default_args,
    description='Run Great Expectations on Snowflake table and upload report to S3',
    schedule_interval='0 0 * * *',  # Set your desired schedule
    start_date=datetime(2023, 4, 11),
    catchup=False
)

# Function to run Great Expectations on Snowflake table
def run_great_expectations():
    # Connect to Snowflake and fetch data
    cursor = conn.cursor()
    cursor.execute(f'SELECT * FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}')
    cols = [col[0] for col in cursor.description]
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    # Convert data to a Pandas DataFrame
    df = pd.DataFrame(results, columns=cols)

    # Define your expectation suite (modify according to your use case)
    suite_name = "jobs_validation"
    expectation_suite = {
        "expectation_suite_name": suite_name,
        "meta": {"great_expectations_version": ge.__version__, "data_asset_name": "my_data_asset"},
        "expectations": [
            # Advanced Expectations for COMPANY_NAME column
            {
                "expectation_type": "expect_column_values_to_match_regex",
                "column": "COMPANY_NAME",
                "regex": r"^[A-Za-z0-9\s\.,'-]+$",
                "mostly": 0.95
            },
            # Advanced Expectations for JOB_TITLE column
            {
                "expectation_type": "expect_column_values_to_match_regex",
                "column": "JOB_TITLE",
                "regex": r"^[A-Za-z0-9\s\.,'-]+$",
                "mostly": 0.95
            },
            # Advanced Expectations for LOCATION column
            {
                "expectation_type": "expect_column_values_to_match_regex",
                "column": "LOCATION",
                "regex": r"^[A-Za-z0-9\s\.,'-]+$",
                "mostly": 0.95
            },
            # Advanced Expectations for COMPANY_DOMAIN column
            {
                "expectation_type": "expect_column_values_to_match_regex",
                "column": "COMPANY_DOMAIN",
                "regex": r"^[A-Za-z0-9\.-]+\.[A-Za-z]{2,}$",
                "mostly": 0.95
            },
            # Advanced Expectations for JOB_URL column
            {
                "expectation_type": "expect_column_values_to_match_regex",
                "column": "JOB_URL",
                "regex": r"^https://www\.linkedin\.com/jobs/view/.+$"
            },
            # Advanced Expectations for POSTED_ON column
            {
                "expectation_type": "expect_column_values_to_be_between",
                "column": "POSTED_ON",
                "min_value": "2022-01-01",
                "max_value": datetime.today().strftime('%Y-%m-%d'),
                "mostly": 0.95
            },
            # Advanced Expectations for JOB_ID column
            {
                "expectation_type": "expect_column_values_to_be_of_type",
                "column": "JOB_ID",
                "type_": "int",
                "mostly": 0.95
            },
            # Advanced Expectations for CITY column
            {
                "expectation_type": "expect_column_values_to_match_regex",
                "column": "CITY",
                "regex": r"^[A-Za-z\s\.,'-]+$",
                "mostly": 0.95
            },
            # Advanced Expectations for STATE column
            {
                "expectation_type": "expect_column_values_to_match_regex",
                "column": "STATE",
                "regex": r"^[A-Za-z]{2}$",
                "mostly": 0.95
            },
            # Add more expectations as needed
        ],
    }

    # Initialize a Great Expectations DataContext
    context = ge.data_context.DataContext('/path/to/great_expectations')

    # Create a new SqlAlchemyDataset for the Snowflake table
    data_asset_name = "my_data_asset"
    datasource_name = "snowflake"
    batch_kwargs = {
        "table": SNOWFLAKE_TABLE,
        "schema": SNOWFLAKE_SCHEMA,
        "database": SNOWFLAKE_DATABASE,
    }
    batch = SqlAlchemyDataset(
        context,
        batch_kwargs=batch_kwargs,
        expectation_suite_name=suite_name,
        data_asset_name=data_asset_name,
        datasource_name=datasource_name,
    )
    
    # Save the expectation suite
    context.save_expectation_suite(expectation_suite, suite_name)

    # Validate the DataFrame against the expectation suite
    results = df.validate(expectation_suite=suite_name, result_format="SUMMARY")

    # Implement further actions based on validation results
    print(results)

    # Set up a checkpoint
    checkpoint_name = "my_checkpoint"
    checkpoint_config = CheckpointConfig(
        name=checkpoint_name,
        data_asset_name=data_asset_name,
        run_name=datetime.utcnow().isoformat(),
    )
    results = context.run_checkpoint(
        checkpoint_name=checkpoint_name,
        run_id=datetime.utcnow().isoformat(),
        batch=batch,
    )

    # Implement further actions based on checkpoint results
    print(results)

    return None

# Function to upload Great Expectations report to S3
def upload_to_s3():
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AIRFLOW_VAR_AWS_ACCESS_KEY,
        aws_secret_access_key=AIRFLOW_VAR_AWS_SECRET_KEY,
        region_name=AIRFLOW_VAR_AWS_REGION,
    )

    # Implement code to upload the Great Expectations report to S3
    # You can use the S3 client to upload the report file

    return None

# Airflow tasks
run_ge_task = PythonOperator(
    task_id='run_great_expectations',
    python_callable=run_great_expectations,
    dag=dag
)

upload_ge_report_task = PythonOperator(
    task_id='upload_ge_report',
    python_callable=upload_to_s3,
    dag=dag
)

# Set task dependencies
run_ge_task >> upload_ge_report_task

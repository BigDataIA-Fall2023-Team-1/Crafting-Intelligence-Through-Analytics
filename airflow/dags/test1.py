from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from datetime import datetime,timedelta
import pandas as pd
import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from time import sleep
from random import randint
from selenium.webdriver.chrome.service import Service
import re
import boto3
from io import BytesIO
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

conn = snowflake.connector.connect(
    account='wt07623.us-east4.gcp',
    user ='RONNIE',
    password = 'Pratham@123',
    warehouse= 'HOL_WH1',
    database='HOL_DB1',
    schema= 'PUBLIC',
)
# Set your S3 credentials
aws_access_key_id = os.getenv("AIRFLOW_VAR_AWS_ACCESS_KEY")
aws_secret_access_key = os.getenv("AIRFLOW_VAR_AWS_SECRET_KEY")
s3_bucket_name = os.getenv("AIRFLOW_VAR_S3_BUCKET_NAME")

# Function to perform the scraping
def scrape_jobs():  
    job_search_keyword = ['Data Engineer', 'Data Analyst', 'Software Developer','Data Scientist','Software Engineer','Machine Learning','Cloud','Supply Chain','DevOps', 'Business Analyst', 'AI']
    all_jobs = []
    remote_webdriver = 'http://172.18.0.4:4444'
    for job_ in job_search_keyword:
        option= webdriver.ChromeOptions()
        option.add_argument("--disable-dev-shm-usage")
        option.add_argument("--incognito")
        driver = webdriver.Remote(f'{remote_webdriver}', options=option)
        pagination_url = 'https://www.indeed.com/jobs?q={}&l={}&radius=35&filter=0&sort=date&start={}'
        print(pagination_url)  # Corrected variable name
        driver.get(pagination_url.format(job_, 'United+States', 0)) 
        for i in range(0,1):
                driver.get(pagination_url.format(job_, 'United+States', i * 10))
                print(driver.current_url)             
                sleep(randint(2, 6))
                #driver.refresh()
                WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "mosaic-jobResults")))
                job_page = driver.find_element(By.ID, "mosaic-jobResults")
                jobs = job_page.find_elements(By.CLASS_NAME, "job_seen_beacon")
                for jj in jobs:
                    job_title = jj.find_element(By.CLASS_NAME, "jobTitle")
                    job_location = jj.find_element(By.CLASS_NAME, "company_location")
                    all_jobs.append([job_title.text,job_location.text,
                    job_title.find_element(By.CSS_SELECTOR,"a").get_attribute("id"),      
                    jj.find_element(By.CLASS_NAME,"date").text,
                    job_title.find_element(By.CSS_SELECTOR,"a").get_attribute("href")])
        driver.quit()
        df= pd.DataFrame(all_jobs, columns=['job', 'company','job_id', 'Job_Posted_Date', 'link1'])
    return df


def Data_cleaning(**kwargs):
    ti = kwargs["ti"]
    df = ti.xcom_pull(task_ids="Data_Scraping")
    aws_access_key_id = os.getenv("AIRFLOW_VAR_AWS_ACCESS_KEY")
    aws_secret_access_key = os.getenv("AIRFLOW_VAR_AWS_SECRET_KEY")
    message="Not Available"
    message1="Remote"
    df['Job_Posted_Date'] = df['Job_Posted_Date'].apply(lambda x: convert_relative_time(x) if x is not None else None)
    df['Job_Posted_Date'].fillna(datetime.now().strftime("%m/%d/%Y"), inplace=True)
    for index, row in df.iterrows():
        company_data = row['company'].split('\n') if pd.notna(row['company']) else ['']
        df.at[index, 'Company Name'] = company_data[0]

        if len(company_data) > 1 and ', ' in company_data[1]:
            city_state = company_data[1].split(', ', 1)
            df.at[index, 'City'] = city_state[0]
            df.at[index, 'State'] = city_state[1] if len(city_state) > 1 else message
        else:
            df.at[index, 'City'] = message1
            df.at[index, 'State'] = message
    df['Location'] = 'United States'
    df.drop(['company'], axis=1, inplace=True)
    new_df = pd.DataFrame(columns=['Company_Name', 'Job_Title', 'Location', 'Job_Url', 'Posted_On', 'Job_ID', 'City', 'State'])
    new_df['Company_Name'] = df['Company Name']
    new_df['Job_Title'] = df['job']
    new_df['Location'] = df['Location']
    new_df['Job_Url'] = df['link1']
    new_df['Posted_On'] = df['Job_Posted_Date']
    new_df['Job_ID'] = df['job_id']
    new_df['City'] = df['City']
    new_df['State'] = df['State']
    new_df = new_df.dropna()
    # Convert 'Posted On' to datetime format and then to "mm-dd-yyyy"
    new_df['Posted_On'] = pd.to_datetime(new_df['Posted_On'], errors='coerce')
    new_df['Posted_On'] = pd.to_datetime(new_df['Posted_On'].dt.strftime('%m-%d-%Y'))
    new_df = new_df.drop_duplicates(subset=['Job_ID'])

    current_directory = os.path.dirname(os.path.abspath(__file__ or '.'))
    filename = f"raw_jobs.csv"
    filepath = os.path.join(current_directory, filename)
    # Save the DataFrame to a CSV file
    cleaned_csv=new_df.to_csv(filepath, index=False)
    csv_content = new_df.to_csv(index=False).encode('utf-8')

    # Upload the embeddings for 'pypdf_content' to S3
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    s3.put_object(Bucket='damg-scraped-jobs', Key='scraped.csv', Body=csv_content)
    time.sleep(20)


def load_stage():
    cursor = conn.cursor()
    # Truncate the JOBS_STAGE table
    cursor.execute("""TRUNCATE TABLE JOBS_STAGE """)

    # Retrieve AWS credentials
    aws_access_key_id = os.getenv("AIRFLOW_VAR_AWS_ACCESS_KEY")
    aws_secret_access_key = os.getenv("AIRFLOW_VAR_AWS_SECRET_KEY")

    # Formulate the COPY INTO command with actual credentials
    copy_command = f"""
        COPY INTO JOBS_STAGE
        FROM 's3://damg-scraped-jobs/scraped.csv'
        CREDENTIALS = (AWS_KEY_ID='{aws_access_key_id}' AWS_SECRET_KEY='{aws_secret_access_key}')
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' RECORD_DELIMITER = '\n' SKIP_HEADER = 1);
    """

    # Execute the COPY INTO command
    cursor.execute(copy_command)

    # Close the cursor and connection
    cursor.close()
    conn.close()

    # Wait for 20 seconds
    time.sleep(20)


def merge():
    cursor = conn.cursor()
    # Modify the CREATE TABLE statement based on your requirements
    cursor.execute("""
        MERGE INTO HOL_DB1.PUBLIC.US_JOBS_DATABASE t
        USING HOL_DB1.PUBLIC.JOBS_STAGE s
        ON t.JOB_ID = s.JOB_ID
        WHEN MATCHED THEN
            UPDATE SET t.COMPANY_NAME = s.COMPANY_NAME,
                       t.JOB_TITLE = s.JOB_TITLE,
                       t.LOCATION = s.LOCATION,
                       t.JOB_URL = s.JOB_URL,
                       t.POSTED_ON = s.POSTED_ON,
                       t.JOB_ID = s.JOB_ID,
                       t.CITY = s.CITY,
                       t.STATE = s.STATE
        WHEN NOT MATCHED THEN
            INSERT (COMPANY_NAME, JOB_TITLE, LOCATION, JOB_URL, POSTED_ON, JOB_ID, CITY, STATE)
            VALUES (s.COMPANY_NAME, s.JOB_TITLE, s.LOCATION, s.JOB_URL, s.POSTED_ON, s.JOB_ID, s.CITY, s.STATE);
        """)
    cursor.close()
    conn.close()

def convert_relative_time(relative_time):
    try:
        # Extract numerical value using regular expression
        days_ago = int(re.search(r'\d+', relative_time).group())
        # Calculate the datetime
        date_result = datetime.now() - timedelta(days=days_ago)
        # Return the formatted date
        return date_result.strftime("%m/%d/%Y")
    except:
        return None
    

dag= DAG(
    dag_id= "Final",
    schedule_interval="@hourly",
    start_date=days_ago(0),
    dagrun_timeout= timedelta(minutes=60),
    tags=["Final Project","damg7245"],
)
with dag:
    # Define PythonOperators for each task
    scraping_task = PythonOperator(
        task_id='Data_Scraping',
        python_callable=scrape_jobs
    )
    csv_to_dataframe_task = PythonOperator(
        task_id='Data_Cleaning',
        python_callable=Data_cleaning
    )
    snowflake_stage_task= PythonOperator(
        task_id='load_stage_table',
        python_callable=load_stage,
        dag=dag
    )

    merge_task = PythonOperator(
        task_id='merge_table',
        python_callable=merge,
        dag=dag
    )

    scraping_task>>csv_to_dataframe_task>>snowflake_stage_task>>merge_task

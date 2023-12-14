import streamlit as st
import pandas as pd
import os
from dotenv import load_dotenv
import s3fs

load_dotenv()

aws_access_key_id = os.getenv("AIRFLOW_VAR_AWS_ACCESS_KEY1")
aws_secret_access_key = os.getenv("AIRFLOW_VAR_AWS_SECRET_KEY1")
s3_bucket_name = os.getenv("AIRFLOW_VAR_S3_BUCKET_NAME1")
s3_file_key = "NetworkAI.csv"


# Set up S3 filesystem
fs_s3 = s3fs.S3FileSystem(
    key=aws_access_key_id,
    secret=aws_secret_access_key
)

# Load data from S3
s3_uri = f"s3://{s3_bucket_name}/{s3_file_key}"
with fs_s3.open(s3_uri, 'rb') as file:
    df = pd.read_csv(file)


st.title("NetworkAI Explorer")
# Create a dropdown with unique values
selected_company = st.selectbox("Select Company Name", df['Company Name'].unique())

# Filter data based on the selected company
filtered_data = df[df['Company Name'] == selected_company]

# Display the filtered data
st.write("Linkedin Profiles:")
st.write(filtered_data)

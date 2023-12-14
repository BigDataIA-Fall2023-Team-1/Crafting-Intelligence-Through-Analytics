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

# Set the maximum number of allowed selections
max_selections = 3

# Create a multiselect with unique values and limit the number of selections
selected_companies = st.multiselect(
    "Select Company Names", df['Company Name'].unique(), default=[], key="multiselect"
)

# Check if the number of selections exceeds the limit
if len(selected_companies) > max_selections:
    st.warning(f"In the free tier, you can only select up to {max_selections} companies.")
    st.write("If you want to select more companies or want more data, you should upgrade the subscription.")
    
    # Display upgrade subscription button
    upgrade_button = st.button("Upgrade Subscription")

    # Handle button click
    if upgrade_button:
        # Redirect to another Streamlit page
        st.experimental_set_query_params(upgrade=True)

# Filter data based on the selected companies
filtered_data = df[df['Company Name'].isin(selected_companies)]

# Display the filtered data for the first 3 selected companies
for company in selected_companies[:max_selections]:
    data_for_company = filtered_data[filtered_data['Company Name'] == company]
    if not data_for_company.empty:
        st.write(f"Linkedin Profiles for {company}:")
        st.write(data_for_company)
    else:
        st.write(f"No data available for {company}.")

# Check if the upgrade parameter is present in the URL
upgrade_requested = st.experimental_get_query_params().get("upgrade", False)

# # Redirect to another page if upgrade is requested
# if upgrade_requested:
#     st.write("Redirecting to Upgrade Page...")
#     # Add code here to redirect to another Streamlit page




# import streamlit as st
# import pandas as pd
# import os
# from dotenv import load_dotenv
# import s3fs

# load_dotenv()

# aws_access_key_id = os.getenv("AIRFLOW_VAR_AWS_ACCESS_KEY1")
# aws_secret_access_key = os.getenv("AIRFLOW_VAR_AWS_SECRET_KEY1")
# s3_bucket_name = os.getenv("AIRFLOW_VAR_S3_BUCKET_NAME1")
# s3_file_key = "NetworkAI.csv"

# # Set up S3 filesystem
# fs_s3 = s3fs.S3FileSystem(
#     key=aws_access_key_id,
#     secret=aws_secret_access_key
# )

# # Load data from S3
# s3_uri = f"s3://{s3_bucket_name}/{s3_file_key}"
# with fs_s3.open(s3_uri, 'rb') as file:
#     df = pd.read_csv(file)

# st.title("NetworkAI Explorer")

# # Set the maximum number of allowed selections
# max_selections = 3

# # Create a multiselect with unique values and limit the number of selections
# selected_companies = st.multiselect(
#     "Select CompanyNames", df['COMPANYNAME'].unique(), default=[], key="multiselect"
# )

# # Check if the number of selections exceeds the limit
# if len(selected_companies) > max_selections:
#     st.warning(f"In the free tier, you can only select up to {max_selections} companies.")
#     st.write("If you want to select more companies or want more data, you should upgrade the subscription.")
    
#     # Display upgrade subscription button
#     upgrade_button = st.button("Upgrade Subscription")

#     # Handle button click
#     if upgrade_button:
#         # Redirect to another Streamlit page
#         st.experimental_set_query_params(upgrade=True)

# # Filter data based on the selected companies
# filtered_data = df[df['COMPANYNAME'].isin(selected_companies)]

# # Display the filtered data for the first 3 selected companies
# for company in selected_companies[:max_selections]:
#     data_for_company = filtered_data[filtered_data['COMPANYNAME'] == company]
#     if not data_for_company.empty:
#         st.write(f"Linkedin Profiles for {company}:")
#         st.write(data_for_company)
#     else:
#         st.write(f"No data available for {company}.")

# # Check if the upgrade parameter is present in the URL
# upgrade_requested = st.experimental_get_query_params().get("upgrade", False)

# # # Redirect to another page if upgrade is requested
# # if upgrade_requested:
# #     st.write("Redirecting to Upgrade Page...")
# #     # Add code here to redirect to another Streamlit page



import os
import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from snowflake.connector import connect, ProgrammingError

# Load environment variables from the .env file
load_dotenv()

# Environment Variables
snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
snowflake_user = os.getenv("SNOWFLAKE_USER")
snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
snowflake_database = os.getenv("SNOWFLAKE_DATABASE")
snowflake_role = os.getenv("SNOWFLAKE_ROLE")
snowflake_schema = os.getenv("SNOWFLAKE_SCHEMA1")
snowflake_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
snowflake_table = os.getenv("SNOWFLAKE_TABLE")

# Function to establish a connection to Snowflake
def get_snowflake_connection():
    return connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema,
        role=snowflake_role
    )

# Create a connection
conn = get_snowflake_connection()

# Fetch data from Snowflake table
try:
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {snowflake_database}.{snowflake_table}.{snowflake_schema}")
        data = cursor.fetchall()
except ProgrammingError as e:
    st.error(f"Error executing Snowflake query: {e}")
    data = None

# Check if data is available
if data:
    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(data, columns=[col[0] for col in cursor.description])

    st.title("Network Explorer")

    # Set the maximum number of allowed selections
    max_selections = 3

    # Create a multiselect with unique values and limit the number of selections
    selected_companies = st.multiselect(
        "Select CompanyNames", df['COMPANYNAME'].unique(), default=[], key="multiselect"
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
    filtered_data = df[df['COMPANYNAME'].isin(selected_companies)]

    # Display the filtered data for the first 3 selected companies
    for company in selected_companies[:max_selections]:
        data_for_company = filtered_data[filtered_data['COMPANYNAME'] == company]
        if not data_for_company.empty:
            st.write(f"Linkedin Profiles for {company}:")
            st.write(data_for_company)
        else:
            st.write(f"No data available for {company}.")
else:
    st.warning("No data available from Snowflake.")




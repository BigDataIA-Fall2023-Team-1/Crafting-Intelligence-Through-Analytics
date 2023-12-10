import streamlit as st
import pandas as pd
import plotly.express as px
import os
import requests
from snowflake.connector import connect
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

host_ip_address = os.getenv("HOST_IP_ADDRESS")

# Environment Variables
snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
snowflake_user = os.getenv("SNOWFLAKE_USER")
snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
snowflake_database = os.getenv("SNOWFLAKE_DATABASE")
snowflake_role = os.getenv("SNOWFLAKE_ROLE")

# Function to establish a connection to Snowflake
def get_snowflake_connection():
    return connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        database=snowflake_database,
        role=snowflake_role
    )

# Create a connection
conn = get_snowflake_connection()

# Streamlit app
@st.cache_data
def main():
    st.title("Streamlit Analytics Dashboard")

    # Sample Graph 1: Job Titles Distribution
    query_job_titles = """
    SELECT
        JOB_TITLE,
        COUNT(*) AS JOB_COUNT
    FROM
        JOBS.TEST
    GROUP BY
        JOB_TITLE;
    """
    result_job_titles = execute_query(conn, query_job_titles)
    df_job_titles = pd.DataFrame(result_job_titles, columns=['Job Title', 'Job Count'])
    st.title("Job Titles Distribution")
    fig_job_titles = px.bar(df_job_titles, x='Job Title', y='Job Count')
    st.plotly_chart(fig_job_titles, use_container_width=True)

    # Sample Graph 2: Locations Distribution
    query_locations = """
    SELECT
        LOCATION,
        COUNT(*) AS LOCATION_COUNT
    FROM
        JOBS.TEST
    GROUP BY
        LOCATION;
    """
    result_locations = execute_query(conn, query_locations)
    df_locations = pd.DataFrame(result_locations, columns=['Location', 'Location Count'])
    st.title("Locations Distribution")
    fig_locations = px.bar(df_locations, x='Location', y='Location Count')
    st.plotly_chart(fig_locations, use_container_width=True)

    # Sample Graph 3: Job Posting Over Time
    query_job_posting_over_time = """
    SELECT
        POSTED_ON,
        COUNT(*) AS POST_COUNT
    FROM
        JOBS.TEST
    GROUP BY
        POSTED_ON;
    """
    result_job_posting_over_time = execute_query(conn, query_job_posting_over_time)
    df_job_posting_over_time = pd.DataFrame(result_job_posting_over_time, columns=['Posted On', 'Post Count'])
    df_job_posting_over_time['Posted On'] = pd.to_datetime(df_job_posting_over_time['Posted On'])
    st.title("Job Posting Over Time")
    fig_job_posting_over_time = px.line(df_job_posting_over_time, x='Posted On', y='Post Count', markers=True)
    st.plotly_chart(fig_job_posting_over_time, use_container_width=True)

    # Sample Graph 4: State-wise Job Distribution
    query_state_job_distribution = """
    SELECT
        STATE,
        COUNT(*) AS JOB_COUNT
    FROM
        JOBS.TEST
    GROUP BY
        STATE;
    """
    result_state_job_distribution = execute_query(conn, query_state_job_distribution)
    df_state_job_distribution = pd.DataFrame(result_state_job_distribution, columns=['State', 'Job Count'])
    st.title("State-wise Job Distribution")
    fig_state_job_distribution = px.bar(df_state_job_distribution, x='State', y='Job Count')
    st.plotly_chart(fig_state_job_distribution, use_container_width=True)

# Function to execute Snowflake queries
def execute_query(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
    return result

# If the user is authenticated, they can access protected data
if "access_token" in st.session_state:
    access_token = st.session_state.access_token
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(f"http://{host_ip_address}:8000/protected", headers=headers)
    if response.status_code == 200:
        authenticated_user = response.json()
        main() 
else:
    st.text("Please login/register to access the Application.")
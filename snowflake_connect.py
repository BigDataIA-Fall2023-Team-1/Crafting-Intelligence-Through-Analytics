import os
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from snowflake.connector import connect

# Load environment variables from the .env file
load_dotenv()

# Environment Variables
snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
snowflake_user = os.getenv("SNOWFLAKE_USER")
snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
snowflake_database = os.getenv("SNOWFLAKE_DATABASE")
snowflake_table = os.getenv("SNOWFLAKE_TABLE")
snowflake_role = os.getenv("SNOWFLAKE_ROLE")

# Function to establish a connection to Snowflake
def get_snowflake_connection():
    return connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        database=snowflake_database,
        role=snowflake_role,
        table=snowflake_table
    )

def snow_connect(query):
    # Create a connection
    conn = get_snowflake_connection()

    # Create a cursor
    cursor = conn.cursor()

    # Define a SQL query
    sql = f"""
    {query};
    """
    # Execute the query and convert the result to a Pandas dataframe
    cursor.execute(sql)
    df = cursor.fetch_pandas_all()

    # Add Streamlit features to your app to display the results of your query
    st.dataframe(df)

    # Close the cursor and connection
    cursor.close()
    conn.close()
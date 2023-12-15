import os
import streamlit as st
import pandas as pd
import requests
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
snowflake_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
snowflake_table = "NETWORKAI"
host_ip_address = os.getenv("HOST_IP_ADDRESS")

# Function to establish a connection to Snowflake
def get_snowflake_connection():
    return connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        role=snowflake_role
    )

# Create a connection
conn = get_snowflake_connection()

# Fetch data from Snowflake table
try:
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {snowflake_table}")
        data = cursor.fetchall()
except ProgrammingError as e:
    st.error(f"Error executing Snowflake query: {e}")
    data = None

# If the user is authenticated, they can access protected data
if "access_token" in st.session_state:
    access_token = st.session_state.access_token
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(f"http://{host_ip_address}:8000/protected", headers=headers)
    if response.status_code == 200:
        authenticated_user = response.json()

    # Check if data is available
    if data:
        # Convert the data to a pandas DataFrame
        df = pd.DataFrame(data, columns=[col[0] for col in cursor.description])

        st.title("Network Explorer")

        # Set the maximum number of allowed selections
        max_selections = 3

        # Create a multiselect with unique values and limit the number of selections
        selected_companies = st.multiselect(
            "Select Company Names To Increase Your Potential Network", df['COMPANYNAME'].unique(), default=[], key="multiselect"
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

else:
    st.text("Please login/register to access the Application.")
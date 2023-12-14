import os
import re
import warnings
import logging
import PyPDF2
import requests
from io import BytesIO
import streamlit as st
from dotenv import load_dotenv
from chain import load_chain
from snowflake_connect import snow_connect, get_snowflake_connection
from ui.streamlit_ui import StreamlitUICallbackHandler, message_func
from snowflake.snowpark.exceptions import SnowparkSQLException

warnings.filterwarnings("ignore")
chat_history = []

# Load environment variables from the .env file
load_dotenv()

# Log metrics
logging.basicConfig(level=logging.INFO)

host_ip_address = os.getenv("HOST_IP_ADDRESS")


if "access_token" in st.session_state:
    access_token = st.session_state.access_token
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(f"http://{host_ip_address}:8000/protected", headers=headers)
    if response.status_code == 200:
        authenticated_user = response.json()
        st.title("OpenAI Chat")

        # Display initial image and text in the center
        st.image("image.jpg", width=75, use_column_width=False, output_format="auto")
        st.write("How can I help you today?") 

        INITIAL_MESSAGE = [
            {"role": "user", "content": "Hi!"},
            {
                "role": "assistant",
                "content": "Hey there! I'm here to help you today and assist you with job search❄️🔍",
            },
        ]

        with open("ui/styles.md", "r") as styles_file:
            styles_content = styles_file.read()

        # Add a reset button
        if "messages" in st.session_state.keys() and st.session_state["messages"] != INITIAL_MESSAGE:
            if st.sidebar.button("Reset Chat"):
                st.session_state["messages"] = INITIAL_MESSAGE
                st.session_state["history"] = []

        st.write(styles_content, unsafe_allow_html=True)

        # Initialize the chat messages history
        if "messages" not in st.session_state.keys():
            st.session_state["messages"] = INITIAL_MESSAGE

        if "history" not in st.session_state:
            st.session_state["history"] = []

        if "model" not in st.session_state:
            st.session_state["model"] = "GPT-3.5"

        # Prompt for user input and save
        if prompt := st.chat_input():
            st.session_state.messages.append({"role": "user", "content": prompt})

        for message in st.session_state.messages:
            message_func(
                message["content"],
                True if message["role"] == "user" else False,
                True if message["role"] == "data" else False,
            )

        callback_handler = StreamlitUICallbackHandler()

        chain = load_chain(st.session_state["model"], callback_handler)

        def append_chat_history(question, answer):
            st.session_state["history"].append((question, answer))

        def get_sql(text):
            sql_match = re.search(r"```sql\n(.*)\n```", text, re.DOTALL)
            return sql_match.group(1) if sql_match else None

        def append_message(content, role="assistant", display=False):
            message = {"role": role, "content": content}
            st.session_state.messages.append(message)
            if role != "data":
                append_chat_history(st.session_state.messages[-2]["content"], content)

            if callback_handler.has_streaming_ended:
                callback_handler.has_streaming_ended = False
                return

        def handle_sql_exception(query, conn, e, retries=2):
            append_message("Uh oh, I made an error, let me try to fix it..")
            error_message = (
                "You gave me a wrong SQL. FIX The SQL query by searching the schema definition:  \n```sql\n"
                + query
                + "\n```\n Error message: \n "
                + str(e)
            )
            new_query = chain({"question": error_message, "chat_history": ""})["answer"]
            append_message(new_query)
            if get_sql(new_query) and retries > 0:
                return execute_sql(get_sql(new_query), conn, retries - 1)
            else:
                append_message("I'm sorry, I couldn't fix the error. Please try again.")
                return None

        def execute_sql(query, conn, retries=2):
            if re.match(r"^\s*(drop|alter|truncate|delete|insert|update)\s", query, re.I):
                append_message("Sorry, I can't execute queries that can modify the database.")
                return None
            try:
                # Create a cursor
                cursor = conn.cursor()

                # Execute the query and fetch the result
                cursor.execute(query)
                result = cursor.fetch_pandas_all()
                
                # Close the cursor
                cursor.close()

                return st.dataframe(result)
            except SnowparkSQLException as e:
                return handle_sql_exception(query, conn, e, retries)
            
        def read_pdf(file):
            # Read the content of the uploaded PDF file
            pdf_bytes = uploaded_file.read()

            # Use BytesIO to create a "file-like" object
            pdf_file = BytesIO(pdf_bytes)

            # Read the PDF content using PyPDF2
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            text = ""
            for page_num in range(len(pdf_reader.pages)):
                text += pdf_reader.pages[page_num].extract_text()

            return text

        # Display PDF content
        uploaded_file = st.sidebar.file_uploader("Upload a PDF file", type=["pdf"])
        if uploaded_file is not None:
            st.sidebar.success("File successfully uploaded!")

            # Check if PDF content is already in session state
            if "pdf_content" not in st.session_state.keys():
                st.session_state.pdf_content = ""

            # Read and store the PDF content
            pdf_text = read_pdf(uploaded_file)
            st.session_state.pdf_content = pdf_text
            
            # Display PDF content from session state
            st.subheader("PDF Content:")
            st.text(st.session_state.pdf_content)
            
        if st.session_state.messages[-1]["role"] != "assistant":
            content = st.session_state.messages[-1]["content"]
            if isinstance(content, str):
                result_dict = chain({"question": content, "chat_history": st.session_state["history"]})
                result = result_dict.get("text")  
                append_message(result)
                if get_sql(result):
                    conn = get_snowflake_connection()
                    df = execute_sql(get_sql(result), conn)
                    if df is not None:
                        callback_handler.display_dataframe(df)
                        append_message(df, "data", True)
            else:
                st.write("Error: Unable to retrieve an sql_query.")

else:
    st.text("Please login/register to access the Application.") 
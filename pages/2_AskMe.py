import os
import openai
import requests
import streamlit as st
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

host_ip_address = os.getenv("HOST_IP_ADDRESS")

def question_answer(access_token):

    st.title("OpenAI Chat")

    # Display initial image and text in the center
    st.image("image.jpg", width=75, use_column_width=False, output_format="auto")
    st.write("How can I help you today?") 

    # Get user question
    question = st.chat_input("Your Question")

    if question:
        headers = {"Authorization": f"Bearer {access_token}"}
        data = {"question": question}
        response = requests.post(f"http://{host_ip_address}:8000/process_question", json=data, headers=headers)

        if response.status_code == 200:
            answer = response.json().get("answer")
            st.write(f"Answer: {answer}")
        else:
            st.write("Error: Unable to retrieve an answer.")
    
# If the user is authenticated, they can access protected data
if "access_token" in st.session_state:
    access_token = st.session_state.access_token
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(f"http://{host_ip_address}:8000/protected", headers=headers)
    if response.status_code == 200:
        authenticated_user = response.json()
        question_answer(access_token)
else:
    st.text("Please login/register to access the Application.")
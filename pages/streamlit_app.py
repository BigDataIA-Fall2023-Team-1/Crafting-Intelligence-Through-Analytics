# streamlit_app.py
import streamlit as st
import requests

# Streamlit UI for payment form
amount = st.number_input("Enter the amount:")
token = st.text_input("Enter the card token:")

if st.button("Submit Payment"):
    # Make an HTTP request to the FastAPI server
    try:
        response = requests.post("http://localhost:8000/charge", data={"amount": amount, "token": token})
        response_data = response.json()

        # Handle the response
        if response.status_code == 200:
            st.success("Payment successful! Charge ID: {}".format(response_data["charge_id"]))
        else:
            st.error("Payment failed. Error: {}".format(response_data["detail"]))
    except requests.RequestException as e:
        st.error(f"Error connecting to server: {str(e)}")

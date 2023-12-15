import streamlit as st
import stripe
from dotenv import load_dotenv
import requests
import os

load_dotenv()

# Set your Stripe test key
stripe.api_key = os.getenv('STRIPE_API_KEY')
host_ip_address = os.getenv("HOST_IP_ADDRESS")

# If the user is authenticated, they can access protected data
if "access_token" in st.session_state:
    access_token = st.session_state.access_token
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(f"http://{host_ip_address}:8000/protected", headers=headers)
    if response.status_code == 200:
        authenticated_user = response.json()

    # Streamlit UI for subscription and payment form
    st.title("PAYMENT GATEWAY")

    # User inputs
    subscription_options = ["Gold", "Platinum"]
    selected_subscription = st.selectbox("Select Subscription:", subscription_options)

    if selected_subscription == "Gold":
        default_amount = 10
    elif selected_subscription == "Platinum":
        default_amount = 15
    else:
        default_amount = 0

    st.write(f"Selected Subscription: {selected_subscription}")
    st.write(f"Amount: ${default_amount}")

    # Payment details
    st.subheader("Payment Details")

    # Credit card number input with formatting
    card_number = st.text_input("Card Number", type="password", key="card_number")

    # Format the card number into groups of four digits
    formatted_card_number = " ".join([card_number[i:i+4] for i in range(0, len(card_number), 4)])
    st.text(f"Formatted Card Number: {formatted_card_number}")

    expiration_date = st.date_input("Expiration Date")
    cvc = st.text_input("CVC/CVV")

    # Submit button
    if st.button("Submit Payment"):
        try:
            # Create a Payment Intent with success_url including {CHECKOUT_SESSION_ID}
            payment_intent = stripe.PaymentIntent.create(
                amount=default_amount * 100,  # Amount in cents
                currency="usd",
                payment_method="pm_card_visa",  # Test card number for Visa
                confirmation_method="manual",
                confirm=True,
                success_url=f"http://yoursite.com/order/success?session_id={{CHECKOUT_SESSION_ID}}",  # Replace with your actual success URL
            )

            # Display success message
            st.success("Payment successful!")
            st.markdown(f"Please complete your payment [here]({payment_intent.charges.data[0].payment_intent})")

        except stripe.error.CardError as e:
            st.error(f"Card error: {e.error.message}")
        except stripe.error.StripeError as e:
            st.error(f"Stripe error: {e.error.message}")
else:
    st.text("Please login/register to access the Application.")

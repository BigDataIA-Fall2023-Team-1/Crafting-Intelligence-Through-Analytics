import streamlit as st
import stripe
from dotenv import load_dotenv
import os

load_dotenv()

# Set your Stripe test key
stripe.api_key = os.getenv('STRIPE_API_KEY')

# if "access_token" in st.session_state:
#     access_token = st.session_state.access_token
#     headers = {"Authorization": f"Bearer {access_token}"}
#     response = requests.get(f"http://{host_ip_address}:8000/protected", headers=headers)
#     if response.status_code == 200:
#         authenticated_user = response.json()

# Streamlit UI for subscription and payment form
subscription_options = ["Gold", "Platinum"]
selected_subscription = st.radio("Select Subscription:", subscription_options)

# Set default amount based on the selected subscription
if selected_subscription == "Gold":
    default_amount = 10
elif selected_subscription == "Platinum":
    default_amount = 15
else:
    default_amount = 0

# Display the default amount as read-only text
st.text(f"Amount: ${default_amount}")

card_number = st.text_input("Enter the card number:")
# username = st.text_input("Enter your username:")
# password = st.text_input("Enter your password:", type="password")

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

        # Redirect to the Checkout Session's URL
        st.markdown(f"Please complete your payment [here]({payment_intent.charges.data[0].payment_intent})")

    except stripe.error.CardError as e:
        st.error(f"Card error: {e.error.message}")
    except stripe.error.StripeError as e:
        st.error(f"Stripe error: {e.error.message}")

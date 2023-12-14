# stripe.py
from fastapi import FastAPI, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
import stripe
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# Enable CORS to allow communication with Streamlit frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Set your Stripe secret key
stripe.api_key = os.getenv("STRIPE_API_KEY")

@app.post("/charge")
async def charge(amount: int = Form(...), token: str = Form(...)):
    try:
        # Create a charge using the Stripe API
        charge = stripe.Charge.create(
            amount=amount,
            currency="usd",
            source=token,
            description="Example Charge"
        )

        # Return success response
        return {"status": "success", "charge_id": charge.id}
    except stripe.error.CardError as e:
        # Return error response for card errors
        raise HTTPException(status_code=400, detail=str(e))

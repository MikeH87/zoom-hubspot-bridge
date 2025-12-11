import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

HUBSPOT_BASE_URL = os.getenv("HUBSPOT_BASE_URL", "https://api.hubapi.com")
HUBSPOT_PRIVATE_APP_TOKEN = os.getenv("HUBSPOT_PRIVATE_APP_TOKEN")
ZOOM_WEBHOOK_SECRET_TOKEN = os.getenv("ZOOM_WEBHOOK_SECRET_TOKEN")


if not HUBSPOT_PRIVATE_APP_TOKEN:
    raise RuntimeError("HUBSPOT_PRIVATE_APP_TOKEN is not set. Please check your .env file.")

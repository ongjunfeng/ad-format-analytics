from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()

AWS_CONFIG = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "region_name": os.getenv("AWS_REGION", "ap-southeast-2"),
}

INSTAGRAM_USERNAME = os.getenv("INSTAGRAM_USERNAME")

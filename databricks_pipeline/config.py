"""
Configuration management for analytics scraping.
"""
import logging
import os
import json
from dataclasses import dataclass
from typing import Dict, Any
from dotenv import load_dotenv

# Set up logger
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Apify Configuration
@dataclass
class ApifyConfig:
    """Configuration for scraping parameters used with ApifyService"""
    actor_name: str                     # Apify actor name e.g. "trudax/reddit-scraper-lite"
    config_name: str                    # Unique name for this config e.g. "reddit_catmoms"
    platform: str                       # Platform name e.g. "reddit", "tiktok"
    input_parameters: Dict[str, Any]    # Input parameters for the actor, refer to actor docs

# Input parameters of Apify Config stored as JSON. Refer to actor documentation
REEL_SCRAPER_CONFIG_FILE = "reel_scraper_config_small.json" #TODO: Change back once done testing
with open(REEL_SCRAPER_CONFIG_FILE, "r") as file:
    reel_scraper_config = json.load(file)

# Instagram reel scraping config setup
IG_REEL_SCRAPING_CONFIG = ApifyConfig(
    actor_name="apify/instagram-reel-scraper",
    config_name="instagram_reels",
    platform="instagram",
    input_parameters=reel_scraper_config
)

# Instagram account configuration for Instaloader fallback.
INSTAGRAM_USERNAME = os.getenv("INSTAGRAM_USERNAME")
INSTAGRAM_PASSWORD = os.getenv("INSTAGRAM_PASSWORD")
if not (INSTAGRAM_PASSWORD and INSTAGRAM_USERNAME):
    logger.warning("Warning: Instagram username/password not configured. No fallback for media_url via Instaloader.")

# S3 Credentials
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BASE_PATH = os.getenv("S3_BASE_PATH")
S3_BUCKET_PATH = os.getenv("S3_BUCKET_PATH")
if not (AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY or S3_BASE_PATH or S3_BUCKET_PATH):
    logger.error("ERROR: S3 credentials not configured. No media can be uploaded to S3.")
    raise Exception("S3 credentials not configured. Set up credentials in .env file.")


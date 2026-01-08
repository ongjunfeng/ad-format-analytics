"""
Configuration management for analytics scraping.
"""
import os
import json
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

@dataclass
class ScrapingConfig:
    """Configuration for scraping parameters."""
    actor_name: str                     # Apify actor name e.g. "trudax/reddit-scraper-lite"
    config_name: str                    # Unique name for this config e.g. "reddit_catmoms"
    platform: str                       # Platform name e.g. "reddit", "tiktok"
    input_parameters: Dict[str, Any]    # Input parameters for the actor, refer to actor docs

# Base directory for all datasets. The directory for saved datasets is in the parent folder `data_pipeline`
BASE_DATA_DIR = Path("..") / os.getenv("DEFAULT_DATA_DIR", "datasets")

# Get the directory where config.py resides
CONFIG_DIR = Path(__file__).parent

# Construct the absolute path to the JSON file
JSON_PATH = CONFIG_DIR / "reel_scraper_config.json"

with open(JSON_PATH, "r") as file:
    reel_scraper_config = json.load(file)

# Instagram reel scraping config setup
IG_REEL_SCRAPING_CONFIG = ScrapingConfig(
    actor_name="apify/instagram-reel-scraper",
    config_name="instagram_reels",
    platform="instagram",
    input_parameters=reel_scraper_config
)

@dataclass
class DatabricksConfig:
    """Configuration for Databricks connection details."""
    host: str
    token: str
    catalog: str
    schema: str
    
    @property
    def url(self) -> str:
        """Constructs the Databricks SQL endpoint URL."""
        return f"https://{self.host}/sql/1.0/endpoints"
    
DATABRICKS_CONFIG = DatabricksConfig(
    host=os.getenv("DATABRICKS_HOST"),
    token=os.getenv("DATABRICKS_TOKEN"),
    catalog=os.getenv("DATABRICKS_CATALOG", "main"), # Use a default if not set
    schema=os.getenv("DATABRICKS_SCHEMA", "viral_analytics") # Use a default if not set
)

# Sanity Check for Databricks
if not all([DATABRICKS_CONFIG.host, DATABRICKS_CONFIG.token]):
    print("WARNING: Databricks host or token is not set in environment variables.")

# --- AWS Configuration (Placeholder - assumed to exist for S3Service) ---
AWS_CONFIG = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "region_name": os.getenv("AWS_REGION")
}

# --- Instagram Configuration ---
INSTAGRAM_USERNAME = os.getenv("INSTAGRAM_USERNAME")

if not INSTAGRAM_USERNAME:
    print("WARNING: INSTAGRAM_USERNAME environment variable is not set.")

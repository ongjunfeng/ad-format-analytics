"""A module for working with the Apify API.

This module provides classes and methods for interacting with
the Apify API to simplify the process of scraping data with a predefined config.

Classes:
    ApifyService: Interacts with Apify API for scraping and retrieving data
    ApifyResponse: Holds Apify's response and scraping metadata
"""
import os
import time
import logging
import pandas as pd
from datetime import datetime
from apify_client import ApifyClient
from config import ApifyConfig

# Setup logging
logger = logging.getLogger(__name__)

class ApifyService:
    """
    Service class for obtaining data through Apify API
    given the proper API token and predefined configs or dataset_id.
    """
    def __init__(self):
        """
        Initialize the ApifyService with an ApifyClient instance.
        Requires the "APIFY_API_TOKEN environment variable to be set.
        """
        API_TOKEN = os.getenv("APIFY_API_TOKEN")
        if not API_TOKEN:
            raise ValueError("APIFY_API_TOKEN environment variable not set.")
        self.apify_client = ApifyClient(API_TOKEN)

    def get_config_df(self, config: ApifyConfig, wait_secs=300) -> pd.DataFrame:
        """Retrieves a DataFrame by starting a scraping operation with a ApifyConfig

        Args:
        - config (ApifyConfig): a defined ApifyConfig
        - wait_secs (int): the maximum wait time for the scrape job in seconds
        Returns: pandas DataFrame object for the scrape job
        """
        # Run the actor with provided scraping config
        try:
            start_time = time.time()
            logger.info("Starting Apify scraper with defined ApifyConfig...")
            run_response = self.apify_client.actor(config.actor_name).call(
                run_input=config.input_parameters,
                wait_secs=wait_secs,  # Wait up to 5 minutes by default
                logger=logger
            )
            execution_time = time.time() - start_time
            dataset_id = run_response['defaultDatasetId']
            logger.info(f"✅ Scraper finished in {execution_time:.2f}s.")
            logger.info(f"Scraper returned dataset_id: {dataset_id}")

        except Exception as e:
            logger.error(f"❌ Scraper execution failed: {str(e)}")
            raise

        # Fetch dataset via returned dataset_id
        return self.get_dataset_id_df(dataset_id=dataset_id)

    def get_dataset_id_df(self, dataset_id: str) -> pd.DataFrame:
        """Retrieves a DataFrame from a previous scrape run's dataset_id

        Args:
        - dataset_id (str): The dataset_id returned from a past scrape
        Returns: pandas DataFrame object for the dataset_id
        """
        logger.info(f"Attempting to fetch dataset_id: {dataset_id}")
        try:
            dataset_items = self.apify_client.dataset(dataset_id).list_items()
            logger.info(f"✅ Fetched {dataset_items.count} items from dataset_id {dataset_id}")
        except Exception as e:
            logger.error(f"❌ Failed to fetch dataset items: {str(e)}")
            raise

        data = dataset_items.items
        if not data:
            logger.warning("⚠️ No data to convert to DataFrame.")
            return pd.DataFrame()
        
        try:
            df = pd.json_normalize(data)
            logger.info(f"✅ Converted {len(data)} items to DataFrame with {df.shape[1]} columns.")
            df["dataset_id"] = dataset_id
            df["ingested_epoch"] = time.time()
            df["ingested_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return df
        except Exception as e:
            logger.error(f"❌ Failed to convert data to DataFrame: {str(e)}")
            raise
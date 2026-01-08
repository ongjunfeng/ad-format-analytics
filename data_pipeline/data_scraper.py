"""
Apify API client for data scraping.
"""
import time
import os
from typing import Optional
from datetime import datetime
import logging
from apify_client import ApifyClient

from config import ScrapingConfig
from scraping_result import ScrapingResult

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataScraper:
    """Abstraction for ingesting data using Apify API and predefined configs."""
    def __init__(self):
        self.apify_client = ApifyClient(os.getenv("APIFY_API_TOKEN"))

    def run_scraping_config(self, config: ScrapingConfig) -> ScrapingResult:
        """Perform the scraping operation."""
        # Run the actor with provided scraping config
        try:
            start_time = time.time()
            run_response = self.apify_client.actor(config.actor_name).call(
                run_input=config.input_parameters,
                wait_secs=5*60,  # Wait up to 5 minutes
                logger=logger
            )
            execution_time = time.time() - start_time
            dataset_id = run_response['defaultDatasetId']
            logger.info(f"✅ Scraper finished in {execution_time:.2f}s, fetching dataset...")
            return self.fetch_dataset_items(dataset_id, config, execution_time)

        except Exception as e:
            logger.error(f"❌ Scraper execution failed: {str(e)}")
            raise

    def fetch_dataset_items(self, dataset_id: str, config: Optional[ScrapingConfig] = None,
                            execution_time: float = 0.0) -> ScrapingResult:
        # Fetch the dataset items from run
        try:
            dataset_items = self.apify_client.dataset(dataset_id).list_items()
            logger.info(f"✅ Fetched {dataset_items.count} items from dataset.")

            return ScrapingResult(
                data=dataset_items.items,
                dataset_id=dataset_id,
                total_items=dataset_items.count,
                execution_time=execution_time,
                config=config,
                timestamp=datetime.now()
            )
        
        except Exception as e:
            logger.error(f"❌ Failed to fetch dataset items: {str(e)}")
            raise
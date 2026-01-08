"""
Apify API client for TikTok data scraping.
"""
import requests
import json
import time
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import logging

from config import APIConfig, ScrapingConfig

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ScrapingResult:
    """Container for scraping results and metadata."""
    data: List[Dict[str, Any]]
    total_items: int
    execution_time: float
    tier: str
    timestamp: datetime
    
class ApifyClient:
    """Client for interacting with Apify API."""
    
    def __init__(self):
        self.config = APIConfig()
        self.session = requests.Session()
        self.session.headers.update(self.config.headers)
        
    def run_sync_scraper(self, 
                        scraping_config: ScrapingConfig,
                        timeout: int = 300,
                        max_items: Optional[int] = None) -> Dict[str, Any]:
        """
        Run TikTok scraper synchronously and return dataset items.
        
        Args:
            scraping_config: Configuration for the scraping job
            timeout: Timeout in seconds (max 300 for sync endpoint)
            max_items: Maximum number of items to return
            
        Returns:
            Dictionary containing scraped data and metadata
        """
        url = f"{self.config.base_url}/acts/{self.config.tiktok_actor_id}/run-sync-get-dataset-items"
        
        # Prepare query parameters
        params = {
            "timeout": min(timeout, 300),  # API limit
            "format": "json",
            "clean": True,  # Skip empty items and hidden fields
        }
        
        if max_items:
            params["maxItems"] = max_items
            
        # Prepare request body
        payload = scraping_config.to_apify_input()
        
        logger.info(f"Starting TikTok scraper with config: {json.dumps(payload, indent=2)}")
        
        try:
            start_time = time.time()
            response = self.session.post(url, json=payload, params=params)
            execution_time = time.time() - start_time
            
            response.raise_for_status()
            data = response.json()
            
            logger.info(f"Scraping completed in {execution_time:.2f}s. Retrieved {len(data)} items.")
            
            return {
                "data": data,
                "total_items": len(data),
                "execution_time": execution_time,
                "timestamp": datetime.now().isoformat(),
                "success": True
            }
            
        except requests.exceptions.Timeout:
            logger.error(f"Request timed out after {timeout} seconds")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise
            
    def run_async_scraper(self, 
                         scraping_config: ScrapingConfig,
                         timeout: Optional[int] = None,
                         memory: Optional[int] = None) -> str:
        """
        Run TikTok scraper asynchronously and return run ID.
        
        Args:
            scraping_config: Configuration for the scraping job
            timeout: Timeout in seconds
            memory: Memory limit in MB
            
        Returns:
            Run ID for tracking the asynchronous job
        """
        url = f"{self.config.base_url}/acts/{self.config.tiktok_actor_id}/runs"
        
        params = {}
        if timeout:
            params["timeout"] = timeout
        if memory:
            params["memory"] = memory
            
        payload = scraping_config.to_apify_input()
        
        logger.info(f"Starting async TikTok scraper with config: {json.dumps(payload, indent=2)}")
        
        try:
            response = self.session.post(url, json=payload, params=params)
            response.raise_for_status()
            
            run_data = response.json()
            run_id = run_data['data']['id']
            
            logger.info(f"Async scraper started with run ID: {run_id}")
            return run_id
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to start async scraper: {str(e)}")
            raise
            
    def get_run_status(self, run_id: str) -> Dict[str, Any]:
        """Get status of an asynchronous run."""
        url = f"{self.config.base_url}/actor-runs/{run_id}"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get run status: {str(e)}")
            raise
            
    def get_run_dataset(self, run_id: str, 
                       format: str = "json",
                       clean: bool = True,
                       limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get dataset items from a completed run."""
        # First get the run info to find dataset ID
        run_info = self.get_run_status(run_id)
        dataset_id = run_info['data']['defaultDatasetId']
        
        url = f"{self.config.base_url}/datasets/{dataset_id}/items"
        
        params = {
            "format": format,
            "clean": clean
        }
        
        if limit:
            params["limit"] = limit
            
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get dataset: {str(e)}")
            raise

    def scrape_tier_data(self, tier: str, max_items: Optional[int] = None) -> ScrapingResult:
        """
        Scrape data for a specific tier.
        
        Args:
            tier: Tier name (tier1, tier2, tier3)
            max_items: Maximum items to retrieve
            
        Returns:
            ScrapingResult containing data and metadata
        """
        from config import TIER_CONFIGS
        
        if tier not in TIER_CONFIGS:
            raise ValueError(f"Invalid tier: {tier}. Available tiers: {list(TIER_CONFIGS.keys())}")
            
        config = TIER_CONFIGS[tier]
        
        logger.info(f"Starting {tier} data collection...")
        
        try:
            result = self.run_sync_scraper(config, max_items=max_items)
            
            return ScrapingResult(
                data=result["data"],
                total_items=result["total_items"],
                execution_time=result["execution_time"],
                tier=tier,
                timestamp=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"Failed to scrape {tier} data: {str(e)}")
            raise
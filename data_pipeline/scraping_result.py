"""
Apify API client for data scraping.
"""
import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import logging
import pandas as pd

from config import ScrapingConfig, BASE_DATA_DIR

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ScrapingResult:
    """Container for scraping results and metadata."""
    data: List[Dict[str, Any]]
    dataset_id: str
    total_items: int
    execution_time: float
    config: Optional[ScrapingConfig]
    timestamp: datetime

    def _ensure_data_directory(self) -> Path:
        """Ensure data directory exists and return path."""
        # Ensure base data directory exists
        data_dir = BASE_DATA_DIR
        data_dir.mkdir(exist_ok=True)
        return data_dir
    
    def to_pandas(self) -> pd.DataFrame:
        """Convert scraped data to pandas DataFrame."""
        if not self.data:
            logger.warning("⚠️ No data to convert to DataFrame.")
            return pd.DataFrame()
        
        try:
            df = pd.json_normalize(self.data)
            logger.info(f"✅ Converted {len(self.data)} items to DataFrame with {df.shape[1]} columns.")
            return df
        except Exception as e:
            logger.error(f"❌ Failed to convert data to DataFrame: {str(e)}")
            raise

    def save_data(self) -> Dict[str, Path]:
        """
        Save scraped data in multiple formats.
            
        Returns:
            Dictionary with format -> file path mappings
        """
        # If no config provided, set default filename to dataset id and timestamp
        timestamp = self.timestamp.strftime('%Y%m%d_%H%M%S')
        filename = f"{self.dataset_id}_{timestamp}"
        # If config provided, set filename to config name and timestamp
        if self.config:
            config = self.config
            filename = f"{config.config_name}_{timestamp}"
        data_dir = self._ensure_data_directory()
        
        # Generate filenames
        json_file = data_dir / f"{filename}.json"
        csv_file = data_dir / f"{filename}.csv"
        metadata_file = data_dir / f"{filename}_metadata.json"
        
        saved_files = {}
        
        try:
            # Save raw JSON data
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, indent=2, ensure_ascii=False)
            saved_files['json'] = json_file
            logger.info(f"💾 Saved JSON data: {json_file}")
            
            # Save as CSV if data exists
            if self.data:
                df = self.to_pandas()
                df.to_csv(csv_file, index=False, encoding='utf-8')
                saved_files['csv'] = csv_file
                logger.info(f"💾 Saved CSV data: {csv_file}")
            
            # Save metadata
            metadata = {
                "dataset_id": self.dataset_id,
                "total_items": self.total_items,
                "execution_time": self.execution_time,
                "config": self.config.input_parameters if self.config else None,
                "timestamp": self.timestamp.isoformat()                
            }
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            saved_files['metadata'] = metadata_file
            logger.info(f"💾 Saved metadata: {metadata_file}")
            
            return saved_files
            
        except Exception as e:
            logger.error(f"❌ Failed to save scraped data: {str(e)}")
            raise

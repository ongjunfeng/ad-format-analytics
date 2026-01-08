"""
Configuration management for TikTok analytics scraping.
"""
import os
from dataclasses import dataclass
from typing import List, Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

@dataclass
class ScrapingConfig:
    """Configuration for TikTok scraping parameters."""
    search_queries: List[str]
    hashtags: List[str]
    results_per_page: int = 200
    should_download_videos: bool = False
    should_download_covers: bool = False
    should_download_subtitles: bool = False
    should_download_slideshow_images: bool = False
    should_download_avatars: bool = False
    should_download_music_covers: bool = False
    search_section: str = "/video"
    proxy_country_code: str = "US"

    def to_apify_input(self) -> Dict[str, Any]:
        """Convert config to Apify API input format."""
        return {
            "searchQueries": self.search_queries,
            "hashtags": self.hashtags,
            "resultsPerPage": self.results_per_page,
            "shouldDownloadVideos": self.should_download_videos,
            "shouldDownloadCovers": self.should_download_covers,
            "shouldDownloadSubtitles": self.should_download_subtitles,
            "shouldDownloadSlideshowImages": self.should_download_slideshow_images,
            "shouldDownloadAvatars": self.should_download_avatars,
            "shouldDownloadMusicCovers": self.should_download_music_covers,
            "searchSection": self.search_section,
            "proxyCountryCode": self.proxy_country_code
        }

class APIConfig:
    """Apify API configuration."""
    
    def __init__(self):
        self.api_token = os.getenv("APIFY_API_TOKEN")
        if not self.api_token:
            raise ValueError("APIFY_API_TOKEN environment variable is required")
        
        self.tiktok_actor_id = os.getenv("TIKTOK_ACTOR_ID", "clockworks~tiktok-scraper")
        self.base_url = "https://api.apify.com/v2"
        
    @property
    def headers(self) -> Dict[str, str]:
        """Get API headers."""
        return {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }

# Tier 1: Broad FYP Content
TIER1_CONFIG = ScrapingConfig(
    search_queries=[
        "fyp", "viral", "trending", "for you", "relatable", 
        "storytime", "life hack", "pov", "this you?", "tell me why"
    ],
    hashtags=[
        "fyp", "viral", "trending", "foryou", "relatable",
        "storytime", "lifehack", "pov"
    ],
    results_per_page=300  # Higher volume for broad content
)

# Tier 2: Pet Parent Bridge Content
TIER2_CONFIG = ScrapingConfig(
    search_queries=[
        "pet parent", "pet owner", "fur baby", "pet mom", "pet dad",
        "pet life", "pet problems", "pet expenses", "pet care tips", 
        "pet insurance worth it"
    ],
    hashtags=[
        "petparent", "petowner", "furbaby", "petmom", "petdad",
        "petlife", "petproblems", "petcare"
    ],
    results_per_page=200
)

# Tier 3: Cat Mom Niche
TIER3_CONFIG = ScrapingConfig(
    search_queries=[
        "cat mom", "cat parent", "indoor cat", "cat behavior", 
        "vet bills", "cat insurance", "cat anxiety", "cat health",
        "crazy cat lady", "cat product review"
    ],
    hashtags=[
        "catmom", "catparent", "catlady", "indoorcat", "catbehavior",
        "catsoftiktok", "catcare", "catproducts"
    ],
    results_per_page=200
)

TIER_CONFIGS = {
    "tier1": TIER1_CONFIG,
    "tier2": TIER2_CONFIG, 
    "tier3": TIER3_CONFIG
}
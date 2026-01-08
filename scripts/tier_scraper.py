"""
Individual tier scraping scripts for targeted TikTok data collection.
"""
import argparse
import json
import os
from datetime import datetime
from pathlib import Path
import logging

from apify_client import ApifyClient, ScrapingResult
from config import TIER_CONFIGS
from data_processor import save_scraping_results

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def scrape_tier1_fyp(max_items: int = None, save_data: bool = True) -> ScrapingResult:
    """
    Scrape Tier 1: Broad FYP content that catches massive eyeballs.
    
    Focus: High-volume viral content, trending formats, broad appeal hooks
    """
    client = ApifyClient()
    
    logger.info("🎯 Starting Tier 1 (FYP Broad) data collection...")
    logger.info("Target: Viral content, trending formats, mass appeal hooks")
    
    try:
        result = client.scrape_tier_data("tier1", max_items=max_items)
        
        logger.info(f"✅ Tier 1 completed: {result.total_items} videos collected in {result.execution_time:.2f}s")
        
        if save_data:
            save_scraping_results(result, "tier1_fyp_broad")
            
        return result
        
    except Exception as e:
        logger.error(f"❌ Tier 1 scraping failed: {str(e)}")
        raise

def scrape_tier2_pet_parents(max_items: int = None, save_data: bool = True) -> ScrapingResult:
    """
    Scrape Tier 2: Pet parent bridge content.
    
    Focus: Content that bridges general audience to pet ownership themes
    """
    client = ApifyClient()
    
    logger.info("🐕 Starting Tier 2 (Pet Parents) data collection...")
    logger.info("Target: Pet parent content, bridging general to pet-specific themes")
    
    try:
        result = client.scrape_tier_data("tier2", max_items=max_items)
        
        logger.info(f"✅ Tier 2 completed: {result.total_items} videos collected in {result.execution_time:.2f}s")
        
        if save_data:
            save_scraping_results(result, "tier2_pet_parents")
            
        return result
        
    except Exception as e:
        logger.error(f"❌ Tier 2 scraping failed: {str(e)}")
        raise

def scrape_tier3_cat_moms(max_items: int = None, save_data: bool = True) -> ScrapingResult:
    """
    Scrape Tier 3: Specific cat mom niche content.
    
    Focus: Highly targeted cat mom content, specific pain points and solutions
    """
    client = ApifyClient()
    
    logger.info("🐱 Starting Tier 3 (Cat Moms) data collection...")
    logger.info("Target: Cat mom niche, specific pain points, product reviews, insurance topics")
    
    try:
        result = client.scrape_tier_data("tier3", max_items=max_items)
        
        logger.info(f"✅ Tier 3 completed: {result.total_items} videos collected in {result.execution_time:.2f}s")
        
        if save_data:
            save_scraping_results(result, "tier3_cat_moms")
            
        return result
        
    except Exception as e:
        logger.error(f"❌ Tier 3 scraping failed: {str(e)}")
        raise

def scrape_single_tier(tier: str, max_items: int = None, save_data: bool = True) -> ScrapingResult:
    """
    Scrape a specific tier by name.
    
    Args:
        tier: Tier name (tier1, tier2, or tier3)
        max_items: Maximum number of items to scrape
        save_data: Whether to save results to file
    """
    tier_functions = {
        "tier1": scrape_tier1_fyp,
        "tier2": scrape_tier2_pet_parents, 
        "tier3": scrape_tier3_cat_moms
    }
    
    if tier not in tier_functions:
        raise ValueError(f"Invalid tier: {tier}. Available: {list(tier_functions.keys())}")
        
    return tier_functions[tier](max_items=max_items, save_data=save_data)

def main():
    """Command line interface for tier scraping."""
    parser = argparse.ArgumentParser(description="Scrape TikTok data by tier")
    parser.add_argument("tier", choices=["tier1", "tier2", "tier3", "all"], 
                       help="Tier to scrape")
    parser.add_argument("--max-items", type=int, 
                       help="Maximum number of items to scrape per tier")
    parser.add_argument("--no-save", action="store_true", 
                       help="Don't save results to file")
    parser.add_argument("--output-dir", type=str, default="data",
                       help="Output directory for results")
    
    args = parser.parse_args()
    
    # Set output directory
    os.environ["TIKTOK_DATA_DIR"] = args.output_dir
    
    try:
        if args.tier == "all":
            logger.info("🚀 Starting full tier analysis pipeline...")
            
            results = {}
            for tier in ["tier1", "tier2", "tier3"]:
                result = scrape_single_tier(tier, args.max_items, not args.no_save)
                results[tier] = result
                
            # Summary
            total_videos = sum(r.total_items for r in results.values())
            total_time = sum(r.execution_time for r in results.values())
            
            logger.info("="*50)
            logger.info("📊 COMPLETE TIER ANALYSIS SUMMARY")
            logger.info("="*50)
            for tier, result in results.items():
                logger.info(f"{tier.upper()}: {result.total_items} videos ({result.execution_time:.1f}s)")
            logger.info(f"TOTAL: {total_videos} videos collected in {total_time:.1f}s")
            logger.info("="*50)
            
        else:
            result = scrape_single_tier(args.tier, args.max_items, not args.no_save)
            logger.info(f"✅ {args.tier.upper()} scraping completed: {result.total_items} videos")
            
    except Exception as e:
        logger.error(f"❌ Scraping failed: {str(e)}")
        return 1
        
    return 0

if __name__ == "__main__":
    exit(main())
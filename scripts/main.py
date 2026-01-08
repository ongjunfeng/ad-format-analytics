#!/usr/bin/env python3
"""
Main orchestrator script for TikTok ad format analytics data collection.

This script coordinates the complete data collection pipeline across all tiers
and generates comprehensive analysis reports.
"""
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from tier_scraper import scrape_tier1_fyp, scrape_tier2_pet_parents, scrape_tier3_cat_moms
from data_processor import generate_tier_comparison_report, save_analysis_report
from apify_client import ScrapingResult

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_complete_analysis(max_items_per_tier: int = None, 
                         save_individual: bool = True,
                         generate_report: bool = True) -> dict:
    """
    Run complete TikTok analytics data collection across all tiers.
    
    Args:
        max_items_per_tier: Maximum items to collect per tier
        save_individual: Whether to save individual tier results
        generate_report: Whether to generate cross-tier analysis report
        
    Returns:
        Dictionary containing all results and analysis
    """
    logger.info("🚀 STARTING COMPLETE TIKTOK AD FORMAT ANALYTICS")
    logger.info("=" * 60)
    
    start_time = datetime.now()
    results = {}
    
    try:
        # Tier 1: Broad FYP Content
        logger.info("📈 PHASE 1: Collecting broad FYP viral content...")
        tier1_result = scrape_tier1_fyp(max_items=max_items_per_tier, save_data=save_individual)
        results['tier1'] = tier1_result
        
        logger.info(f"   ✅ Tier 1 complete: {tier1_result.total_items} videos")
        logger.info(f"   🎯 Focus: Viral mechanics, mass-appeal hooks, trending formats")
        
        # Tier 2: Pet Parent Bridge
        logger.info("\n🐕 PHASE 2: Collecting pet parent bridge content...")
        tier2_result = scrape_tier2_pet_parents(max_items=max_items_per_tier, save_data=save_individual)
        results['tier2'] = tier2_result
        
        logger.info(f"   ✅ Tier 2 complete: {tier2_result.total_items} videos")
        logger.info(f"   🎯 Focus: General-to-pet audience bridging, pet parent themes")
        
        # Tier 3: Cat Mom Niche  
        logger.info("\n🐱 PHASE 3: Collecting cat mom niche content...")
        tier3_result = scrape_tier3_cat_moms(max_items=max_items_per_tier, save_data=save_individual)
        results['tier3'] = tier3_result
        
        logger.info(f"   ✅ Tier 3 complete: {tier3_result.total_items} videos")
        logger.info(f"   🎯 Focus: Cat mom pain points, insurance topics, product reviews")
        
        # Generate comprehensive analysis report
        if generate_report:
            logger.info("\n📊 PHASE 4: Generating cross-tier analysis report...")
            analysis_report = generate_tier_comparison_report(results)
            report_file = save_analysis_report(analysis_report, "complete_tier_analysis")
            
            logger.info(f"   ✅ Analysis report saved: {report_file}")
            results['analysis_report'] = analysis_report
            results['report_file'] = report_file
        
        # Final summary
        total_videos = sum(r.total_items for r in [tier1_result, tier2_result, tier3_result])
        total_time = datetime.now() - start_time
        
        logger.info("\n" + "=" * 60)
        logger.info("🎉 COMPLETE ANALYSIS FINISHED!")
        logger.info("=" * 60)
        logger.info(f"📊 TIER 1 (FYP Broad):     {tier1_result.total_items:>6} videos")
        logger.info(f"🐕 TIER 2 (Pet Parents):   {tier2_result.total_items:>6} videos")
        logger.info(f"🐱 TIER 3 (Cat Moms):      {tier3_result.total_items:>6} videos")
        logger.info(f"📈 TOTAL VIDEOS COLLECTED: {total_videos:>6}")
        logger.info(f"⏱️  TOTAL EXECUTION TIME:   {total_time}")
        logger.info("=" * 60)
        
        # Key insights summary
        logger.info("\n🔍 KEY COLLECTION INSIGHTS:")
        logger.info(f"   • Viral Content Analysis: Ready for hook/format pattern extraction")
        logger.info(f"   • Audience Bridging Data: Ready for transition strategy analysis")  
        logger.info(f"   • Niche Targeting Intel: Ready for cat mom pain point mapping")
        logger.info(f"   • Cross-Tier Comparison: Available for funnel optimization insights")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Analysis pipeline failed: {str(e)}")
        raise

def run_single_tier_analysis(tier: str, max_items: int = None) -> ScrapingResult:
    """Run analysis for a single tier."""
    tier_functions = {
        "tier1": scrape_tier1_fyp,
        "tier2": scrape_tier2_pet_parents,
        "tier3": scrape_tier3_cat_moms
    }
    
    if tier not in tier_functions:
        raise ValueError(f"Invalid tier: {tier}. Available: {list(tier_functions.keys())}")
    
    logger.info(f"🎯 Running single tier analysis: {tier.upper()}")
    result = tier_functions[tier](max_items=max_items, save_data=True)
    
    logger.info(f"✅ {tier.upper()} analysis complete: {result.total_items} videos collected")
    return result

def main():
    """Command line interface for the complete analytics pipeline."""
    parser = argparse.ArgumentParser(
        description="TikTok Ad Format Analytics - Complete Data Collection Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                           # Run complete analysis (all tiers)
  python main.py --tier tier1              # Run single tier analysis
  python main.py --max-items 100           # Limit items per tier
  python main.py --quick                   # Quick analysis (50 items per tier)
  python main.py --tier tier3 --max-items 500  # Deep dive into cat mom content
        """
    )
    
    parser.add_argument("--tier", choices=["tier1", "tier2", "tier3"],
                       help="Run single tier analysis instead of complete pipeline")
    parser.add_argument("--max-items", type=int,
                       help="Maximum items to collect per tier")
    parser.add_argument("--quick", action="store_true",
                       help="Quick analysis mode (50 items per tier)")
    parser.add_argument("--no-save", action="store_true",
                       help="Don't save individual tier results")
    parser.add_argument("--no-report", action="store_true", 
                       help="Skip cross-tier analysis report generation")
    
    args = parser.parse_args()
    
    # Set max items based on flags
    max_items = args.max_items
    if args.quick and not max_items:
        max_items = 50
    
    try:
        if args.tier:
            # Single tier analysis
            result = run_single_tier_analysis(args.tier, max_items)
            logger.info(f"🎯 Analysis complete for {args.tier.upper()}")
            
        else:
            # Complete pipeline
            results = run_complete_analysis(
                max_items_per_tier=max_items,
                save_individual=not args.no_save,
                generate_report=not args.no_report
            )
            
            logger.info("🏁 Complete TikTok ad format analytics pipeline finished!")
            logger.info("📁 Check the 'data/' directory for all collected data and analysis")
        
        return 0
        
    except KeyboardInterrupt:
        logger.info("\n⏹️  Analysis interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
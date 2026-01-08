#!/usr/bin/env python3

import sys
import logging
import json
import subprocess
import pandas as pd
import os
import time
from pathlib import Path
from datetime import datetime
from typing import List
from config import ScrapingConfig, IG_REEL_SCRAPING_CONFIG, DatabricksConfig, DATABRICKS_CONFIG
from data_scraper import DataScraper
from scraping_result import ScrapingResult
from video_processor import process_dataframe
from label_data_as_viral import get_top_engagement_from_both 
from s3_service import S3Service
from service import analyze_video 
from databricks_service import DatabricksService 

# TO BE IMPLEMENTED
from hypothesis_engine import HypothesisEngine # Assuming this is where your HypothesisEngine class resides

def extract_post_id_from_url(url: str) -> str:
    """Extract post ID (shortcode) from Instagram URL."""
    # Example: https://www.instagram.com/reel/C8mtEPSp4b8/
    try:
        # Split by '/', filter out empty strings, get the second-to-last element
        parts = url.rstrip('/').split('/')
        # It's usually the second-to-last part for post URLs, but handling 'p' path in case
        if 'p' in parts:
            p_index = parts.index('p')
            return parts[p_index + 1] if len(parts) > p_index + 1 else parts[-1]
        
        post_id = parts[-1] if parts[-1] else parts[-2]
        return post_id
    except:
        return None

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define the output JSON path for the Cat Data Hypothesis
CAT_HYPOTHESIS_JSON = "cat_data_hypotheses.json"


def main():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ==================== STEP 1: SCRAPE INSTAGRAM DATA (APIFY/AIRFLOW) ====================
    logger.info("=" * 70)
    logger.info("STEP 1: SCRAPING INSTAGRAM DATA")
    logger.info("=" * 70)

    scraper = DataScraper()
    logger.info("Starting scraper run:")
    ig_posts = scraper.run_scraping_config(IG_REEL_SCRAPING_CONFIG)
    scraped_df = ig_posts.to_pandas()
    
    os.makedirs("datasets/instagram", exist_ok=True)
    scraped_excel = f"datasets/instagram/cat_data_{timestamp}.xlsx"
    scraped_df.to_excel(scraped_excel, index=False)
    logger.info(f"💾 Saved scraped data: {scraped_excel} ({len(scraped_df)} posts)")

    # ==================== STEP 2: LABEL DATA & INITIAL MERGE ====================
    logger.info("\n" + "=" * 70)
    logger.info("STEP 2: LABELING VIRAL CONTENT & LOADING LABELED DATA")
    logger.info("=" * 70)

    ad_data_path = "Ad Level Data.xlsx"
    cat_data_path = scraped_excel

    # Label viral content (This step is assumed to create 'labeled_scraped_data.json')
    logger.info("🏷️ Running viral labeling process...")
    thresholds = get_top_engagement_from_both(
        ad_data_path=ad_data_path,
        cat_data_path=cat_data_path,
        top_percent=0.2
    )
    logger.info(f"✅ Labeling complete. Thresholds: {thresholds}")

    # Load labeled scraped JSON back for downstream steps
    labeled_json_path = "labeled_scraped_data.json"
    if not os.path.exists(labeled_json_path):
        logger.error(f"❌ Labeled data file not found at {labeled_json_path}. Exiting.")
        sys.exit(1)
        
    with open(labeled_json_path, "r", encoding="utf-8") as f:
        labeled_data = json.load(f)
    labeled_df = pd.DataFrame(labeled_data)
    logger.info(f"✅ Loaded labeled data with {len(labeled_df)} records")

    # ==================== STEP 3: UPLOAD MEDIA TO S3 (Instaloader/Video Processor) ====================
    logger.info("\n" + "=" * 70)
    logger.info("STEP 3: UPLOADING MEDIA TO S3")
    logger.info("=" * 70)
    
    # Upload videos to S3
    process_dataframe(labeled_df, "Post URL")
    logger.info("✅ Media uploaded to S3")

    # ==================== STEP 4: DOWNLOAD FROM S3 & ORGANIZE (for Gemini LLM) ====================
    logger.info("\n" + "=" * 70)
    logger.info("STEP 4: DOWNLOADING FROM S3 & ORGANIZING LOCALLY")
    logger.info("=" * 70)
    
    s3_service = S3Service()
    download_base_dir = "datasets/instagram/temp_llm_input"
    s3_service.delete_local_downloads(download_base_dir)

    # Prepare for download
    labeled_df['post_id'] = labeled_df['Post URL'].apply(extract_post_id_from_url)
    viral_post_ids = labeled_df[labeled_df.get('viral', False) == True]['post_id'].dropna().tolist()
    non_viral_post_ids = labeled_df[labeled_df.get('viral', False) == False]['post_id'].dropna().tolist()
    
    logger.info(f"📊 Viral posts: {len(viral_post_ids)}, Non-viral posts: {len(non_viral_post_ids)}")
    
    # Download videos in batches
    logger.info("⬇️ Downloading viral and non-viral videos...")
    viral_downloaded = s3_service.download_batch(viral_post_ids, os.path.join(download_base_dir, "viral"))
    non_viral_downloaded = s3_service.download_batch(non_viral_post_ids, os.path.join(download_base_dir, "non_viral"))
    
    all_downloaded_posts = {**viral_downloaded, **non_viral_downloaded}
    
    # Update DataFrame with local file paths and filter
    labeled_df['local_video_path'] = labeled_df['post_id'].map(all_downloaded_posts)
    llm_input_df = labeled_df.dropna(subset=['local_video_path']).copy()
    
    logger.info(f"✅ Ready for LLM analysis: {len(llm_input_df)} posts with local video files.")

    # ==================== STEP 5: GEMINI LLM VIDEO ANALYSIS (Cat Data) ====================
    logger.info("\n" + "=" * 70)
    logger.info("STEP 5: GEMINI LLM VIDEO ANALYSIS (CAT DATA)")
    logger.info("=" * 70)
    
    all_video_analysis = []
    
    for index, row in llm_input_df.iterrows():
        post_id = row['post_id']
        video_path = row['local_video_path']
        post_metadata = row.to_dict()
        
        try:
            logger.info(f"🧠 Analyzing video: {post_id}")
            
            # Call analyze_video from service.py
            analysis_results_dict = analyze_video(video_path, post_metadata, post_id)
            
            # Extract the required fields from the service.py output
            result_row = {
                'post_id': post_id,
                'Post URL': row['Post URL'],
                # Capture the three required fields
                'video_analysis': analysis_results_dict.get('video_analysis'),
                'performance_context': analysis_results_dict.get('performance_context'),
                'virality_analysis': analysis_results_dict.get('virality_analysis')
            }
            
            all_video_analysis.append(result_row)
            logger.info(f"✅ Analysis complete for {post_id}")
            
        except Exception as e:
            logger.error(f"❌ Failed LLM analysis for post {post_id}: {e}")
            
    analysis_df = pd.DataFrame(all_video_analysis)
    
    # Merge LLM analysis back into the main scraped dataset
    # Drop existing LLM-related columns before merge if they somehow existed
    cols_to_drop = ['video_analysis', 'performance_context', 'virality_analysis']
    llm_enriched_df = pd.merge(llm_input_df.drop(columns=cols_to_drop, errors='ignore'), 
                                 analysis_df, 
                                 on=['Post URL', 'post_id'], 
                                 how='inner')
    
    # Save the enriched data
    final_output_path = f"datasets/instagram/llm_enriched_cat_data_{timestamp}.xlsx"
    llm_enriched_df.to_excel(final_output_path, index=False)
    logger.info(f"💾 Saved LLM-enriched CAT data: {final_output_path} ({len(llm_enriched_df)} records)")
    
    
    # ==================== STEP 6: HYPOTHESIS GENERATION (Cat Data) ====================
    logger.info("\n" + "=" * 70)
    logger.info("STEP 6: HYPOTHESIS GENERATION (CAT DATA)")
    logger.info("=" * 70)
    
    hypothesis_engine = HypothesisEngine(llm_enriched_df)
    
    # 1. Compare observations (Viral vs Non-viral)
    logger.info("🔬 Generating comparative observations...")
    comparative_report = hypothesis_engine.compare_viral_vs_non_viral_observations()
    
    # 2. Generate cross-category hypothesis
    logger.info("💡 Generating cross-category hypotheses...")
    hypotheses = hypothesis_engine.generate_cross_category_hypothesis()

    # 3. Generate Ad Format Suggestions
    logger.info("📝 Generating Ad Format Suggestions...")
    ad_suggestions = hypothesis_engine.generate_ad_formats_suggestions(hypotheses)
    
    # Package and save the Cat Data Hypothesis results
    cat_hypotheses_output = {
        "timestamp": timestamp,
        "source": "Scraped Cat Content",
        "comparative_observations": comparative_report,
        "cross_category_hypotheses": hypotheses,
        "ad_format_suggestions": ad_suggestions
    }
    
    # ==================== STEP 7: CLEANUP & NEXT STEPS ====================
    # The next step would be to load the Ad Data, run LLM analysis on it, 
    # and use CAT_HYPOTHESIS_JSON as input to the Ad Data Hypothesis Engine.
    logger.info("\n" + "=" * 70)
    logger.info("NEXT STEP: Ad Data LLM Analysis & Hypothesis Generation, using Cat Data Hypotheses as input.")
    logger.info("=" * 70)
    
    # Final cleanup of local video files
    s3_service.delete_local_downloads(download_base_dir)

    # Package and save the Cat Data Hypothesis results
    cat_hypotheses_output = {
        "timestamp": timestamp,
        "source": "Scraped Cat Content",
        "comparative_observations": comparative_report,
        "cross_category_hypotheses": hypotheses,
        "ad_format_suggestions": ad_suggestions
    }

    CAT_HYPOTHESIS_JSON = "cat_data_hypotheses.json"
    with open(CAT_HYPOTHESIS_JSON, "w") as f:
        json.dump(cat_hypotheses_output, f, indent=4)
    logger.info(f"💾 Saved CAT data hypotheses as input for Ad data analysis: {CAT_HYPOTHESIS_JSON}")
    
    
    # ==================== STEP 7: DATABRICKS UPLOAD & CLEANUP ====================
    logger.info("\n" + "=" * 70)
    logger.info("STEP 7: UPLOADING DATA TO DATABRICKS & CLEANUP")
    logger.info("=" * 70)
    
    s3_service = S3Service()
    download_base_dir = "datasets/instagram/temp_llm_input"
    
    try:
        # Initialize Databricks Service using config
        db_service = DatabricksService(config=DATABRICKS_CONFIG)
        
        # 1. Upload the LLM-enriched DataFrame
        db_service.upload_dataframe(
            df=llm_enriched_df, 
            table_name="llm_enriched_cat_content", 
            mode="append" # Use 'overwrite' if you want to replace the table every run
        )
        
        # 2. Upload the Hypothesis results (convert the JSON structure to a small DataFrame)
        hypotheses_df = pd.DataFrame([
            {"timestamp": timestamp, 
             "source": "cat_content", 
             "hypotheses_json": json.dumps(cat_hypotheses_output)}
        ])
        
        db_service.upload_dataframe(
            df=hypotheses_df, 
            table_name="analysis_hypotheses", 
            mode="append"
        )
        
    except Exception as e:
        logger.error(f"🔥 Critical: Databricks upload failed. Data not persisted to warehouse. Error: {e}")
        # The pipeline should likely continue to clean up even on failure
        
    # Final cleanup of local video files
    s3_service.delete_local_downloads(download_base_dir)

    logger.info("\n" + "=" * 70)
    logger.info("✅ Pipeline execution for Scraped Cat Data analysis complete!")
    logger.info("======================================================================")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
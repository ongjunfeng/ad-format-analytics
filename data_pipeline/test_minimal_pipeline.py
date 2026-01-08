"""
Minimal Pipeline Test - Runs on 5 posts only
Tests: Labeling → S3 → Download → Gemini (mock) → Hypothesis → Databricks
"""
import sys
import logging
import json
import pandas as pd
import os
from datetime import datetime

from s3_service import S3Service
from hypothesis_engine import HypothesisEngine
from databricks_service import DatabricksService
from config import DATABRICKS_CONFIG

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print("="*70)
    print("MINIMAL PIPELINE TEST - 5 POSTS ONLY")
    print("="*70)
    
    # ==================== STEP 1: LOAD EXISTING LABELED DATA ====================
    logger.info("\n📥 STEP 1: Loading existing labeled data...")
    
    with open("../datasets/instagram/labeled_scraped_data.json", "r", encoding="utf-8") as f:
        labeled_data = json.load(f)
    
    # Take only first 5 posts for testing
    labeled_data_sample = labeled_data[:5]
    labeled_df = pd.DataFrame(labeled_data_sample)
    
    logger.info(f"✅ Loaded {len(labeled_df)} posts for testing")
    logger.info(f"   Viral: {labeled_df.get('viral', pd.Series([False])).sum()}")
    logger.info(f"   Non-viral: {(~labeled_df.get('viral', pd.Series([True]))).sum()}")
    
    # ==================== STEP 2: EXTRACT POST IDs ====================
    logger.info("\n🔍 STEP 2: Extracting post IDs...")
    
    def extract_post_id(url):
        try:
            parts = url.rstrip('/').split('/')
            if 'p' in parts:
                p_index = parts.index('p')
                return parts[p_index + 1]
            return parts[-1] if parts[-1] else parts[-2]
        except:
            return None
    
    labeled_df['post_id'] = labeled_df['Post URL'].apply(extract_post_id)
    labeled_df = labeled_df.dropna(subset=['post_id'])
    
    logger.info(f"✅ Extracted {len(labeled_df)} post IDs")
    for idx, row in labeled_df.iterrows():
        logger.info(f"   - {row['post_id']}: {row.get('Views', 0):,} views")
    
    # ==================== STEP 3: CHECK S3 (OPTIONAL - Skip if no videos) ====================
    logger.info("\n☁️ STEP 3: Checking S3 videos...")
    logger.info("⏭️ Skipping S3 download for now (videos may not be uploaded yet)")
    
    # If you want to test S3:
    # s3_service = S3Service()
    # post_ids = labeled_df['post_id'].tolist()
    # downloaded = s3_service.download_batch(post_ids[:2], local_dir="../datasets/instagram/test_downloads")
    # logger.info(f"✅ Downloaded {len(downloaded)} videos")
    
    # ==================== STEP 4: SIMULATE GEMINI ANALYSIS ====================
    logger.info("\n🧠 STEP 4: Simulating Gemini analysis (using mock data)...")
    
    # For this test, add mock analysis to existing data
    labeled_df['video_analysis'] = 'Mock video analysis: ' + labeled_df['post_id']
    labeled_df['virality_analysis'] = 'Mock virality analysis for post ' + labeled_df['post_id']
    
    logger.info(f"✅ Added mock Gemini analysis to {len(labeled_df)} posts")
    
    # ==================== STEP 5: HYPOTHESIS GENERATION ====================
    logger.info("\n🔬 STEP 5: Generating hypotheses with HypothesisEngine...")
    
    engine = HypothesisEngine(labeled_df)
    
    # Generate all outputs
    logger.info("   Generating comparative observations...")
    observations = engine.compare_viral_vs_non_viral_observations()
    
    logger.info("   Generating cross-category hypotheses...")
    hypotheses = engine.generate_cross_category_hypothesis()
    
    logger.info("   Generating ad format suggestions...")
    ad_formats = engine.generate_ad_formats_suggestions(hypotheses)
    
    logger.info(f"✅ Hypothesis generation complete!")
    logger.info(f"   - {len(hypotheses.get('hypotheses', []))} hypotheses")
    logger.info(f"   - {len(ad_formats.get('ad_formats', []))} ad formats")
    
    # ==================== STEP 6: SAVE RESULTS ====================
    logger.info("\n💾 STEP 6: Saving results...")
    
    output_package = {
        "timestamp": timestamp,
        "source": "minimal_pipeline_test",
        "sample_size": len(labeled_df),
        "comparative_observations": observations,
        "cross_category_hypotheses": hypotheses,
        "ad_format_suggestions": ad_formats
    }
    
    output_file = f"minimal_pipeline_output_{timestamp}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output_package, f, indent=2, ensure_ascii=False)
    
    logger.info(f"✅ Saved results to: {output_file}")
    
    # ==================== STEP 7: DATABRICKS UPLOAD (OPTIONAL) ====================
    logger.info("\n📊 STEP 7: Databricks upload...")
    
    try:
        db_service = DatabricksService(config=DATABRICKS_CONFIG)
        
        # Upload the enriched dataframe
        logger.info("   Uploading enriched data to Databricks...")
        db_service.upload_dataframe(
            df=labeled_df,
            table_name="test_minimal_pipeline_data",
            mode="overwrite"
        )
        
        # Upload hypothesis summary
        hypothesis_summary = pd.DataFrame([{
            "timestamp": timestamp,
            "source": "minimal_test",
            "num_hypotheses": len(hypotheses.get('hypotheses', [])),
            "num_ad_formats": len(ad_formats.get('ad_formats', [])),
            "sample_size": len(labeled_df)
        }])
        
        db_service.upload_dataframe(
            df=hypothesis_summary,
            table_name="test_hypothesis_summary",
            mode="append"
        )
        
        logger.info("✅ Successfully uploaded to Databricks!")
        
    except Exception as e:
        logger.warning(f"⚠️ Databricks upload failed (this is OK for testing): {e}")
        logger.info("   Continuing without Databricks...")
    
    # ==================== SUMMARY ====================
    print("\n" + "="*70)
    print("✅ MINIMAL PIPELINE TEST COMPLETE!")
    print("="*70)
    print(f"\nResults:")
    print(f"  - Processed: {len(labeled_df)} posts")
    print(f"  - Hypotheses generated: {len(hypotheses.get('hypotheses', []))}")
    print(f"  - Ad formats created: {len(ad_formats.get('ad_formats', []))}")
    print(f"  - Output saved to: {output_file}")
    print(f"\nNext steps:")
    print(f"  1. Review {output_file} to see the results")
    print(f"  2. Check Databricks tables (if uploaded)")
    print(f"  3. Scale up to more posts if everything looks good")
    print("="*70)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
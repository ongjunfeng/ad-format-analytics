"""
Dry run test - Simulates pipeline flow without external APIs
"""
import pandas as pd
from hypothesis_engine import HypothesisEngine
import json

print("="*70)
print("DRY RUN: SIMULATING FULL PIPELINE")
print("="*70)

# Simulate Step 1: Scraped data
print("\n📥 Step 1: Simulating scraped data...")
scraped_df = pd.DataFrame([
    {'Post URL': 'https://instagram.com/reel/TEST1/', 'Views': 100000, 'Likes': 10000, 'Comments': 500},
    {'Post URL': 'https://instagram.com/reel/TEST2/', 'Views': 5000, 'Likes': 500, 'Comments': 50}
])
print(f"   ✅ Scraped {len(scraped_df)} posts")

# Simulate Step 2: Labeled data
print("\n🏷️ Step 2: Simulating viral labeling...")
scraped_df['viral'] = [True, False]
scraped_df['post_id'] = ['TEST1', 'TEST2']
print(f"   ✅ Labeled data: {scraped_df['viral'].sum()} viral, {(~scraped_df['viral']).sum()} non-viral")

# Simulate Step 3: S3 upload (skip - would upload to S3)
print("\n☁️ Step 3: S3 upload...")
print("   ⏭️ Skipped (would upload videos)")

# Simulate Step 4: Gemini analysis
print("\n🧠 Step 4: Simulating Gemini analysis...")
scraped_df['video_analysis'] = 'Mock video analysis'
scraped_df['virality_analysis'] = 'Mock virality analysis'
print(f"   ✅ Analyzed {len(scraped_df)} videos")

# Step 5: Hypothesis generation (REAL)
print("\n🔬 Step 5: Running REAL hypothesis engine...")
engine = HypothesisEngine(scraped_df)

observations = engine.compare_viral_vs_non_viral_observations()
print(f"   ✅ Generated comparative observations")

hypotheses = engine.generate_cross_category_hypothesis()
print(f"   ✅ Generated {len(hypotheses['hypotheses'])} hypotheses")

ad_formats = engine.generate_ad_formats_suggestions(hypotheses)
print(f"   ✅ Generated {len(ad_formats['ad_formats'])} ad format suggestions")

# Step 6: Save output
print("\n💾 Step 6: Saving results...")
output = {
    "timestamp": "2024-01-01",
    "source": "dry_run_test",
    "comparative_observations": observations,
    "cross_category_hypotheses": hypotheses,
    "ad_format_suggestions": ad_formats
}

with open("dry_run_output.json", "w") as f:
    json.dump(output, f, indent=2)
print(f"   ✅ Saved to dry_run_output.json")

# Step 7: Databricks upload (skip)
print("\n📊 Step 7: Databricks upload...")
print("   ⏭️ Skipped (would upload to Databricks)")

print("\n" + "="*70)
print("✅ DRY RUN COMPLETE - Pipeline flow validated!")
print("="*70)
print("\nYou can now:")
print("1. Check dry_run_output.json to see the results")
print("2. Run the real pipeline with actual data")
print("3. Add more unit tests for individual components")
import pandas as pd
import json
from hypothesis_engine import HypothesisEngine

print("="*70)
print("MANUAL TEST: HYPOTHESIS ENGINE")
print("="*70)

# Create sample data
sample_df = pd.DataFrame([
    {
        'post_id': 'VIRAL1',
        'Post URL': 'https://instagram.com/viral1/',
        'viral': True,
        'Views': 100000,
        'Likes': 10000,
        'Comments': 500,
        'video_analysis': 'Mock analysis',
        'virality_analysis': 'Mock virality'
    },
    {
        'post_id': 'NONVIRAL1',
        'Post URL': 'https://instagram.com/nonviral1/',
        'viral': False,
        'Views': 5000,
        'Likes': 500,
        'Comments': 50,
        'video_analysis': 'Mock analysis',
        'virality_analysis': 'Mock virality'
    }
])

# Initialize engine
engine = HypothesisEngine(sample_df)

# Test all methods
print("\n1. Comparative Observations:")
obs = engine.compare_viral_vs_non_viral_observations()
print(json.dumps(obs, indent=2))

print("\n2. Hypotheses:")
hyp = engine.generate_cross_category_hypothesis()
print(json.dumps(hyp, indent=2))

print("\n3. Ad Formats:")
ads = engine.generate_ad_formats_suggestions(hyp)
print(json.dumps(ads, indent=2))

print("\n" + "="*70)
print("✅ TEST COMPLETE")
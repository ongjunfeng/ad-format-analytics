import sys
import os
import pytest
import pandas as pd
import json

# Add data_pipeline directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data_pipeline')))


class TestPipelineIntegration:
    """Integration test for components we can control"""
    
    @pytest.fixture
    def sample_analysis_data(self):
        """Sample data after Gemini analysis"""
        return pd.DataFrame([
            {
                'post_id': 'VIRAL1',
                'Post URL': 'https://www.instagram.com/reel/VIRAL1/',
                'Views': 100000,
                'Likes': 10000,
                'Comments': 500,
                'viral': True,
                'video_analysis': 'Engaging hook in first 3 seconds',
                'virality_analysis': 'Strong emotional appeal'
            },
            {
                'post_id': 'VIRAL2',
                'Post URL': 'https://www.instagram.com/reel/VIRAL2/',
                'Views': 150000,
                'Likes': 15000,
                'Comments': 800,
                'viral': True,
                'video_analysis': 'Dynamic camera work',
                'virality_analysis': 'Trending audio used'
            },
            {
                'post_id': 'NONVIRAL1',
                'Post URL': 'https://www.instagram.com/reel/NONVIRAL1/',
                'Views': 5000,
                'Likes': 500,
                'Comments': 50,
                'viral': False,
                'video_analysis': 'Slow opening',
                'virality_analysis': 'Low engagement'
            },
            {
                'post_id': 'NONVIRAL2',
                'Post URL': 'https://www.instagram.com/reel/NONVIRAL2/',
                'Views': 3000,
                'Likes': 300,
                'Comments': 30,
                'viral': False,
                'video_analysis': 'Generic content',
                'virality_analysis': 'Minimal reach'
            }
        ])
    
    def test_hypothesis_engine_full_workflow(self, sample_analysis_data):
        """Test complete hypothesis generation workflow"""
        from hypothesis_engine import HypothesisEngine
        
        # Initialize engine
        engine = HypothesisEngine(sample_analysis_data)
        
        # Test all three methods in sequence
        print("\n" + "="*60)
        print("TESTING FULL HYPOTHESIS ENGINE WORKFLOW")
        print("="*60)
        
        # Step 1: Compare observations
        print("\n1️⃣ Comparing viral vs non-viral observations...")
        observations = engine.compare_viral_vs_non_viral_observations()
        
        assert isinstance(observations, dict)
        assert 'viral_patterns_observed' in observations
        assert 'non_viral_gaps_identified' in observations
        assert len(observations['viral_patterns_observed']) > 0
        print(f"   ✅ Generated {len(observations['viral_patterns_observed'])} viral patterns")
        
        # Step 2: Generate hypotheses
        print("\n2️⃣ Generating cross-category hypotheses...")
        hypotheses = engine.generate_cross_category_hypothesis()
        
        assert isinstance(hypotheses, dict)
        assert 'hypotheses' in hypotheses
        assert len(hypotheses['hypotheses']) > 0
        print(f"   ✅ Generated {len(hypotheses['hypotheses'])} hypotheses")
        
        # Step 3: Generate ad formats
        print("\n3️⃣ Generating ad format suggestions...")
        ad_formats = engine.generate_ad_formats_suggestions(hypotheses)
        
        assert isinstance(ad_formats, dict)
        assert 'ad_formats' in ad_formats
        assert len(ad_formats['ad_formats']) > 0
        print(f"   ✅ Generated {len(ad_formats['ad_formats'])} ad format suggestions")
        
        # Test summary stats
        print("\n4️⃣ Checking summary statistics...")
        stats = engine.get_summary_stats()
        assert stats['total_posts'] == 4
        assert stats['viral_posts'] == 2
        assert stats['non_viral_posts'] == 2
        print(f"   ✅ Stats correct: {stats}")
        
        print("\n" + "="*60)
        print("✅ FULL WORKFLOW COMPLETED SUCCESSFULLY!")
        print("="*60)
    
    def test_output_structure_matches_expected(self, sample_analysis_data):
        """Test that outputs match the structure needed for downstream"""
        from hypothesis_engine import HypothesisEngine
        
        engine = HypothesisEngine(sample_analysis_data)
        
        # Generate all outputs
        observations = engine.compare_viral_vs_non_viral_observations()
        hypotheses = engine.generate_cross_category_hypothesis()
        ad_formats = engine.generate_ad_formats_suggestions(hypotheses)
        
        # Package as would be done in main.py
        output_package = {
            "timestamp": "2024-01-01",
            "source": "test_data",
            "comparative_observations": observations,
            "cross_category_hypotheses": hypotheses,
            "ad_format_suggestions": ad_formats
        }
        
        # Test it's JSON serializable
        json_str = json.dumps(output_package, indent=2)
        assert len(json_str) > 0
        
        # Test it can be loaded back
        loaded = json.loads(json_str)
        assert 'comparative_observations' in loaded
        assert 'cross_category_hypotheses' in loaded
        assert 'ad_format_suggestions' in loaded
        
        print(f"\n✅ Output package is valid JSON ({len(json_str)} bytes)")
        print(f"✅ Contains all required sections")
    
    def test_metrics_calculation_accuracy(self, sample_analysis_data):
        """Test that metrics are calculated correctly"""
        from hypothesis_engine import HypothesisEngine
        
        engine = HypothesisEngine(sample_analysis_data)
        observations = engine.compare_viral_vs_non_viral_observations()
        
        metrics = observations['metrics_comparison']
        
        # Check viral metrics
        viral_avg_views = metrics['viral']['avg_views']
        assert viral_avg_views == 125000  # (100000 + 150000) / 2
        
        # Check viral lift
        viral_lift = metrics['viral_lift']['views_multiplier']
        assert viral_lift > 1  # Viral should have more views
        
        print(f"\n✅ Viral avg views: {viral_avg_views:,.0f}")
        print(f"✅ Viral lift: {viral_lift:.2f}x")
    
    def test_mock_labels_present(self, sample_analysis_data):
        """Verify that [MOCK] labels are in outputs"""
        from hypothesis_engine import HypothesisEngine
        
        engine = HypothesisEngine(sample_analysis_data)
        observations = engine.compare_viral_vs_non_viral_observations()
        hypotheses = engine.generate_cross_category_hypothesis()
        ad_formats = engine.generate_ad_formats_suggestions(hypotheses)
        
        # Check for [MOCK] labels
        patterns = observations['viral_patterns_observed']
        assert any('[MOCK]' in str(p) for p in patterns)
        
        hypothesis_list = hypotheses['hypotheses']
        assert any('[MOCK]' in str(h.get('hypothesis', '')) for h in hypothesis_list)
        
        ad_format_list = ad_formats['ad_formats']
        assert any('[MOCK]' in str(af.get('format_name', '')) for af in ad_format_list)
        
        print(f"\n✅ All outputs properly labeled as [MOCK]")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

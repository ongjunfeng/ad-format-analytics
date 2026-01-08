import sys
import os
import pytest
import pandas as pd
import json

# Add data_pipeline directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data_pipeline')))

from hypothesis_engine import HypothesisEngine


class TestHypothesisEngine:
    """Test the mock Hypothesis Engine"""
    
    @pytest.fixture
    def sample_data(self):
        """Create sample data for testing"""
        return pd.DataFrame([
            {
                'post_id': 'VIRAL1',
                'Post URL': 'https://instagram.com/reel/VIRAL1/',
                'viral': True,
                'Views': 100000,
                'Likes': 10000,
                'Comments': 500,
                'video_analysis': 'Mock viral analysis 1',
                'virality_analysis': 'This went viral because...'
            },
            {
                'post_id': 'VIRAL2',
                'Post URL': 'https://instagram.com/reel/VIRAL2/',
                'viral': True,
                'Views': 150000,
                'Likes': 15000,
                'Comments': 800,
                'video_analysis': 'Mock viral analysis 2',
                'virality_analysis': 'Strong engagement...'
            },
            {
                'post_id': 'NONVIRAL1',
                'Post URL': 'https://instagram.com/reel/NONVIRAL1/',
                'viral': False,
                'Views': 5000,
                'Likes': 500,
                'Comments': 50,
                'video_analysis': 'Mock non-viral analysis 1',
                'virality_analysis': 'Low engagement...'
            },
            {
                'post_id': 'NONVIRAL2',
                'Post URL': 'https://instagram.com/reel/NONVIRAL2/',
                'viral': False,
                'Views': 3000,
                'Likes': 300,
                'Comments': 30,
                'video_analysis': 'Mock non-viral analysis 2',
                'virality_analysis': 'Minimal reach...'
            }
        ])
    
    def test_initialization(self, sample_data):
        """Test that HypothesisEngine initializes correctly"""
        engine = HypothesisEngine(sample_data)
        
        assert engine is not None
        assert len(engine.df) == 4
        assert len(engine.viral_df) == 2
        assert len(engine.non_viral_df) == 2
    
    def test_compare_observations_returns_dict(self, sample_data):
        """Test that compare_observations returns a dictionary"""
        engine = HypothesisEngine(sample_data)
        result = engine.compare_viral_vs_non_viral_observations()
        
        assert isinstance(result, dict)
        assert 'viral_patterns_observed' in result
        assert 'non_viral_gaps_identified' in result
        assert 'metrics_comparison' in result
        print(f"\n✅ Comparative observations generated with {len(result['viral_patterns_observed'])} patterns")
    
    def test_generate_hypotheses_returns_dict(self, sample_data):
        """Test that hypothesis generation returns a dictionary"""
        engine = HypothesisEngine(sample_data)
        result = engine.generate_cross_category_hypothesis()
        
        assert isinstance(result, dict)
        assert 'hypotheses' in result
        assert isinstance(result['hypotheses'], list)
        assert len(result['hypotheses']) > 0
        print(f"\n✅ Generated {len(result['hypotheses'])} hypotheses")
    
    def test_generate_ad_formats_returns_dict(self, sample_data):
        """Test that ad format generation returns a dictionary"""
        engine = HypothesisEngine(sample_data)
        hypotheses = engine.generate_cross_category_hypothesis()
        result = engine.generate_ad_formats_suggestions(hypotheses)
        
        assert isinstance(result, dict)
        assert 'ad_formats' in result
        assert isinstance(result['ad_formats'], list)
        assert len(result['ad_formats']) > 0
        print(f"\n✅ Generated {len(result['ad_formats'])} ad format suggestions")
    
    def test_get_summary_stats(self, sample_data):
        """Test summary statistics calculation"""
        engine = HypothesisEngine(sample_data)
        stats = engine.get_summary_stats()
        
        assert stats['total_posts'] == 4
        assert stats['viral_posts'] == 2
        assert stats['non_viral_posts'] == 2
        assert stats['viral_percentage'] == 50.0
        print(f"\n✅ Stats: {stats}")
    
    def test_mock_data_has_labels(self, sample_data):
        """Test that mock data is properly labeled with [MOCK]"""
        engine = HypothesisEngine(sample_data)
        result = engine.compare_viral_vs_non_viral_observations()
        
        # Check that mock labels exist
        patterns = result['viral_patterns_observed']
        assert any('[MOCK]' in pattern for pattern in patterns)
        print(f"\n✅ Mock labels found in output")


# Run this if executed directly
if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
"""
Dummy/Mock Hypothesis Engine for Testing Pipeline Flow
This is a placeholder implementation that returns mock data
Replace with real implementation later
"""
import pandas as pd
import json
import logging
from typing import Dict, List
from datetime import datetime

logger = logging.getLogger(__name__)


class HypothesisEngine:
    """
    Mock Hypothesis Engine - Returns dummy data for testing
    TODO: Replace with real Gemini-based analysis
    """
    
    def __init__(self, df: pd.DataFrame):
        """
        Initialize with analyzed dataframe
        
        Args:
            df: DataFrame with columns: post_id, viral, video_analysis, virality_analysis
        """
        self.df = df
        logger.info(f"🧪 [MOCK] HypothesisEngine initialized with {len(df)} records")
        
        # Separate viral vs non-viral
        self.viral_df = df[df.get('viral', False) == True]
        self.non_viral_df = df[df.get('viral', False) == False]
        
        logger.info(f"  📊 Viral: {len(self.viral_df)}, Non-viral: {len(self.non_viral_df)}")
    
    def compare_viral_vs_non_viral_observations(self) -> Dict:
        """
        Compare viral vs non-viral content observations
        
        Returns:
            Dictionary with comparative analysis (MOCK DATA)
        """
        logger.info("🔬 [MOCK] Generating comparative observations...")
        
        # Calculate basic stats
        viral_avg_views = self.viral_df.get('Views', pd.Series([0])).mean() if len(self.viral_df) > 0 else 0
        non_viral_avg_views = self.non_viral_df.get('Views', pd.Series([0])).mean() if len(self.non_viral_df) > 0 else 0
        
        viral_avg_likes = self.viral_df.get('Likes', pd.Series([0])).mean() if len(self.viral_df) > 0 else 0
        non_viral_avg_likes = self.non_viral_df.get('Likes', pd.Series([0])).mean() if len(self.non_viral_df) > 0 else 0
        
        mock_comparison = {
            "timestamp": datetime.now().isoformat(),
            "analysis_type": "comparative_observations",
            "sample_size": {
                "viral_count": len(self.viral_df),
                "non_viral_count": len(self.non_viral_df),
                "total": len(self.df)
            },
            "metrics_comparison": {
                "viral": {
                    "avg_views": float(viral_avg_views),
                    "avg_likes": float(viral_avg_likes),
                    "avg_engagement_rate": float((viral_avg_likes / max(viral_avg_views, 1)) * 100)
                },
                "non_viral": {
                    "avg_views": float(non_viral_avg_views),
                    "avg_likes": float(non_viral_avg_likes),
                    "avg_engagement_rate": float((non_viral_avg_likes / max(non_viral_avg_views, 1)) * 100)
                },
                "viral_lift": {
                    "views_multiplier": float(viral_avg_views / max(non_viral_avg_views, 1)),
                    "likes_multiplier": float(viral_avg_likes / max(non_viral_avg_likes, 1))
                }
            },
            "viral_patterns_observed": [
                "🔥 [MOCK] Strong hook in first 3 seconds",
                "🔥 [MOCK] High-energy presentation style",
                "🔥 [MOCK] Emotional appeal or humor",
                "🔥 [MOCK] Clear call-to-action",
                "🔥 [MOCK] Trending audio or format"
            ],
            "non_viral_gaps_identified": [
                "⚠️ [MOCK] Slow or unclear opening",
                "⚠️ [MOCK] Low production quality",
                "⚠️ [MOCK] Lack of emotional engagement",
                "⚠️ [MOCK] No clear value proposition",
                "⚠️ [MOCK] Generic or oversaturated content"
            ],
            "key_differentiators": [
                "📌 [MOCK] Viral content has 3.2x higher engagement in first 5 seconds",
                "📌 [MOCK] Viral creators use dynamic camera angles 2x more often",
                "📌 [MOCK] Viral content features human faces in thumbnails 78% of time",
                "📌 [MOCK] Viral videos maintain viewer attention 47% longer"
            ],
            "_note": "⚠️ THIS IS MOCK DATA - Replace with real Gemini analysis"
        }
        
        logger.info(f"✅ [MOCK] Generated comparative report with {len(mock_comparison['viral_patterns_observed'])} patterns")
        return mock_comparison
    
    def generate_cross_category_hypothesis(self) -> Dict:
        """
        Generate cross-category hypotheses from observations
        
        Returns:
            Dictionary with hypotheses (MOCK DATA)
        """
        logger.info("💡 [MOCK] Generating cross-category hypotheses...")
        
        mock_hypotheses = {
            "timestamp": datetime.now().isoformat(),
            "analysis_type": "cross_category_hypotheses",
            "hypotheses": [
                {
                    "id": "H1",
                    "hypothesis": "[MOCK] Videos with emotional hooks in the first 3 seconds achieve 5x higher viral potential regardless of category",
                    "confidence_level": "High",
                    "supporting_evidence": [
                        "92% of viral videos analyzed had clear emotional triggers within first 3 seconds",
                        "Non-viral videos averaged 8.2 seconds before emotional engagement",
                        "Cross-category pattern observed in beauty, tech, and lifestyle content"
                    ],
                    "test_recommendation": "A/B test ads with emotional hook vs. product-first opening",
                    "expected_impact": "25-40% increase in engagement rate"
                },
                {
                    "id": "H2",
                    "hypothesis": "[MOCK] User-generated content style outperforms polished production in authentic engagement",
                    "confidence_level": "Medium",
                    "supporting_evidence": [
                        "UGC-style videos had 2.3x higher comment rates",
                        "Overly polished content perceived as 'too ad-like'",
                        "Trend consistent across Gen Z and Millennial audiences"
                    ],
                    "test_recommendation": "Create 'raw' version of polished ad creative",
                    "expected_impact": "15-30% increase in shareability"
                },
                {
                    "id": "H3",
                    "hypothesis": "[MOCK] Problem-solution narrative structure drives higher completion rates",
                    "confidence_level": "High",
                    "supporting_evidence": [
                        "Viral videos using problem-solution format had 87% avg completion rate",
                        "Linear product showcase videos averaged 34% completion",
                        "Clear before/after demonstration resonates universally"
                    ],
                    "test_recommendation": "Restructure ads to lead with relatable problem",
                    "expected_impact": "40-60% improvement in watch time"
                },
                {
                    "id": "H4",
                    "hypothesis": "[MOCK] Trending audio increases discoverability by 3-4x regardless of visual content quality",
                    "confidence_level": "Medium-High",
                    "supporting_evidence": [
                        "78% of viral videos used trending audio",
                        "Algorithm prioritizes content with popular audio tracks",
                        "Audio-driven discovery accounts for 42% of viral reach"
                    ],
                    "test_recommendation": "Monitor trending audio and adapt creative quickly",
                    "expected_impact": "200-300% increase in organic reach"
                }
            ],
            "universal_principles": [
                "🎯 [MOCK] Attention must be captured within 1-3 seconds",
                "🎯 [MOCK] Authenticity trumps production quality",
                "🎯 [MOCK] Emotional resonance > Product features",
                "🎯 [MOCK] Platform-native format > Repurposed content",
                "🎯 [MOCK] Clear value proposition in first frame"
            ],
            "_note": "⚠️ THIS IS MOCK DATA - Replace with real Gemini-generated hypotheses"
        }
        
        logger.info(f"✅ [MOCK] Generated {len(mock_hypotheses['hypotheses'])} hypotheses")
        return mock_hypotheses
    
    def generate_ad_formats_suggestions(self, hypotheses: Dict) -> Dict:
        """
        Generate specific ad format recommendations based on hypotheses
        
        Args:
            hypotheses: Output from generate_cross_category_hypothesis()
            
        Returns:
            Dictionary with ad format suggestions (MOCK DATA)
        """
        logger.info("📝 [MOCK] Generating ad format suggestions...")
        
        mock_ad_formats = {
            "timestamp": datetime.now().isoformat(),
            "analysis_type": "ad_format_recommendations",
            "based_on_hypotheses": [h.get('id', 'unknown') for h in hypotheses.get('hypotheses', [])],
            "ad_formats": [
                {
                    "format_id": "AF1",
                    "format_name": "[MOCK] Hook-First Emotional Reel",
                    "platform": "Instagram Reels",
                    "duration_seconds": 15,
                    "structure": {
                        "0-3s": "Emotional hook or shocking statement (e.g., 'I wasted $500 until I found this...')",
                        "3-8s": "Quick problem demonstration with relatable pain point",
                        "8-12s": "Product reveal and solution showcase",
                        "12-15s": "Clear CTA with urgency element"
                    },
                    "creative_elements": [
                        "Close-up face shot in first frame",
                        "Text overlay with provocative question",
                        "Trending audio (check TikTok Creative Center)",
                        "Before/after visual transition",
                        "On-screen text for sound-off viewing"
                    ],
                    "script_template": "HOOK: 'Stop! I wish I knew this before spending $$$ on [problem]...' → PROBLEM: Show frustration/pain → SOLUTION: 'Then I found [product]' → RESULT: Happy customer moment → CTA: 'Link in bio for 20% off!'",
                    "inspiration_from": "Viral pattern H1 - Emotional hooks",
                    "expected_performance": "3-5x baseline engagement"
                },
                {
                    "format_id": "AF2",
                    "format_name": "[MOCK] UGC-Style Testimonial",
                    "platform": "Instagram/TikTok",
                    "duration_seconds": 20,
                    "structure": {
                        "0-5s": "Authentic unboxing or first impression",
                        "5-12s": "Real-time product testing with genuine reaction",
                        "12-17s": "Honest pros/cons discussion",
                        "17-20s": "Natural recommendation to friend/audience"
                    },
                    "creative_elements": [
                        "Selfie-style handheld camera",
                        "Natural lighting (no studio setup)",
                        "Casual, conversational tone",
                        "Minimal editing - jump cuts only",
                        "Real customer or nano-influencer"
                    ],
                    "script_template": "OPEN: 'Okay so I just got this and...' → TEST: Actually use product on camera → REACT: Genuine surprise/delight → SHARE: 'Honestly didn't expect this but...' → RECOMMEND: 'Definitely trying again'",
                    "inspiration_from": "Viral pattern H2 - UGC style",
                    "expected_performance": "2-3x higher trust signals"
                },
                {
                    "format_id": "AF3",
                    "format_name": "[MOCK] Problem-Solution Story",
                    "platform": "Instagram Reels/Stories",
                    "duration_seconds": 25,
                    "structure": {
                        "0-5s": "Establish relatable problem with exaggeration",
                        "5-10s": "Show failed attempts or frustration",
                        "10-18s": "Introduce solution and demonstrate",
                        "18-22s": "Show transformation/results",
                        "22-25s": "CTA with limited-time offer"
                    },
                    "creative_elements": [
                        "Split-screen before/after",
                        "Expressive facial reactions",
                        "Fast-paced transitions",
                        "Visual progress indicators",
                        "Price/value proposition overlay"
                    ],
                    "script_template": "PROBLEM: 'Anyone else struggling with [pain]?' → STRUGGLE: Show multiple failed solutions → DISCOVERY: 'Until I found this...' → DEMO: Quick product demonstration → TRANSFORMATION: 'Now I can finally...' → OFFER: 'Get yours 30% off'",
                    "inspiration_from": "Viral pattern H3 - Problem-solution narrative",
                    "expected_performance": "40-60% higher completion rate"
                }
            ],
            "creative_briefs": [
                {
                    "brief_id": "CB1",
                    "brief_name": "[MOCK] Emotional Hook Campaign",
                    "objective": "Drive awareness and consideration through emotional engagement",
                    "target_audience": "25-40 year olds, problem-aware, active on social",
                    "must_have_elements": [
                        "Emotional hook in first 3 seconds",
                        "Close-up of human face",
                        "Trending audio track",
                        "Clear problem-solution narrative",
                        "Authentic, non-scripted feel"
                    ],
                    "avoid_elements": [
                        "No corporate branding in first 5 seconds",
                        "No stock footage",
                        "No overly polished production",
                        "No talking heads with scripts",
                        "No hard sales language"
                    ],
                    "kpis": {
                        "primary": "Hook rate (3-sec view rate) > 65%",
                        "secondary": ["Completion rate > 45%", "CTA click rate > 5%"]
                    },
                    "example_script": "HOOK: 'I can't believe I'm about to show you this...' [shocked face close-up] → 'So I've been dealing with [problem] for YEARS' [show frustration] → 'Then my friend sent me this video' [transition] → 'And honestly? Game changer.' [show product] → 'Link in bio if you need this in your life' [genuine smile]"
                }
            ],
            "testing_framework": {
                "phase_1_discovery": "Test 3-5 variations of hook styles with same product",
                "phase_2_validation": "Scale winning hook format across product line",
                "phase_3_optimization": "A/B test CTA placement and audio choices",
                "success_metrics": ["Hook rate >60%", "Engagement rate >8%", "ROAS >3.5x"]
            },
            "_note": "⚠️ THIS IS MOCK DATA - Replace with real Gemini-generated ad formats"
        }
        
        logger.info(f"✅ [MOCK] Generated {len(mock_ad_formats['ad_formats'])} ad format recommendations")
        return mock_ad_formats
    
    def get_summary_stats(self) -> Dict:
        """
        Get summary statistics for logging/debugging
        
        Returns:
            Dictionary with summary stats
        """
        return {
            "total_posts": len(self.df),
            "viral_posts": len(self.viral_df),
            "non_viral_posts": len(self.non_viral_df),
            "viral_percentage": (len(self.viral_df) / len(self.df) * 100) if len(self.df) > 0 else 0
        }
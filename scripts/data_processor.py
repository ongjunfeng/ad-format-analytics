"""
Data processing and analysis utilities for TikTok scraping results.
"""
import json
import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging

from apify_client import ScrapingResult

# Setup logging
logger = logging.getLogger(__name__)

def ensure_data_directory() -> Path:
    """Ensure data directory exists and return path."""
    data_dir = Path(os.getenv("TIKTOK_DATA_DIR", "data"))
    data_dir.mkdir(exist_ok=True)
    
    # Create subdirectories
    (data_dir / "raw").mkdir(exist_ok=True)
    (data_dir / "processed").mkdir(exist_ok=True)
    (data_dir / "analysis").mkdir(exist_ok=True)
    
    return data_dir

def save_scraping_results(result: ScrapingResult, filename_prefix: str) -> Dict[str, Path]:
    """
    Save scraping results in multiple formats.
    
    Args:
        result: ScrapingResult object
        filename_prefix: Prefix for output files
        
    Returns:
        Dictionary with format -> file path mappings
    """
    data_dir = ensure_data_directory()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Generate filenames
    json_file = data_dir / "raw" / f"{filename_prefix}_{timestamp}.json"
    csv_file = data_dir / "raw" / f"{filename_prefix}_{timestamp}.csv"
    metadata_file = data_dir / "raw" / f"{filename_prefix}_{timestamp}_metadata.json"
    
    saved_files = {}
    
    try:
        # Save raw JSON data
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(result.data, f, indent=2, ensure_ascii=False)
        saved_files['json'] = json_file
        logger.info(f"💾 Saved JSON data: {json_file}")
        
        # Save as CSV if data exists
        if result.data:
            df = pd.json_normalize(result.data)
            df.to_csv(csv_file, index=False, encoding='utf-8')
            saved_files['csv'] = csv_file
            logger.info(f"💾 Saved CSV data: {csv_file}")
        
        # Save metadata
        metadata = {
            "tier": result.tier,
            "timestamp": result.timestamp.isoformat(),
            "total_items": result.total_items,
            "execution_time": result.execution_time,
            "files": {
                "json": str(json_file),
                "csv": str(csv_file) if 'csv' in saved_files else None
            }
        }
        
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)
        saved_files['metadata'] = metadata_file
        logger.info(f"💾 Saved metadata: {metadata_file}")
        
        return saved_files
        
    except Exception as e:
        logger.error(f"❌ Failed to save results: {str(e)}")
        raise

def analyze_engagement_metrics(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Analyze engagement metrics from TikTok data.
    
    Args:
        data: List of video data dictionaries
        
    Returns:
        Dictionary with engagement analysis results
    """
    if not data:
        return {"error": "No data provided"}
    
    # Convert to DataFrame for easier analysis
    df = pd.json_normalize(data)
    
    # Calculate engagement metrics
    engagement_cols = ['diggCount', 'shareCount', 'commentCount', 'playCount']
    available_cols = [col for col in engagement_cols if col in df.columns]
    
    if not available_cols:
        return {"error": "No engagement metrics found in data"}
    
    analysis = {
        "total_videos": len(df),
        "metrics": {}
    }
    
    # Basic statistics for each metric
    for col in available_cols:
        if col in df.columns:
            analysis["metrics"][col] = {
                "mean": float(df[col].mean()) if not df[col].isna().all() else 0,
                "median": float(df[col].median()) if not df[col].isna().all() else 0,
                "max": float(df[col].max()) if not df[col].isna().all() else 0,
                "min": float(df[col].min()) if not df[col].isna().all() else 0,
                "std": float(df[col].std()) if not df[col].isna().all() else 0
            }
    
    # Top performing videos (by likes if available)
    if 'diggCount' in df.columns:
        top_videos = df.nlargest(10, 'diggCount')[['webVideoUrl', 'diggCount', 'text']].to_dict('records')
        analysis["top_videos"] = top_videos
    
    # Engagement rate calculation (if play count available)
    if 'diggCount' in df.columns and 'playCount' in df.columns:
        df['engagement_rate'] = df['diggCount'] / df['playCount'].replace(0, 1)
        analysis["engagement_rate"] = {
            "mean": float(df['engagement_rate'].mean()),
            "median": float(df['engagement_rate'].median())
        }
    
    return analysis

def extract_content_patterns(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Extract content patterns from TikTok video data.
    
    Args:
        data: List of video data dictionaries
        
    Returns:
        Dictionary with content pattern analysis
    """
    if not data:
        return {"error": "No data provided"}
    
    df = pd.json_normalize(data)
    
    patterns = {
        "total_videos": len(df),
        "content_analysis": {}
    }
    
    # Text analysis if available
    if 'text' in df.columns:
        texts = df['text'].dropna()
        
        # Common words/phrases (simple analysis)
        all_text = ' '.join(texts.str.lower())
        words = all_text.split()
        
        from collections import Counter
        common_words = Counter(words).most_common(20)
        patterns["content_analysis"]["common_words"] = common_words
        
        # Hashtag analysis
        hashtags = []
        for text in texts:
            hashtags.extend([word for word in str(text).split() if word.startswith('#')])
        
        if hashtags:
            common_hashtags = Counter(hashtags).most_common(10)
            patterns["content_analysis"]["common_hashtags"] = common_hashtags
    
    # Creator analysis
    if 'author.nickname' in df.columns:
        top_creators = df['author.nickname'].value_counts().head(10).to_dict()
        patterns["top_creators"] = top_creators
    
    # Duration analysis
    if 'video.duration' in df.columns:
        patterns["duration_stats"] = {
            "mean": float(df['video.duration'].mean()),
            "median": float(df['video.duration'].median()),
            "min": float(df['video.duration'].min()),
            "max": float(df['video.duration'].max())
        }
    
    return patterns

def generate_tier_comparison_report(tier_results: Dict[str, ScrapingResult]) -> Dict[str, Any]:
    """
    Generate comparison report across tiers.
    
    Args:
        tier_results: Dictionary mapping tier names to ScrapingResult objects
        
    Returns:
        Comprehensive comparison report
    """
    report = {
        "comparison_summary": {},
        "tier_analysis": {},
        "cross_tier_insights": {}
    }
    
    # Summary comparison
    for tier, result in tier_results.items():
        engagement = analyze_engagement_metrics(result.data)
        patterns = extract_content_patterns(result.data)
        
        report["comparison_summary"][tier] = {
            "total_videos": result.total_items,
            "execution_time": result.execution_time,
            "avg_engagement": engagement.get("metrics", {}).get("diggCount", {}).get("mean", 0)
        }
        
        report["tier_analysis"][tier] = {
            "engagement_metrics": engagement,
            "content_patterns": patterns
        }
    
    # Cross-tier insights
    all_data = []
    for result in tier_results.values():
        all_data.extend(result.data)
    
    report["cross_tier_insights"] = {
        "total_videos_all_tiers": len(all_data),
        "overall_engagement": analyze_engagement_metrics(all_data),
        "overall_patterns": extract_content_patterns(all_data)
    }
    
    return report

def save_analysis_report(report: Dict[str, Any], filename: str) -> Path:
    """Save analysis report to file."""
    data_dir = ensure_data_directory()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    report_file = data_dir / "analysis" / f"{filename}_{timestamp}.json"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    logger.info(f"📊 Analysis report saved: {report_file}")
    return report_file
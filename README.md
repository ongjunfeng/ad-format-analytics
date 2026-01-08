# Ad Format Analytics

A Python application for collecting and analyzing social media video data to understand content performance and audience engagement patterns.

## 🎯 Project Overview

This analytics pipeline provides a flexible framework for collecting and analyzing video content data using configurable query tiers and processing workflows.

## 🚀 Quick Start

1. **Setup Environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Configure API Access**
   ```bash
   cp .env.example .env
   # Edit .env and add your API credentials
   ```

3. **Run Analysis**
   ```bash
   python scripts/main.py
   ```

## 📊 Usage Examples

```bash
# Complete pipeline
python scripts/main.py

# Quick analysis
python scripts/main.py --quick

# Single tier analysis
python scripts/main.py --tier tier1 --max-items 500

# Custom limits
python scripts/main.py --max-items 100
```

## 📁 Project Structure

```
scripts/
├── main.py              # Main orchestrator script
├── tier_scraper.py      # Tier-based scraping functions
├── apify_client.py      # API client wrapper
├── data_processor.py    # Data processing utilities
└── config.py           # Configuration management

datasets/                # Collected datasets
```

## 🎛️ Configuration

The application uses configurable query tiers that can be customized in `scripts/config.py`:

- **Tier 1**: Broad content queries
- **Tier 2**: Bridge content queries  
- **Tier 3**: Niche-specific queries

## 📈 Analysis Features

- **Engagement Metrics**: Comprehensive engagement analysis
- **Content Pattern Analysis**: Text, hashtag, and creator insights
- **Cross-Tier Comparison**: Multi-tier analysis and reporting
- **Export Formats**: JSON, CSV, and analysis reports

## 🔧 Requirements

- API access for data collection
- Python 3.7+
- Dependencies listed in `requirements.txt`

## 🎯 Use Cases

- **Content Research**: Analyze high-performing content formats
- **Audience Analysis**: Understand engagement patterns across segments
- **Competitive Intelligence**: Study successful content strategies
- **Performance Optimization**: Data-driven content insights
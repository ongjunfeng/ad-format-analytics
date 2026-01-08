# Instagram Reels ETL Pipeline

This directory contains an ETL (Extract, Transform, Load) pipeline designed to analyze Instagram Reels content, specifically focused on cat-related content. The pipeline is designed to be run in a Databricks environment.

## Overview

The pipeline performs the following steps:
1. Scrapes Instagram Reels data using Apify
2. Labels content as viral based on engagement metrics
3. Downloads Instagram Reels videos
4. Analyzes video content using Google's Gemini AI

## Key Components

- `etl_pipeline.ipynb`: Main pipeline notebook containing the ETL workflow
- `apify_service.py`: Handles Instagram data scraping via Apify
- `label_data_as_viral.py`: Contains logic for identifying viral content
- `video_processor.py`: Manages video download and processing
- `gemini_service.py`: Interfaces with Google's Gemini AI for video analysis
- `config.py`: Configuration settings and constants
- `*.json`: Scraping configuration files

## Prerequisites

- Python 3.x
- Databricks environment
- Required Python packages (see `requirements.txt`)
- Environment variables:
  - Apify API credentials
  - Gemini API credentials
  - (Other required credentials - see `.env.example`)

## Installation

1. Clone this directory to your Databricks workspace
2. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and fill in your credentials

## Pipeline Flow

### 1. Data Ingestion
- Primary Source: Apify scraping for Instagram Reels data  
- Data Preprocessing: Column dropping and renaming for downstream compatibility  

**Take note:**  
The mapping between the raw Apify columns and the cleaned transformed column names is hardcoded in the `config.py` file. All columns that are not inside of the mapping are dropped.


### 2. Viral Content Labeling
- Applies to both cat content and ad data
- Ad data pipeline terminates after this step  

TODO: Virality function update (Jiang En and Jun Feng)


### 3. Video Download Process
- Primary Method: Direct download via Apify-provided media_url
- Fallback Method: Instaloader (known issues with Databricks)
- Timing Consideration: Same-day download recommended  

**Take note:**  
Instagram uses a Content Delivery Network (CDN) for web content delivery. This media URL typically changes, and would expire within days. Apify retrieves the CDN media URL for an Instagram post, and there should be no issues accessing it within the same day. Instaloader is used as a fallback if the CDN media URL somehow expires by retrieving the latest media URL. There may be some manual work needed to set up Instaloader on databricks by creating the Instaloader login account session file and uploading it to Databricks.

### 4. Video Analysis
- Tool: Google's Gemini AI
- Outputs: Video analysis and hypothesis generation

### 5. Saving output
- Pandas dataframe converted to Spark dataframe
- Spark dataframe writes to Delta table stored in `workspace.test`

## Current Limitations and Future Improvements

### Data Collection
- **Configuration Management**
  - Current: Static account list in `config.py` (IG_REEL_SCRAPING_CONFIG)
  - Needed: Automated weekly rotation of cat-related accounts
 
  <!-- Q: Do you have a list of potential accounts to rotate through? -->
  <!-- Q: What criteria would be used to select new accounts? -->

### Pipeline Scheduling
- **Current Schedule**: 12AM every Monday
- **Performance Considerations**:
  - Long processing times due to Gemini video analysis
  - Potential Instaloader delays
- **Proposed Optimization**:
  - More frequent, smaller batch processing
  - Options: Daily or bi-daily runs with reduced data volume
  <!-- Q: What would be an ideal batch size for more frequent runs? -->
  <!-- Q: Have you measured average processing time per video? -->

### Technical Challenges
- **Gemini API Reliability**
  - Issue: Occasional 503 errors (model overload)
  - Reference: [StackOverflow Discussion](https://stackoverflow.com/questions/78154047/encountering-503-error-when-calling-gemini-api-from-google-colab)
  - Needed: Robust error handling and retry mechanism

- **Instaloader**
  - Issue: Login needed for Instaloader
  - Solution: Find a way to load a session file into Databricks. Best if automatic
  - Notes: Lower priority, our pipeline might not need it
  <!-- Q: What retry strategy would be most appropriate for your use case? -->
  <!-- Q: Is there a backup analysis method if Gemini fails repeatedly? -->

### Virality
- **Virality Labelling**
  - Implement new virality labelling done by Jiang En and Jun Feng
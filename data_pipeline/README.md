

# Data Pipeline

This package contains the data pipeline for processing instagram reels for viral ad analysis. It orchestrates the entire workflow from scraping to downloading of videos, post-processing, etc. The scraping makes use of the Apify API. The main entry point is `main.py`, which details the pipeline steps.

<!-- TODO: Add a high-level diagram or description of the pipeline stages (e.g., scraping, cleaning, saving, analysis) -->
On a high level, the pipeline will ingest some data based on the scraping config set up. The instagram reels url likes are processed and uploaded to an S3 bucket for further analysis.

## 🎯 Project Overview

The `data_pipeline` package provides a modular pipeline for ingesting, processing, and saving social media data. The pipeline is configured via `config.py` and executed through `main.py`. Scraping is performed using the Apify API, with results converted to a tabular format as a pandas dataframe for processing. Right now, the url links will be processed and uploaded to an s3 bucket.

**Key components:**
- `main.py`: Orchestrates the pipeline (scraping, saving, etc.)
- `config.py`: Defines scraping parameters and pipeline configuration
- `data_ingestion.py`: Contains the Apify API wrapper and scraping logic
- `scraping_result.py`: Defines the format that the data scraped from Apify should take. Can be converted to pandas Dataframe.
- `video_processor.py`: Contains functions responsible for parsing instagram reel urls, downloading and uploading them to s3.


## 🚀 Quick Start

1. **Setup Environment**
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Configure API Access**
   - Create an `.env` file from the given `.env.example` file
   - Add your Apify API key to a `.env` file as `APIFY_API_TOKEN`. 

3. **Set Up Pipeline Config**
   - Edit `.env` with the relevant config info
   - Edit `config.py` to define your scraping and pipeline parameters using the `ScrapingConfig` dataclass.

4. **Run the Pipeline**
   ```bash
   python main.py
   ```

## 📊 Usage Example

The pipeline is designed to be run from `main.py`. Example:

```bash
python main.py
```

TODO: Will be working on automated scheduling for the pipeline.

You can customize the scraping and processing logic by editing `config.py` and updating `main.py` accordingly.

## 📁 Package Structure

```
data_pipeline/
├── config.py                  # ScrapingConfig dataclass and pipeline configs
├── scraping_result.py         # ScrapingResult class (defines response format and methods)
├── data_scraper.py            # DataScraper class (Apify API wrapper)
├── video_processor.py         # Contains methods to process url and upload to s3
├── main.py                    # Pipeline orchestrator
├── reel_scraper_config.json   # JSON config for reel scraper used in Apify
└── README.md                  # This file

datasets/               # Output data (JSON, CSV, metadata)
```

## 🎛️ Configuration

Define pipeline and scraping parameters in `config.py` using the `ScrapingConfig` dataclass.  
Note that the input_parameters need to be a dictionary format that is the same as the Apify actor input.  
Example:

```python
IG_REELS_EXAMPLE = ScrapingConfig(
   actor_name="apify/instagram-reel-scraper",
   config_name="instagram_reels",
   platform="instagram",
   input_parameters={
      "usernames": [
         "cats_of_instagram"
      ]
   }
)
```

## Features

- **Pipeline Orchestration**: Runs the full data workflow from scraping to saving
- **Apify API Integration**: Uses Apify actors for robust scraping
- **Configurable**: Easily set up scraping and pipeline parameters
- **Multi-format Saving**: Automatically saves results as JSON, CSV, and metadata
- **Extensible**: Add new pipeline steps or post-processing as needed


## 🔧 Requirements

- Apify API access (`APIFY_API_TOKEN` in `.env`)
- Python 3.9+
- Dependencies in `requirements.txt`

## 🚧 Development Status

This package is in early development. More features and robustness will be added over time. Feedback and contributions are welcome!

## 🎯 Use Cases

- **Social Media Research**: Collect and analyze platform-specific data
- **Content Strategy**: Gather insights for content optimization
- **Data Export**: Save results for further analysis in JSON/CSV

---

## ❓ Questions for Further Documentation

- What platforms are supported by the pipeline? (e.g., Reddit, TikTok, Twitter, etc.)
- What are the required inputs and outputs for each pipeline stage?
- How can users customize or extend the pipeline (e.g., add new processing steps)?
- What error handling and logging features are available?
- Are there any scheduling or automation options?
- How is data quality and integrity ensured?
- What are the recommended best practices for using this pipeline?
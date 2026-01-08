# Databricks notebook source
import logging
from gemini_service import analyze_video
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# COMMAND ----------

# Read previous task's taskValues
silver_path = dbutils.jobs.taskValues.get(
    taskKey="02_silver_label_and_upload", 
    key="silver_path",
    defaultValue="workspace.test.nus_silver_instagram_transformed"
)
dataset_id = dbutils.jobs.taskValues.get(
    taskKey="02_silver_label_and_upload", 
    key="dataset_id",
    defaultValue="91qIDxbbk340bcKdW"
)

# Load silver data
silver_df = spark.table(silver_path)
silver_df = silver_df.filter(silver_df.dataset_id == dataset_id)

# COMMAND ----------

# Run Gemini analysis
logger.info(f"Running Gemini analysis for dataset_id={dataset_id}")
pdf = silver_df.toPandas()
# TODO: Implement the sampling of full dataset (XM)
sample_df = pdf.head(20)
analysis_df = analyze_video(sample_df)  # add columns like sentiment, topic, etc.
analysis_df

# COMMAND ----------

# Save final table
gold_path = "workspace.test.nus_viral_video_analysis_temp"
spark_df = spark.createDataFrame(analysis_df)
spark_df.write.format("delta").mode("append").saveAsTable(gold_path)

logger.info(f"Gold table written to {gold_path} for dataset_id={dataset_id}")
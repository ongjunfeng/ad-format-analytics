# Databricks notebook source
import logging
from label_data_as_viral import compute_engagement
from video_processor import VideoProcessor
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# COMMAND ----------

# Retrieve bronze table info with debugValue fallback
bronze_path = dbutils.jobs.taskValues.get(
    taskKey="01_bronze_apify_ingest",
    key="bronze_path",
    debugValue="workspace.test.nus_bronze_instagram_ingested"
)
dataset_id = dbutils.jobs.taskValues.get(
    taskKey="01_bronze_apify_ingest",
    key="dataset_id",
    debugValue="91qIDxbbk340bcKdW"
)

logger.info(f"Processing dataset_id={dataset_id}")

# Load bronze data
bronze_df = spark.table("workspace.test.nus_bronze_instagram_ingested")
bronze_df = bronze_df.filter(bronze_df.dataset_id == dataset_id)
display(bronze_df)

# COMMAND ----------

# Convert to Pandas for business logic
pdf = bronze_df.toPandas()
logger.info(f"🏷️ Running viral labeling process for dataset_id {dataset_id}...")
labelled_df = compute_engagement(pdf)
logger.info(f"✅ Labeling complete.")
logger.info(f"✅ Labeled data with {len(labelled_df)} records.")
labelled_df

# COMMAND ----------

# Upload videos
vp = VideoProcessor()
logger.info(f"📹 Extracting videos for dataset_id {dataset_id}...")
transformed_df = vp.upload_video_df(labelled_df)
transformed_df

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType, LongType, BooleanType, TimestampType
)
from pyspark.sql.functions import col

# Target delta table schema
TARGET_SCHEMA = StructType([
    StructField("dataset_id", StringType(), False),
    StructField("ingested_epoch", DoubleType()),
    StructField("ingested_timestamp", TimestampType()),
    StructField("post_id", StringType()),
    StructField("views", DoubleType()),
    StructField("likes", DoubleType()),
    StructField("comments", DoubleType()),
    StructField("duration", DoubleType()),
    StructField("caption", StringType()),
    StructField("date", DateType()),
    StructField("post_url", StringType()),
    StructField("media_url", StringType()),
    StructField("username", StringType()),
    StructField("post_number", LongType()),
    StructField("avg_last_50", DoubleType()),
    StructField("viral", LongType()),
    StructField("status_code", LongType()),
    StructField("s3_upload_flag", BooleanType())
])

def enforce_schema(df, schema):
    """Casts and selects columns in the DataFrame to match the target schema."""
    select_cols = [
        col(field.name).cast(field.dataType).alias(field.name)
        for field in schema.fields
    ]
    return df.select(*select_cols)

# COMMAND ----------

# Convert to Spark DataFrame and save to Delta
spark_df = spark.createDataFrame(transformed_df)

# Enforce schema and write to target Delta table
silver_path = "workspace.test.nus_silver_instagram_transformed"
silver_df = enforce_schema(spark_df, TARGET_SCHEMA)
spark_df.write.format("delta").mode("append").saveAsTable(silver_path)

# Pass dataset_id down to the last task for reference
dbutils.jobs.taskValues.set(key="silver_path", value=silver_path)
dbutils.jobs.taskValues.set(key="dataset_id", value=dataset_id)

logger.info(f"Silver table written to {silver_path} for dataset_id={dataset_id}")
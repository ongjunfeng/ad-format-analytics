import pandas as pd
from databricks import sql
from config import DatabricksConfig, DATABRICKS_CONFIG
import logging
import os

class DatabricksService:
    """Service to connect and upload data to Databricks."""
    def __init__(self, config: DatabricksConfig):
        self.config = config
        self.table_prefix = f"{config.catalog}.{config.schema}"
        self.logger = logging.getLogger("DatabricksService")

    def _get_connection(self):
        """Establishes a connection to the Databricks SQL endpoint."""
        return sql.connect(
            server_hostname=self.config.host,
            http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}", # Warehouse ID is often required
            access_token=self.config.token,
            catalog=self.config.catalog,
            schema=self.config.schema
        )

    def upload_dataframe(self, df: pd.DataFrame, table_name: str, mode: str = "append"):
        """
        Uploads a Pandas DataFrame to a Databricks Unity Catalog table.
        NOTE: This implementation uses a simple INSERT query via the SQL connector,
        which is slow for large datasets. For production, consider using Databricks
        Delta Lake APIs (e.g., spark.createDataFrame) or CSV/Parquet upload.
        """
        full_table_name = f"`{self.config.catalog}`.`{self.config.schema}`.`{table_name}`"
        self.logger.info(f"Attempting to upload {len(df)} rows to {full_table_name} (Mode: {mode})...")

        if df.empty:
            self.logger.warning(f"DataFrame for {table_name} is empty. Skipping upload.")
            return

        try:
            with self._get_connection() as connection:
                with connection.cursor() as cursor:
                    # 1. Handle Overwrite/Create
                    if mode == "overwrite":
                        cursor.execute(f"DROP TABLE IF EXISTS {full_table_name}")
                        self.logger.info(f"Dropped existing table {full_table_name}.")

                    # Use pandas to_sql for simpler upload if available/configured, 
                    # but since we are using databricks-sql-connector, we do it manually:
                    
                    # For simplicity, we just check if table exists and append/fail
                    # A robust implementation would use a client library for efficient bulk loading.
                    
                    self.logger.warning("Using slow row-by-row insertion. Optimize for production.")
                    
                    # Example of a single row insertion for minimal implementation
                    columns = ", ".join([f"`{col}`" for col in df.columns])
                    placeholders = ", ".join(["%s"] * len(df.columns))
                    insert_query = f"INSERT INTO {full_table_name} ({columns}) VALUES ({placeholders})"

                    # Convert DataFrame to a list of tuples
                    rows = [tuple(row) for row in df.values]
                    
                    # Create table if it doesn't exist (simplistic schema inference)
                    schema_def = ", ".join([f"`{col}` STRING" for col in df.columns])
                    create_table_query = f"CREATE TABLE IF NOT EXISTS {full_table_name} ({schema_def}) USING DELTA"
                    cursor.execute(create_table_query)
                    
                    # Execute batch insert
                    # NOTE: databricks-sql-connector may not support executemany on all endpoints.
                    # This is a conceptual implementation.
                    for row in rows:
                        cursor.execute(insert_query, row)
                    
                    connection.commit()
                    self.logger.info(f"✅ Successfully uploaded {len(df)} records to Databricks table {full_table_name}.")

        except Exception as e:
            self.logger.error(f"❌ Failed to upload DataFrame to Databricks table {table_name}: {e}")
            raise


# NOTE: For the DatabricksService to be available, ensure you install the connector:
# pip install databricks-sql-connector pandas
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
                    self.logger.info(f"Successfully uploaded {len(df)} records to Databricks table {full_table_name}.")

        except Exception as e:
            self.logger.error(f"Failed to upload DataFrame to Databricks table {table_name}: {e}")
            raise

    def get_observations(self, dataset_id: str) -> pd.DataFrame:
        """
        Retrieves observations from nus_gold_instagram_observations table filtered by dataset_id.

        Args:
            dataset_id: The dataset ID to filter by

        Returns:
            pd.DataFrame: DataFrame containing the filtered observations
        """
        table_name = "nus_gold_instagram_observations"
        full_table_name = f"`{self.config.catalog}`.`{self.config.schema}`.`{table_name}`"

        query = f"""
            SELECT *
            FROM {full_table_name}
            WHERE dataset_id = %s
        """

        self.logger.info(f"Querying {full_table_name} for dataset_id: {dataset_id}")

        try:
            with self._get_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(query, (dataset_id,))

                    # Fetch all results
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()

                    # Convert to DataFrame
                    df = pd.DataFrame(rows, columns=columns)

                    self.logger.info(f"Successfully retrieved {len(df)} observations for dataset_id: {dataset_id}")
                    return df

        except Exception as e:
            self.logger.error(f"Failed to query observations for dataset_id {dataset_id}: {e}")
            raise

    def get_hypotheses(self, dataset_id: str) -> pd.DataFrame:
        """
        Retrieves hypotheses from nus_gold_instagram_hypotheses table filtered by dataset_id.

        Args:
            dataset_id: The dataset ID to filter by

        Returns:
            pd.DataFrame: DataFrame containing the filtered hypotheses
        """
        table_name = "nus_gold_instagram_hypotheses"
        full_table_name = f"`{self.config.catalog}`.`{self.config.schema}`.`{table_name}`"

        query = f"""
            SELECT *
            FROM {full_table_name}
            WHERE dataset_id = %s
        """

        self.logger.info(f"Querying {full_table_name} for dataset_id: {dataset_id}")

        try:
            with self._get_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(query, (dataset_id,))

                    # Fetch all results
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()

                    # Convert to DataFrame
                    df = pd.DataFrame(rows, columns=columns)

                    self.logger.info(f"Successfully retrieved {len(df)} hypotheses for dataset_id: {dataset_id}")
                    return df

        except Exception as e:
            self.logger.error(f"Failed to query hypotheses for dataset_id {dataset_id}: {e}")
            raise

    def get_viral_video_analysis(self, dataset_id: str) -> pd.DataFrame:
        """
        Retrieves viral video analysis from nus_viral_video_analysis_temp table filtered by dataset_id.

        Args:
            dataset_id: The dataset ID to filter by

        Returns:
            pd.DataFrame: DataFrame containing the filtered viral video analysis
        """
        table_name = "nus_viral_video_analysis_temp"
        full_table_name = f"`{self.config.catalog}`.`{self.config.schema}`.`{table_name}`"

        query = f"""
            SELECT *
            FROM {full_table_name}
            WHERE dataset_id = %s
        """

        self.logger.info(f"Querying {full_table_name} for dataset_id: {dataset_id}")

        try:
            with self._get_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(query, (dataset_id,))

                    # Fetch all results
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()

                    # Convert to DataFrame
                    df = pd.DataFrame(rows, columns=columns)

                    self.logger.info(f"Successfully retrieved {len(df)} viral video analysis records for dataset_id: {dataset_id}")
                    return df

        except Exception as e:
            self.logger.error(f"Failed to query viral video analysis for dataset_id {dataset_id}: {e}")
            raise

    def get_observations_and_hypotheses(self, dataset_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Retrieves both observations and hypotheses for a given dataset_id.

        Args:
            dataset_id: The dataset ID to filter by

        Returns:
            tuple: (observations_df, hypotheses_df)
        """
        self.logger.info(f"Retrieving observations and hypotheses for dataset_id: {dataset_id}")

        observations_df = self.get_observations(dataset_id)
        hypotheses_df = self.get_hypotheses(dataset_id)

        return observations_df, hypotheses_df


# NOTE: For the DatabricksService to be available, ensure you install the connector:
# pip install databricks-sql-connector pandas
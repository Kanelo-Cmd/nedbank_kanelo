"""
Bronze layer: Ingest raw source data into Delta Parquet tables.

Input paths (read-only mounts — do not write here):
  /data/input/accounts.csv
  /data/input/transactions.jsonl
  /data/input/customers.csv

Output paths (your pipeline must create these directories):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Requirements:
  - Preserve source data as-is; do not transform at this layer.
  - Add an `ingestion_timestamp` column (TIMESTAMP) recording when each
    record entered the Bronze layer. Use a consistent timestamp for the
    entire ingestion run (not per-row).
  - Write each table as a Delta Parquet table (not plain Parquet).
  - Read paths from config/pipeline_config.yaml — do not hardcode paths.
  - All paths are absolute inside the container (e.g. /data/input/accounts.csv).

Spark configuration tip:
  Run Spark in local[2] mode to stay within the 2-vCPU resource constraint.
  Configure Delta Lake using the builder pattern shown in the base image docs.
"""
import os
import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def load_config(config_path: str = "/data/config/pipeline_config.yaml") -> dict:
    """
    Load pipeline configuration from YAML.
    Uses /data/config/pipeline_config.yaml inside Docker.
    Falls back to local config/pipeline_config.yaml for local testing.
    """
    if not os.path.exists(config_path):
        config_path = "config/pipeline_config.yaml"
    with open(config_path, "r") as file:
        return yaml.safe_load(file)
def create_spark_session() -> SparkSession:
    """
    Create Spark session with Delta Lake support.
    local[2] matches the challenge CPU constraint.
    """
    return (
        SparkSession.builder
        .appName("bronze_ingestion")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )
def write_delta(df, output_path: str) -> None:
    """
    Write dataframe to Delta format.
    """
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(output_path)
    )
def run_ingestion():
    # 1. Load pipeline_config.yaml to get input/output paths.
    config = load_config()
    accounts_input = config["input"]["accounts_path"]
    transactions_input = config["input"]["transactions_path"]
    customers_input = config["input"]["customers_path"]
    bronze_output = config["output"]["bronze_path"]
    # 2. Initialise a SparkSession with Delta Lake support (local[2]).
    spark = create_spark_session()
    try:
        # 3. Read accounts.csv → append ingestion_timestamp → write to bronze/accounts/.
        accounts_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(accounts_input)
            .withColumn("ingestion_timestamp", current_timestamp())
        )
        write_delta(accounts_df, f"{bronze_output}/accounts")
        # 4. Read transactions.jsonl → append ingestion_timestamp → write to bronze/transactions/.
        transactions_df = (
            spark.read
            .option("multiLine", "false")
            .json(transactions_input)
            .withColumn("ingestion_timestamp", current_timestamp())
        )
        write_delta(transactions_df, f"{bronze_output}/transactions")
        # 5. Read customers.csv → append ingestion_timestamp → write to bronze/customers/.
        customers_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(customers_input)
            .withColumn("ingestion_timestamp", current_timestamp())
        )
        write_delta(customers_df, f"{bronze_output}/customers")
    finally:
        spark.stop()
if __name__ == "__main__":
    run_ingestion()
# run_ingestion():
    # TODO: Implement Bronze layer ingestion.
    #
    # Suggested steps:
    #   1. Load pipeline_config.yaml to get input/output paths.
    #   2. Initialise a SparkSession with Delta Lake support (local[2]).
    #   3. Read accounts.csv → append ingestion_timestamp → write to bronze/accounts/.
    #   4. Read transactions.jsonl → append ingestion_timestamp → write to bronze/transactions/.
    #   5. Read customers.csv → append ingestion_timestamp → write to bronze/customers/.
    pass

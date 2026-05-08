"""
Silver layer: Clean and conform Bronze tables into validated Silver Delta tables.

Input paths (Bronze layer output — read these, do not modify):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Output paths (your pipeline must create these directories):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Requirements:
  - Deduplicate records within each table on natural keys
    (account_id, transaction_id, customer_id respectively).
  - Standardise data types (e.g. parse date strings to DATE, cast amounts to
    DECIMAL(18,2), normalise currency variants to "ZAR").
  - Apply DQ flagging to transactions:
      - Set dq_flag = NULL for clean records.
      - Set dq_flag to the appropriate issue code for flagged records.
      - Valid codes: ORPHANED_ACCOUNT, DUPLICATE_DEDUPED, TYPE_MISMATCH,
        DATE_FORMAT, CURRENCY_VARIANT, NULL_REQUIRED.
  - At Stage 2, load DQ rules from config/dq_rules.yaml rather than hardcoding.
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.

See output_schema_spec.md §8 for the full list of DQ flag values and their
definitions.
"""
import os
import yaml
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    to_date,
    to_timestamp,
    concat_ws,
    row_number,
    regexp_replace,
    split,
    when,
    lit,
)
from pyspark.sql.types import DecimalType, IntegerType


def load_config(config_path: str = "/data/config/pipeline_config.yaml") -> dict:
    if not os.path.exists(config_path):
        config_path = "config/pipeline_config.yaml"
    with open(config_path, "r") as file:
        return yaml.safe_load(file)
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver_transformation")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )
def deduplicate_latest(df, primary_key: str, order_col: str = "ingestion_timestamp"):
    window_spec = Window.partitionBy(primary_key).orderBy(col(order_col).desc())
    return (
        df.withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )
def clean_accounts(accounts_df):
    accounts_clean = (
        accounts_df
        .select(
            trim(col("account_id")).alias("account_id"),
            trim(col("customer_ref")).alias("customer_ref"),
            upper(trim(col("account_type"))).alias("account_type"),
            upper(trim(col("account_status"))).alias("account_status"),
            to_date(trim(col("open_date"))).alias("open_date"),
            upper(trim(col("product_tier"))).alias("product_tier"),
            regexp_replace(trim(col("mobile_number")), r"\s+", "").alias("mobile_number"),
            upper(trim(col("digital_channel"))).alias("digital_channel"),
            col("credit_limit").cast(DecimalType(18, 2)).alias("credit_limit"),
            col("current_balance").cast(DecimalType(18, 2)).alias("current_balance"),
            to_date(trim(col("last_activity_date"))).alias("last_activity_date"),
            col("ingestion_timestamp"),
        )
        .filter(col("account_id").isNotNull())
    )
    return deduplicate_latest(accounts_clean, "account_id")
def clean_customers(customers_df):
    customers_clean = (
        customers_df
        .select(
            trim(col("customer_id")).alias("customer_id"),
            trim(col("id_number")).alias("id_number"),
            trim(col("first_name")).alias("first_name"),
            trim(col("last_name")).alias("last_name"),
            to_date(trim(col("dob"))).alias("dob"),
            upper(trim(col("gender"))).alias("gender"),
            trim(col("province")).alias("province"),
            upper(trim(col("income_band"))).alias("income_band"),
            upper(trim(col("segment"))).alias("segment"),
            col("risk_score").cast(IntegerType()).alias("risk_score"),
            upper(trim(col("kyc_status"))).alias("kyc_status"),
            trim(col("product_flags")).alias("product_flags"),
            col("ingestion_timestamp"),
        )
        .filter(col("customer_id").isNotNull())
    )
    return deduplicate_latest(customers_clean, "customer_id")
def clean_transactions(transactions_df):
    transactions_clean = (
        transactions_df
        .select(
            trim(col("transaction_id")).alias("transaction_id"),
            trim(col("account_id")).alias("account_id"),
            to_date(trim(col("transaction_date"))).alias("transaction_date"),
            trim(col("transaction_time")).alias("transaction_time"),
            to_timestamp(
                concat_ws(" ", trim(col("transaction_date")), trim(col("transaction_time")))
            ).alias("transaction_timestamp"),
            upper(trim(col("transaction_type"))).alias("transaction_type"),
            upper(trim(col("merchant_category"))).alias("merchant_category"),
            col("amount").cast(DecimalType(18, 2)).alias("amount"),
            upper(trim(col("currency"))).alias("currency"),
            upper(trim(col("channel"))).alias("channel"),
            trim(col("location.province")).alias("location_province"),
            trim(col("location.city")).alias("location_city"),
            trim(col("location.coordinates")).alias("location_coordinates"),
            split(trim(col("location.coordinates")), ",").getItem(0).cast("double").alias("latitude"),
            split(trim(col("location.coordinates")), ",").getItem(1).cast("double").alias("longitude"),
            col("ingestion_timestamp"),
        )
        .filter(col("transaction_id").isNotNull())
    )
    transactions_clean = deduplicate_latest(transactions_clean, "transaction_id")
    
    transactions_dq = (
        transactions_clean
        .withColumn(
            "dq_valid_transaction_id",
            when(col("transaction_id").isNotNull(), lit(True)).otherwise(lit(False))
        )
        .withColumn(
            "dq_valid_account_id",
            when(col("account_id").isNotNull(), lit(True)).otherwise(lit(False))
        )
        .withColumn(
            "dq_valid_transaction_date",
            when(col("transaction_date").isNotNull(), lit(True)).otherwise(lit(False))
        )
        .withColumn(
            "dq_valid_amount",
            when(col("amount").isNotNull(), lit(True)).otherwise(lit(False))
        )
        .withColumn(
            "dq_valid_currency",
            when(col("currency") == lit("ZAR"), lit(True)).otherwise(lit(False))
        )
        .withColumn(
            "dq_is_valid",
            when(
                col("dq_valid_transaction_id")
                & col("dq_valid_account_id")
                & col("dq_valid_transaction_date")
                & col("dq_valid_amount")
                & col("dq_valid_currency"),
                lit(True)
            ).otherwise(lit(False))
        )
    )
    return transactions_dq
def run_transformation():
    # 1. Load pipeline_config.yaml to get input/output paths.
    config = load_config()
    bronze_path = config["output"]["bronze_path"]
    silver_path = config["output"]["silver_path"]
    # 2. Initialise SparkSession.
    spark = create_spark_session()

    try:
        # 3. Read each Bronze table.
        accounts_bronze = spark.read.format("delta").load(f"{bronze_path}/accounts")
        transactions_bronze = spark.read.format("delta").load(f"{bronze_path}/transactions")
        customers_bronze = spark.read.format("delta").load(f"{bronze_path}/customers")
        # 4. Deduplicate, type-cast, and standardise each table.
        accounts_silver = clean_accounts(accounts_bronze)
        customers_silver = clean_customers(customers_bronze)
        # 5. Apply DQ flagging to the transactions table.
        transactions_silver = clean_transactions(transactions_bronze)
        # Resolve account-to-customer linkage.
        account_customer_link = (
            accounts_silver
            .select(
                col("account_id"),
                col("customer_ref").alias("customer_id")
            )
            .join(
                customers_silver.select("customer_id"),
                on="customer_id",
                how="inner"
            )
            .dropDuplicates(["account_id", "customer_id"])
        )
        transactions_silver = (
            transactions_silver
            .join(account_customer_link, on="account_id", how="left")
        )
        # 6. Write cleaned tables to silver/.
        accounts_silver.write.format("delta").mode("overwrite").save(f"{silver_path}/accounts")
        customers_silver.write.format("delta").mode("overwrite").save(f"{silver_path}/customers")
        transactions_silver.write.format("delta").mode("overwrite").save(f"{silver_path}/transactions")
        account_customer_link.write.format("delta").mode("overwrite").save(f"{silver_path}/account_customer_link")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_transformation()

    # TODO: Implement Silver layer transformation.
    #
    # Suggested steps:
    #   1. Load pipeline_config.yaml to get input/output paths.
    #   2. Initialise (or reuse) SparkSession.
    #   3. Read each Bronze table.
    #   4. Deduplicate, type-cast, and standardise each table.
    #   5. Apply DQ flagging to the transactions table.
    #   6. Write cleaned tables to silver/.
    pass

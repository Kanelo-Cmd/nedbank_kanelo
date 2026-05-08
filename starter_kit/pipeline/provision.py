"""
Gold layer: Join and aggregate Silver tables into the scored output schema.

Input paths (Silver layer output — read these, do not modify):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Output paths (your pipeline must create these directories):
  /data/output/gold/fact_transactions/     — 15 fields (see output_schema_spec.md §2)
  /data/output/gold/dim_accounts/          — 11 fields (see output_schema_spec.md §3)
  /data/output/gold/dim_customers/         — 9 fields  (see output_schema_spec.md §4)

Requirements:
  - Generate surrogate keys (_sk fields) that are unique, non-null, and stable
    across pipeline re-runs on the same input data. Use row_number() with a
    stable ORDER BY on the natural key, or sha2(natural_key, 256) cast to BIGINT.
  - Resolve all foreign key relationships:
      fact_transactions.account_sk  → dim_accounts.account_sk
      fact_transactions.customer_sk → dim_customers.customer_sk
      dim_accounts.customer_id      → dim_customers.customer_id
  - Rename accounts.customer_ref → dim_accounts.customer_id at this layer.
  - Derive dim_customers.age_band from dob (do not copy dob directly).
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.
  - At Stage 2, also write /data/output/dq_report.json summarising DQ outcomes.

See output_schema_spec.md for the complete field-by-field specification.
"""
import os
import json
import yaml
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    lit,
    when,
    floor,
    months_between,
    current_date,
    row_number,
    count,
    sum as spark_sum,
    broadcast,
)

def load_config(config_path: str = "/data/config/pipeline_config.yaml") -> dict:
    if not os.path.exists(config_path):
        config_path = "config/pipeline_config.yaml"

    with open(config_path, "r") as file:
        return yaml.safe_load(file)
def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("gold_provisioning")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )
def add_surrogate_key(df, sk_name: str, natural_key: str):
    window_spec = Window.orderBy(col(natural_key))
    return (
        df
        .withColumn(sk_name, row_number().over(window_spec))
    )
def build_dim_customers(customers_df):
    customers_dim = (
        customers_df
        .withColumn(
            "age",
            floor(months_between(current_date(), col("dob")) / lit(12))
        )
        .withColumn(
            "age_band",
            when(col("age").isNull(), lit("UNKNOWN"))
            .when(col("age") < 18, lit("UNDER_18"))
            .when((col("age") >= 18) & (col("age") <= 24), lit("18_24"))
            .when((col("age") >= 25) & (col("age") <= 34), lit("25_34"))
            .when((col("age") >= 35) & (col("age") <= 44), lit("35_44"))
            .when((col("age") >= 45) & (col("age") <= 54), lit("45_54"))
            .when((col("age") >= 55) & (col("age") <= 64), lit("55_64"))
            .otherwise(lit("65_PLUS"))
        )
        .select(
            "customer_id",
            "id_number",
            "first_name",
            "last_name",
            "dob",
            "age",
            "age_band",
            "gender",
            "province",
            "income_band",
            "segment",
            "risk_score",
            "kyc_status",
            "product_flags",
        )
    )
    customers_dim = add_surrogate_key(customers_dim, "customer_sk", "customer_id")
    return customers_dim.select(
        "customer_sk",
        "customer_id",
        "id_number",
        "first_name",
        "last_name",
        "dob",
        "age",
        "age_band",
        "gender",
        "province",
        "income_band",
        "segment",
        "risk_score",
        "kyc_status",
        "product_flags",
    )
def build_dim_accounts(accounts_df):
    accounts_dim = (
        accounts_df
        .withColumnRenamed("customer_ref", "customer_id")
        .select(
            "account_id",
            "customer_id",
            "account_type",
            "account_status",
            "open_date",
            "product_tier",
            "mobile_number",
            "digital_channel",
            "credit_limit",
            "current_balance",
            "last_activity_date",
        )
    )
    accounts_dim = add_surrogate_key(accounts_dim, "account_sk", "account_id")
    return accounts_dim.select(
        "account_sk",
        "account_id",
        "customer_id",
        "account_type",
        "account_status",
        "open_date",
        "product_tier",
        "mobile_number",
        "digital_channel",
        "credit_limit",
        "current_balance",
        "last_activity_date",
    )
def build_fact_transactions(transactions_df, dim_accounts, dim_customers):
    account_lookup = dim_accounts.select(
        "account_sk",
        "account_id",
        "customer_id",
    )
    customer_lookup = dim_customers.select(
        "customer_sk",
        "customer_id",
    )
    fact = (
        transactions_df
        .filter(col("dq_is_valid") == lit(True))
        .join(
            broadcast(account_lookup),
            on="account_id",
            how="left"
        )
        .join(
            broadcast(customer_lookup),
            on="customer_id",
            how="left"
        )
        .select(
            "transaction_id",
            "account_sk",
            "customer_sk",
            "account_id",
            "customer_id",
            "transaction_date",
            "transaction_time",
            "transaction_timestamp",
            "transaction_type",
            "merchant_category",
            "amount",
            "currency",
            "channel",
            "location_province",
            "location_city",
            "latitude",
            "longitude",
            "dq_is_valid",
        )
    )
    return fact
def write_dq_report(transactions_df, output_path: str):
    total_count = transactions_df.count()
    valid_count = transactions_df.filter(col("dq_is_valid") == lit(True)).count()
    invalid_count = total_count - valid_count
    report = {
        "transactions": {
            "total_count": total_count,
            "valid_count": valid_count,
            "invalid_count": invalid_count,
        }
    }
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as file:
        json.dump(report, file, indent=2)
def run_provisioning():
    # 1. Load pipeline_config.yaml to get input/output paths.
    config = load_config()
    silver_path = config["output"]["silver_path"]
    gold_path = config["output"]["gold_path"]
    dq_report_path = config["output"].get("dq_report_path", "/data/output/dq_report.json")
    # 2. Initialise SparkSession.
    spark = create_spark_session()
    try:
        # 3. Read Silver tables.
        accounts_silver = spark.read.format("delta").load(f"{silver_path}/accounts")
        customers_silver = spark.read.format("delta").load(f"{silver_path}/customers")
        transactions_silver = spark.read.format("delta").load(f"{silver_path}/transactions")
        # 4. Build dim_customers with surrogate keys and derived age_band.
        dim_customers = build_dim_customers(customers_silver)
        # 5. Build dim_accounts with surrogate keys; rename customer_ref → customer_id.
        dim_accounts = build_dim_accounts(accounts_silver)
        # 6. Build fact_transactions, resolving account_sk and customer_sk via joins.
        fact_transactions = build_fact_transactions(
            transactions_silver,
            dim_accounts,
            dim_customers,
        )
        # 7. Write all three Gold tables as Delta Parquet.
        dim_customers.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_customers")
        dim_accounts.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_accounts")
        fact_transactions.write.format("delta").mode("overwrite").save(f"{gold_path}/fact_transactions")
        # 8. Stage 2+ DQ report.
        write_dq_report(transactions_silver, dq_report_path)

    finally:
        spark.stop()
if __name__ == "__main__":
    run_provisioning()

    # TODO: Implement Gold layer provisioning.
    #
    # Suggested steps:
    #   1. Load pipeline_config.yaml to get input/output paths.
    #   2. Initialise (or reuse) SparkSession.
    #   3. Read Silver tables.
    #   4. Build dim_customers with surrogate keys and derived age_band.
    #   5. Build dim_accounts with surrogate keys; rename customer_ref → customer_id.
    #   6. Build fact_transactions, resolving account_sk and customer_sk via joins.
    #   7. Write all three Gold tables as Delta Parquet.
    #   8. (Stage 2+) Write dq_report.json to /data/output/.
    pass

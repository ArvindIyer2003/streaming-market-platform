import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

GOLD_PATH = os.path.join(ROOT_DIR, "data", "delta", "gold")
ML_PATH = os.path.join(ROOT_DIR, "data", "delta", "ml")

# Snowflake connection options
sfOptions = {
    "sfURL": f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase": "MARKET_ANALYTICS",
    "sfSchema": "RAW",
    "sfWarehouse": "ANALYTICS_WH"
}


def create_spark():

    return (
        SparkSession.builder
        .appName("DeltaToSnowflake")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            ",".join([
                "io.delta:delta-spark_2.12:3.0.0",
                "net.snowflake:snowflake-jdbc:3.13.30",
                "net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3"
            ])
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def load_to_snowflake(spark, delta_path, table, schema="RAW"):

    logger.info(f"📥 Loading {delta_path} → {schema}.{table}")

    df = spark.read.format("delta").load(delta_path)

    # Fix timestamp column if stored as epoch
    if "TIMESTAMP_PARSED" in df.columns:
        df = df.withColumn(
            "TIMESTAMP_PARSED",
            (col("TIMESTAMP_PARSED") / 1000).cast("timestamp")
        )

    df.write \
        .format("snowflake") \
        .options(**sfOptions) \
        .option("sfSchema", schema) \
        .option("dbtable", table) \
        .mode("overwrite") \
        .save()

    logger.info(f"✅ Loaded {schema}.{table}")


def main():

    logger.info("🚀 Delta → Snowflake Loader")
    logger.info("=" * 60)

    spark = create_spark()

    # GOLD TABLES
    gold_tables = [
        ("fact_stock_trades", "STOCK_TRADES"),
        ("fact_ohlc_candles", "OHLC_CANDLES"),
        ("stock_with_sentiment", "STOCK_SENTIMENT"),
        ("dim_stocks", "DIM_STOCKS"),
    ]

    for folder, table in gold_tables:

        path = os.path.join(GOLD_PATH, folder)

        if os.path.exists(path):
            load_to_snowflake(spark, path, table, "RAW")
        else:
            logger.warning(f"⚠️ Path not found: {path}")

    # ML TABLES
    ml_tables = [
        ("features", "FEATURES"),
    ]

    for folder, table in ml_tables:

        path = os.path.join(ML_PATH, folder)

        if os.path.exists(path):
            load_to_snowflake(spark, path, table, "ML")
        else:
            logger.warning(f"⚠️ Path not found: {path}")

    spark.stop()

    logger.info("=" * 60)
    logger.info("✅ All tables loaded to Snowflake")
    logger.info("Database: MARKET_ANALYTICS")
    logger.info("Schemas: RAW, ML")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
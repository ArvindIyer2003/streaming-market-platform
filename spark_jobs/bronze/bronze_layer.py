"""
Bronze Layer - Spark Structured Streaming Consumer
Location: spark_jobs/bronze/bronze_layer.py
"""
import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
    LongType, BooleanType
)
from loguru import logger

# ================================
# PATH SETUP
# ================================
# Go up 2 levels: bronze/ → spark_jobs/ → project root
ROOT_DIR = os.path.dirname(
    os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )
)
sys.path.append(ROOT_DIR)

# Now we can import config from root
from config import KAFKA_CONFIG

# ================================
# PATHS
# ================================
BRONZE_PATH     = os.path.join(ROOT_DIR, "data", "delta", "bronze")
CHECKPOINT_PATH = os.path.join(ROOT_DIR, "checkpoints", "bronze")

# ================================
# SPARK SESSION
# ================================
def create_spark_session():
    """
    Create Spark session with Delta Lake and Kafka support
    """
    spark = SparkSession.builder \
        .appName("MarketAnalytics-Bronze") \
        .master("local[*]") \
        \
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        ) \
        \
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        ) \
        \
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        ) \
        \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "1536m") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("✅ Spark session created!")
    return spark

# ================================
# SCHEMAS
# ================================
STOCK_SCHEMA = StructType([
    StructField("symbol",           StringType(),  True),
    StructField("name",             StringType(),  True),
    StructField("sector",           StringType(),  True),
    StructField("exchange",         StringType(),  True),
    StructField("currency",         StringType(),  True),
    StructField("price",            DoubleType(),  True),
    StructField("open",             DoubleType(),  True),
    StructField("day_high",         DoubleType(),  True),
    StructField("day_low",          DoubleType(),  True),
    StructField("prev_close",       DoubleType(),  True),
    StructField("volume",           LongType(),    True),
    StructField("price_change",     DoubleType(),  True),
    StructField("price_change_pct", DoubleType(),  True),
    StructField("timestamp",        StringType(),  True),
    StructField("date",             StringType(),  True),
    StructField("market_hours",     BooleanType(), True),
    StructField("source",           StringType(),  True),
])

CRYPTO_SCHEMA = StructType([
    StructField("symbol",           StringType(),  True),
    StructField("name",             StringType(),  True),
    StructField("sector",           StringType(),  True),
    StructField("exchange",         StringType(),  True),
    StructField("currency",         StringType(),  True),
    StructField("price",            DoubleType(),  True),
    StructField("day_high",         DoubleType(),  True),
    StructField("day_low",          DoubleType(),  True),
    StructField("prev_close",       DoubleType(),  True),
    StructField("volume",           LongType(),    True),
    StructField("market_cap",       LongType(),    True),
    StructField("price_change",     DoubleType(),  True),
    StructField("price_change_pct", DoubleType(),  True),
    StructField("timestamp",        StringType(),  True),
    StructField("date",             StringType(),  True),
    StructField("market_open",      BooleanType(), True),
    StructField("source",           StringType(),  True),
])

NEWS_SCHEMA = StructType([
    StructField("symbol",          StringType(), True),
    StructField("headline",        StringType(), True),
    StructField("description",     StringType(), True),
    StructField("source",          StringType(), True),
    StructField("url",             StringType(), True),
    StructField("published_at",    StringType(), True),
    StructField("sentiment_score", DoubleType(), True),
    StructField("sentiment_label", StringType(), True),
    StructField("positive_words",  LongType(),   True),
    StructField("negative_words",  LongType(),   True),
    StructField("timestamp",       StringType(),  True),
    StructField("date",            StringType(),  True),
])

# ================================
# READ FROM KAFKA
# ================================
def read_from_kafka(spark, topic):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",
                KAFKA_CONFIG['bootstrap_servers']) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

# ================================
# PARSE KAFKA MESSAGES
# ================================
def parse_kafka_messages(raw_df, schema):
    return raw_df \
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("kafka_value"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp")
        ) \
        .select(
            F.from_json(
                F.col("kafka_value"),
                schema
            ).alias("data"),
            F.col("kafka_key"),
            F.col("partition"),
            F.col("offset"),
            F.col("kafka_timestamp")
        ) \
        .select(
            "data.*",
            "kafka_key",
            "partition",
            "offset",
            "kafka_timestamp",
            F.current_timestamp().alias("ingested_at")
        )

# ================================
# WRITE TO BRONZE
# ================================
def write_to_bronze(df, table_name, checkpoint_name):
    path       = os.path.join(BRONZE_PATH, table_name)
    checkpoint = os.path.join(CHECKPOINT_PATH, checkpoint_name)

    return df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint) \
        .option("mergeSchema", "true") \
        .trigger(processingTime="10 seconds") \
        .start(path)

# ================================
# MONITORING
# ================================
def log_stats(query, name):
    progress = query.lastProgress
    if progress:
        logger.info(
            f"📊 {name}: "
            f"rows/sec={progress.get('processedRowsPerSecond', 0):.1f} | "
            f"batch={progress.get('batchId', 0)} | "
            f"rows={progress.get('numInputRows', 0)}"
        )

# ================================
# MAIN
# ================================
def main():
    logger.info("🚀 Starting Bronze Layer")
    logger.info(f"📂 Writing to: {BRONZE_PATH}")
    logger.info("=" * 60)

    spark = create_spark_session()

    # Stock stream
    logger.info("📈 Starting stock prices stream...")
    stock_query = write_to_bronze(
        parse_kafka_messages(
            read_from_kafka(spark, KAFKA_CONFIG['topics']['stocks']),
            STOCK_SCHEMA
        ),
        table_name="stock_prices",
        checkpoint_name="stocks"
    )
    logger.info("✅ Stock stream running!")

    # Crypto stream
    logger.info("🪙 Starting crypto prices stream...")
    crypto_query = write_to_bronze(
        parse_kafka_messages(
            read_from_kafka(spark, KAFKA_CONFIG['topics']['crypto']),
            CRYPTO_SCHEMA
        ),
        table_name="crypto_prices",
        checkpoint_name="crypto"
    )
    logger.info("✅ Crypto stream running!")

    # News stream
    logger.info("📰 Starting news sentiment stream...")
    news_query = write_to_bronze(
        parse_kafka_messages(
            read_from_kafka(spark, KAFKA_CONFIG['topics']['news']),
            NEWS_SCHEMA
        ),
        table_name="news_sentiment",
        checkpoint_name="news"
    )
    logger.info("✅ News stream running!")

    logger.info("=" * 60)
    logger.info("🔄 All streams active!")
    logger.info("📂 Check: ./data/delta/bronze/")
    logger.info("🌐 Kafka UI: http://localhost:8080")
    logger.info("Press Ctrl+C to stop")
    logger.info("=" * 60)

    try:
        while True:
            time.sleep(30)
            log_stats(stock_query,  "Stocks")
            log_stats(crypto_query, "Crypto")
            log_stats(news_query,   "News  ")

    except KeyboardInterrupt:
        logger.info("⏹️ Stopping all streams...")
        stock_query.stop()
        crypto_query.stop()
        news_query.stop()

    finally:
        spark.stop()
        logger.info("✅ Done!")

if __name__ == "__main__":
    # Create all directories
    os.makedirs(os.path.join(BRONZE_PATH, "stock_prices"),  exist_ok=True)
    os.makedirs(os.path.join(BRONZE_PATH, "crypto_prices"), exist_ok=True)
    os.makedirs(os.path.join(BRONZE_PATH, "news_sentiment"), exist_ok=True)
    os.makedirs(os.path.join(CHECKPOINT_PATH, "stocks"),    exist_ok=True)
    os.makedirs(os.path.join(CHECKPOINT_PATH, "crypto"),    exist_ok=True)
    os.makedirs(os.path.join(CHECKPOINT_PATH, "news"),      exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    main()
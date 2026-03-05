"""
Silver Layer - Data Cleaning + Simple Feature Engineering
Streaming-compatible version
"""
import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from loguru import logger

# ================================
# PATH SETUP
# ================================
ROOT_DIR = os.path.dirname(
    os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )
)
sys.path.append(ROOT_DIR)

from config import KAFKA_CONFIG

# ================================
# PATHS
# ================================
BRONZE_PATH = os.path.join(ROOT_DIR, "data", "delta", "bronze")
SILVER_PATH = os.path.join(ROOT_DIR, "data", "delta", "silver")
CHECKPOINT_PATH = os.path.join(ROOT_DIR, "checkpoints", "silver")

# ================================
# SPARK SESSION
# ================================
def create_spark_session():
    spark = SparkSession.builder \
        .appName("MarketAnalytics-Silver") \
        .master("local[*]") \
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "1536m") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("✅ Spark session created!")
    return spark

# ================================
# READ FROM BRONZE (STREAMING)
# ================================
def read_bronze_stream(spark, table_name):
    """Read streaming data from Bronze Delta Lake"""
    path = os.path.join(BRONZE_PATH, table_name)
    return spark.readStream \
        .format("delta") \
        .load(path)

# ================================
# CLEAN & ENRICH STOCK DATA
# ================================
def enrich_stock_data(df):
    """
    Clean and add simple enrichments
    Uses only streaming-compatible operations
    """
    
    # Parse timestamps
    df = df \
        .filter(F.col("price").isNotNull()) \
        .filter(F.col("price") > 0) \
        .filter(F.col("volume") >= 0) \
        .withColumn(
            "timestamp_parsed",
            F.to_timestamp(F.col("timestamp"))
        ) \
        .withColumn(
            "date_parsed",
            F.to_date(F.col("timestamp_parsed"))
        ) \
        .withColumn(
            "hour",
            F.hour(F.col("timestamp_parsed"))
        ) \
        .withColumn(
            "minute",
            F.minute(F.col("timestamp_parsed"))
        )
    
    # Add watermark for late data handling
    df = df.withWatermark("timestamp_parsed", "2 minutes")
    
    # Simple anomaly detection (price change only)
    df = df.withColumn(
        "is_price_spike",
        (F.abs(F.col("price_change_pct")) > 2.0)
    )
    
    # Volume anomaly (simple threshold)
    df = df.withColumn(
        "is_high_volume",
        (F.col("volume") > 1000000)  # Adjust threshold as needed
    )
    
    # Price direction
    df = df.withColumn(
        "price_direction",
        F.when(F.col("price_change") > 0, "UP")
        .when(F.col("price_change") < 0, "DOWN")
        .otherwise("FLAT")
    )
    
    # Volatility indicator (abs % change)
    df = df.withColumn(
        "volatility",
        F.abs(F.col("price_change_pct"))
    )
    
    # Overall anomaly flag
    df = df.withColumn(
        "is_anomaly",
        (F.col("is_price_spike") | F.col("is_high_volume"))
    )
    
    return df

# ================================
# TIME-BASED AGGREGATIONS (OHLC)
# ================================
def calculate_ohlc_1min(df):
    """
    Calculate 1-minute OHLC candles
    This IS supported in streaming!
    """
    
    return df \
        .withWatermark("timestamp_parsed", "2 minutes") \
        .groupBy(
            "symbol",
            "sector",
            "exchange",
            "currency",
            F.window("timestamp_parsed", "1 minute")
        ) \
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("volume").alias("volume"),
            F.avg("price").alias("avg_price"),
            F.count("*").alias("tick_count"),
            F.max("price_change_pct").alias("max_change_pct"),
            F.min("price_change_pct").alias("min_change_pct"),
            F.sum(F.when(F.col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count")
        ) \
        .select(
            "symbol",
            "sector", 
            "exchange",
            "currency",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "open",
            "high",
            "low",
            "close",
            "volume",
            "avg_price",
            "tick_count",
            "max_change_pct",
            "min_change_pct",
            "anomaly_count",
            F.current_timestamp().alias("processed_at")
        )

# ================================
# WRITE TO SILVER
# ================================
def write_to_silver(df, table_name, checkpoint_name, output_mode="append"):
    """Write to Silver Delta Lake"""
    path = os.path.join(SILVER_PATH, table_name)
    checkpoint = os.path.join(CHECKPOINT_PATH, checkpoint_name)
    
    return df.writeStream \
        .format("delta") \
        .outputMode(output_mode) \
        .option("checkpointLocation", checkpoint) \
        .option("mergeSchema", "true") \
        .trigger(processingTime="10 seconds") \
        .start(path)

# ================================
# MAIN
# ================================
def main():
    logger.info("🚀 Starting Silver Layer - Feature Engineering")
    logger.info(f"📂 Writing to: {SILVER_PATH}")
    logger.info("=" * 60)
    
    spark = create_spark_session()
    
    # ─────────────────────────────
    # STOCK PRICES - TICK DATA
    # ─────────────────────────────
    logger.info("📈 Processing stock tick data...")
    
    bronze_stocks = read_bronze_stream(spark, "stock_prices")
    enriched_stocks = enrich_stock_data(bronze_stocks)
    
    stocks_tick_query = write_to_silver(
        enriched_stocks,
        table_name="stock_prices_enriched",
        checkpoint_name="stocks_tick"
    )
    
    logger.info("✅ Stock tick stream running!")
    
    # ─────────────────────────────
    # STOCK PRICES - 1MIN OHLC
    # ─────────────────────────────
    logger.info("📊 Processing stock 1-min OHLC...")
    
    ohlc_1min = calculate_ohlc_1min(enriched_stocks)
    
    stocks_ohlc_query = write_to_silver(
        ohlc_1min,
        table_name="stock_prices_1min_ohlc",
        checkpoint_name="stocks_ohlc",
        output_mode="append"  # Complete mode for aggregations
    )
    
    logger.info("✅ Stock OHLC stream running!")
    
    # ─────────────────────────────
    # CRYPTO PRICES
    # ─────────────────────────────
    logger.info("🪙 Processing crypto prices...")
    
    bronze_crypto = read_bronze_stream(spark, "crypto_prices")
    enriched_crypto = enrich_stock_data(bronze_crypto)
    
    crypto_query = write_to_silver(
        enriched_crypto,
        table_name="crypto_prices_enriched",
        checkpoint_name="crypto"
    )
    
    logger.info("✅ Crypto stream running!")
    
    # ─────────────────────────────
    # NEWS SENTIMENT
    # ─────────────────────────────
    logger.info("📰 Processing news sentiment...")
    
    bronze_news = read_bronze_stream(spark, "news_sentiment")
    
    cleaned_news = bronze_news \
        .withColumn("timestamp_parsed", F.to_timestamp(F.col("timestamp"))) \
        .withColumn("date_parsed", F.to_date(F.col("timestamp_parsed"))) \
        .withColumn(
            "sentiment_category",
            F.when(F.col("sentiment_score") > 0.3, "POSITIVE")
            .when(F.col("sentiment_score") < -0.3, "NEGATIVE")
            .otherwise("NEUTRAL")
        )
    
    news_query = write_to_silver(
        cleaned_news,
        table_name="news_sentiment_clean",
        checkpoint_name="news"
    )
    
    logger.info("✅ News stream running!")
    
    # ─────────────────────────────
    # MONITOR
    # ─────────────────────────────
    logger.info("=" * 60)
    logger.info("🔄 All Silver streams running!")
    logger.info("📊 Features: Price enrichment, OHLC candles, Anomalies")
    logger.info("📂 Check: ./data/delta/silver/")
    logger.info("Press Ctrl+C to stop")
    logger.info("=" * 60)
    
    try:
        while True:
            time.sleep(30)
            for query, name in [
                (stocks_tick_query, "Stock Ticks"),
                (stocks_ohlc_query, "Stock OHLC "),
                (crypto_query, "Crypto     "),
                (news_query, "News       ")
            ]:
                progress = query.lastProgress
                if progress:
                    logger.info(
                        f"📊 {name}: "
                        f"rows/sec={progress.get('processedRowsPerSecond', 0):.1f} | "
                        f"batch={progress.get('batchId', 0)} | "
                        f"rows={progress.get('numInputRows', 0)}"
                    )
    
    except KeyboardInterrupt:
        logger.info("⏹️ Stopping all streams...")
        stocks_tick_query.stop()
        stocks_ohlc_query.stop()
        crypto_query.stop()
        news_query.stop()
    
    finally:
        spark.stop()
        logger.info("✅ Done!")

if __name__ == "__main__":
    # Create directories
    os.makedirs(os.path.join(SILVER_PATH, "stock_prices_enriched"), exist_ok=True)
    os.makedirs(os.path.join(SILVER_PATH, "stock_prices_1min_ohlc"), exist_ok=True)
    os.makedirs(os.path.join(SILVER_PATH, "crypto_prices_enriched"), exist_ok=True)
    os.makedirs(os.path.join(SILVER_PATH, "news_sentiment_clean"), exist_ok=True)
    os.makedirs(os.path.join(CHECKPOINT_PATH, "stocks_tick"), exist_ok=True)
    os.makedirs(os.path.join(CHECKPOINT_PATH, "stocks_ohlc"), exist_ok=True)
    os.makedirs(os.path.join(CHECKPOINT_PATH, "crypto"), exist_ok=True)
    os.makedirs(os.path.join(CHECKPOINT_PATH, "news"), exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    main()
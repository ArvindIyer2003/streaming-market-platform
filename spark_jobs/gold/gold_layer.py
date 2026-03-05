"""
Gold Layer - Hybrid Streaming + Batch
Trades/Candles/Hourly streaming, News join batch scheduled
"""
import os
import sys
import time
from datetime import datetime
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

SILVER_PATH = os.path.join(ROOT_DIR, "data", "delta", "silver")
GOLD_PATH = os.path.join(ROOT_DIR, "data", "delta", "gold")
CHECKPOINT_PATH = os.path.join(ROOT_DIR, "checkpoints", "gold")

# ================================
# SPARK SESSION
# ================================
def create_spark_session():
    spark = SparkSession.builder \
        .appName("MarketAnalytics-Gold-Hybrid") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("✅ Spark session created!")
    return spark

# ================================
# READ SILVER
# ================================
def read_silver_stream(spark, table_name):
    """Read as stream"""
    path = os.path.join(SILVER_PATH, table_name)
    return spark.readStream.format("delta").load(path)

def read_silver_batch(spark, table_name):
    """Read as batch"""
    path = os.path.join(SILVER_PATH, table_name)
    return spark.read.format("delta").load(path)

# ================================
# FACT: STOCK TRADES (STREAMING)
# ================================
def create_fact_stock_trades(df):
    return df.select(
        F.col("symbol"),
        F.col("timestamp_parsed"),  # Keep original name
        F.col("date_parsed").alias("trade_date"),
        F.col("sector"),
        F.col("exchange"),
        F.col("currency"),
        F.col("price"),
        F.col("volume"),
        F.col("price_change"),
        F.col("price_change_pct"),
        F.col("is_anomaly"),
        F.col("is_price_spike"),
        F.col("is_high_volume"),
        F.col("price_direction"),
        F.col("volatility"),
        F.current_timestamp().alias("loaded_at")
    )

# ================================
# FACT: OHLC CANDLES (STREAMING)
# ================================
def create_fact_ohlc_candles(df):
    return df.select(
        F.col("symbol"),
        F.col("window_start"),
        F.col("window_end"),
        F.col("sector"),
        F.col("exchange"),
        F.col("currency"),
        F.col("open"),
        F.col("high"),
        F.col("low"),
        F.col("close"),
        F.col("volume"),
        F.col("avg_price"),
        F.col("tick_count"),
        F.col("max_change_pct"),
        F.col("min_change_pct"),
        F.col("anomaly_count"),
        F.when(F.col("close") > F.col("open"), "GREEN")
         .when(F.col("close") < F.col("open"), "RED")
         .otherwise("DOJI").alias("candle_color"),
        ((F.col("high") - F.col("low")) / F.col("open") * 100).alias("candle_range_pct"),
        F.current_timestamp().alias("loaded_at")
    )

# ================================
# HOURLY METRICS (STREAMING)
# ================================
def create_hourly_metrics(df):
    return df \
        .withWatermark("timestamp_parsed", "5 minutes") \
        .groupBy(
            "symbol",
            "sector",
            "exchange",
            F.window("timestamp_parsed", "1 hour")
        ) \
        .agg(
            F.first("price").alias("hour_open"),
            F.max("price").alias("hour_high"),
            F.min("price").alias("hour_low"),
            F.last("price").alias("hour_close"),
            F.avg("price").alias("hour_avg_price"),
            F.sum("volume").alias("hour_volume"),
            F.count("*").alias("tick_count"),
            F.stddev("price").alias("price_stddev"),
            F.sum(F.when(F.col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count")
        ) \
        .select(
            F.col("symbol"),
            F.col("sector"),
            F.col("exchange"),
            F.col("window.start").alias("hour_start"),
            F.col("window.end").alias("hour_end"),
            F.col("hour_open"),
            F.col("hour_high"),
            F.col("hour_low"),
            F.col("hour_close"),
            F.col("hour_avg_price"),
            F.col("hour_volume"),
            F.col("tick_count"),
            F.col("price_stddev"),
            F.col("anomaly_count")
        ) \
        .drop("window")

# ================================
# STOCK + NEWS JOIN (BATCH)
# ================================
def batch_join_stock_news(spark):
    """
    Batch job: Join stock prices with news sentiment
    Runs every 2 minutes
    """
    logger.info("📊 Running batch news join...")
    
    # Read latest data
    stocks_df = read_silver_batch(spark, "stock_prices_enriched")
    news_df = read_silver_batch(spark, "news_sentiment_clean")
    
    # Aggregate news by symbol and day
    news_agg = news_df.groupBy(
        "symbol",
        F.to_date("timestamp_parsed").alias("news_date")
    ).agg(
        F.avg("sentiment_score").alias("news_sentiment"),
        F.count("*").alias("news_count"),
        F.sum(F.when(F.col("sentiment_category") == "POSITIVE", 1).otherwise(0)).alias("positive_news_count"),
        F.sum(F.when(F.col("sentiment_category") == "NEGATIVE", 1).otherwise(0)).alias("negative_news_count")
    )
    
    # Join with stocks
    joined = stocks_df.join(
        news_agg,
        (stocks_df.symbol == news_agg.symbol) &
        (stocks_df.date_parsed == news_agg.news_date),
        "left"
    ).select(
        stocks_df["*"],
        F.coalesce(news_agg.news_sentiment, F.lit(0)).alias("news_sentiment"),
        F.coalesce(news_agg.news_count, F.lit(0)).alias("news_count"),
        F.coalesce(news_agg.positive_news_count, F.lit(0)).alias("positive_news_count"),
        F.coalesce(news_agg.negative_news_count, F.lit(0)).alias("negative_news_count")
    )
    
    # Write to Gold (overwrite mode for batch)
    path = os.path.join(GOLD_PATH, "stock_with_sentiment")
    
    joined.write \
        .format("delta") \
        .mode("overwrite") \
        .save(path)
    
    logger.info(f"✅ Batch news join complete! Rows written: {joined.count()}")

# ================================
# STREAMING WRITE
# ================================
def write_to_gold(df, table_name, checkpoint_name):
    path = os.path.join(GOLD_PATH, table_name)
    checkpoint = os.path.join(CHECKPOINT_PATH, checkpoint_name)

    return df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint) \
        .trigger(processingTime="30 seconds") \
        .start(path)

# ================================
# DIMENSION: STOCKS (BATCH)
# ================================
# ================================
# DIMENSION: STOCKS (BATCH)
# ================================
from datetime import datetime
def create_dim_stocks(spark):
    """Create dimension table (batch, one-time)"""
    from config import STOCK_METADATA
    from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType
    
    # Define explicit schema
    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("name", StringType(), False),
        StructField("sector", StringType(), False),
        StructField("exchange", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("is_current", BooleanType(), False),
        StructField("valid_from", TimestampType(), False),
        StructField("valid_to", TimestampType(), True)
    ])
    
    # Prepare data
    stocks_data = [
        (
            symbol,
            meta['name'],
            meta['sector'],
            meta['exchange'],
            meta['currency'],
            True,
            datetime.now(),
            None
        )
        for symbol, meta in STOCK_METADATA.items()
    ]
    
    # Create DataFrame with explicit schema
    df = spark.createDataFrame(stocks_data, schema)
    
    path = os.path.join(GOLD_PATH, "dim_stocks")
    df.write.format("delta").mode("overwrite").save(path)
    
    logger.info("✅ Dimension stocks created!")
# ================================
# MAIN
# ================================
def main():
    logger.info("🚀 Starting Gold Layer - Hybrid Streaming + Batch")
    logger.info("=" * 60)
    
    spark = create_spark_session()
    
    # ─────────────────────────────
    # STREAMING JOBS
    # ─────────────────────────────
    logger.info("📊 Starting streaming jobs...")
    
    silver_stocks = read_silver_stream(spark, "stock_prices_enriched")
    silver_ohlc = read_silver_stream(spark, "stock_prices_1min_ohlc")
    
    # 1. Fact trades (streaming)
    trades_query = write_to_gold(
        create_fact_stock_trades(silver_stocks),
        "fact_stock_trades",
        "fact_trades"
    )
    logger.info("✅ Fact trades streaming started!")
    
    # 2. Fact candles (streaming)
    candles_query = write_to_gold(
        create_fact_ohlc_candles(silver_ohlc),
        "fact_ohlc_candles",
        "fact_candles"
    )
    logger.info("✅ Fact candles streaming started!")
    
    # 3. Hourly metrics (streaming)
    hourly_query = write_to_gold(
        create_hourly_metrics(silver_stocks),
        "hourly_metrics",
        "hourly"
    )
    logger.info("✅ Hourly metrics streaming started!")
    
    # ─────────────────────────────
    # ONE-TIME BATCH JOBS
    # ─────────────────────────────
    logger.info("📊 Running one-time batch jobs...")
    
    # Create dimension table
    create_dim_stocks(spark)
    
    # Initial news join
    batch_join_stock_news(spark)
    
    # ─────────────────────────────
    # MONITORING LOOP
    # ─────────────────────────────
    logger.info("=" * 60)
    logger.info("🔄 Gold layer running!")
    logger.info("📊 Streaming: Trades, Candles, Hourly")
    logger.info("📊 Batch (every 2min): News join")
    logger.info("Press Ctrl+C to stop")
    logger.info("=" * 60)
    
    batch_counter = 0
    
    try:
        while True:
            time.sleep(30)
            batch_counter += 30
            
            # Monitor streaming jobs
            for query, name in [
                (trades_query, "Trades "),
                (candles_query, "Candles"),
                (hourly_query, "Hourly ")
            ]:
                progress = query.lastProgress
                if progress:
                    logger.info(
                        f"📊 {name}: "
                        f"rows/sec={progress.get('processedRowsPerSecond', 0):.1f} | "
                        f"batch={progress.get('batchId', 0)}"
                    )
            
            # Run batch news join every 2 minutes (120 seconds)
            if batch_counter >= 120:
                batch_join_stock_news(spark)
                batch_counter = 0
                logger.info("⏱️ Next batch join in 2 minutes")
    
    except KeyboardInterrupt:
        logger.info("⏹️ Stopping all streams...")
        trades_query.stop()
        candles_query.stop()
        hourly_query.stop()
    
    finally:
        spark.stop()
        logger.info("✅ Gold layer stopped!")

if __name__ == "__main__":
    # Create directories
    os.makedirs(os.path.join(GOLD_PATH, "fact_stock_trades"), exist_ok=True)
    os.makedirs(os.path.join(GOLD_PATH, "fact_ohlc_candles"), exist_ok=True)
    os.makedirs(os.path.join(GOLD_PATH, "hourly_metrics"), exist_ok=True)
    os.makedirs(os.path.join(GOLD_PATH, "stock_with_sentiment"), exist_ok=True)
    os.makedirs(os.path.join(GOLD_PATH, "dim_stocks"), exist_ok=True)
    os.makedirs(os.path.join(CHECKPOINT_PATH, "fact_trades"), exist_ok=True)
    os.makedirs(os.path.join(CHECKPOINT_PATH, "fact_candles"), exist_ok=True)
    os.makedirs(os.path.join(CHECKPOINT_PATH, "hourly"), exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    main()

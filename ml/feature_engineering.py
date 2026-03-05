"""
Feature Engineering for Price Prediction
Creates ML-ready features from Gold tables
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from loguru import logger

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

GOLD_PATH = os.path.join(ROOT_DIR, "data", "delta", "gold")
ML_PATH = os.path.join(ROOT_DIR, "data", "delta", "ml")

def create_spark():
    return SparkSession.builder \
        .appName("ML-FeatureEngineering") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

def create_ml_features(spark):
    """
    Create ML features from Gold tables
    Target: Predict if price will go UP or DOWN in next 5 minutes
    """
    
    logger.info("📊 Loading Gold tables...")
    
    # Load data
    stocks = spark.read.format("delta") \
        .load(os.path.join(GOLD_PATH, "fact_stock_trades"))
    
    stocks = stocks.withColumnRenamed("trade_timestamp", "timestamp_parsed")
    
    sentiment = spark.read.format("delta") \
        .load(os.path.join(GOLD_PATH, "stock_with_sentiment"))
    
    logger.info(f"Stocks: {stocks.count()} rows")
    
    # ══════════════════════════════════════
    # FEATURE ENGINEERING
    # ══════════════════════════════════════
    
    # Window for each symbol ordered by time
    window = Window.partitionBy("symbol").orderBy("timestamp_parsed")
    
    # Price-based features
    df = stocks.withColumn("price_lag_1", F.lag("price", 1).over(window))
    df = df.withColumn("price_lag_2", F.lag("price", 2).over(window))
    df = df.withColumn("price_lag_3", F.lag("price", 3).over(window))
    df = df.withColumn("price_lag_5", F.lag("price", 5).over(window))
    
    # Moving averages (using lag to avoid look-ahead bias)
    df = df.withColumn("price_ma_5", 
        F.avg("price").over(window.rowsBetween(-5, -1)))
    df = df.withColumn("price_ma_10", 
        F.avg("price").over(window.rowsBetween(-10, -1)))
    df = df.withColumn("price_ma_20", 
        F.avg("price").over(window.rowsBetween(-20, -1)))
    
    # Volatility (std dev)
    df = df.withColumn("price_std_5", 
        F.stddev("price").over(window.rowsBetween(-5, -1)))
    df = df.withColumn("price_std_10", 
        F.stddev("price").over(window.rowsBetween(-10, -1)))
    
    # Volume features
    df = df.withColumn("volume_ma_5", 
        F.avg("volume").over(window.rowsBetween(-5, -1)))
    df = df.withColumn("volume_ratio", 
        F.when(F.col("volume_ma_5") > 0, 
               F.col("volume") / F.col("volume_ma_5"))
        .otherwise(1))
    
    # Price momentum
    df = df.withColumn("momentum_5", 
        F.when(F.col("price_lag_5").isNotNull() & (F.col("price_lag_5") != 0),
               (F.col("price") - F.col("price_lag_5")) / F.col("price_lag_5") * 100)
        .otherwise(0))
    
    # Price change from previous tick
    df = df.withColumn("price_change_1", 
        F.col("price") - F.col("price_lag_1"))
    df = df.withColumn("price_change_pct_1",
        F.when(F.col("price_lag_1") != 0,
               (F.col("price_change_1") / F.col("price_lag_1")) * 100)
        .otherwise(0))
    
    # RSI approximation (simplified)
    df = df.withColumn("gain", 
        F.when(F.col("price_change_1") > 0, F.col("price_change_1")).otherwise(0))
    df = df.withColumn("loss", 
        F.when(F.col("price_change_1") < 0, -F.col("price_change_1")).otherwise(0))
    
    df = df.withColumn("avg_gain", 
        F.avg("gain").over(window.rowsBetween(-14, -1)))
    df = df.withColumn("avg_loss", 
        F.avg("loss").over(window.rowsBetween(-14, -1)))
    
    df = df.withColumn("rsi",
        F.when(F.col("avg_loss") != 0,
               100 - (100 / (1 + (F.col("avg_gain") / F.col("avg_loss")))))
        .otherwise(50))
    
    # Bollinger Bands position
    df = df.withColumn("bb_position",
        F.when((F.col("price_ma_20").isNotNull()) & (F.col("price_std_10") > 0),
               (F.col("price") - F.col("price_ma_20")) / F.col("price_std_10"))
        .otherwise(0))
    
    # Time-based features
    df = df.withColumn("hour", F.hour("timestamp_parsed"))
    df = df.withColumn("day_of_week", F.dayofweek("timestamp_parsed"))
    df = df.withColumn("is_market_open_hour", 
        F.when((F.col("hour") >= 9) & (F.col("hour") <= 16), 1).otherwise(0))
    
    # ══════════════════════════════════════
    # ADD SENTIMENT FEATURES
    # ══════════════════════════════════════
    
    # Get latest sentiment for each symbol
    sentiment_latest = sentiment.groupBy("symbol").agg(
        F.last("news_sentiment").alias("sentiment_score"),
        F.last("news_count").alias("news_count")
    )
    
    df = df.join(sentiment_latest, on="symbol", how="left")
    df = df.withColumn("sentiment_score", F.coalesce("sentiment_score", F.lit(0)))
    df = df.withColumn("news_count", F.coalesce("news_count", F.lit(0)))
    
    # ══════════════════════════════════════
    # CREATE TARGET VARIABLE
    # ══════════════════════════════════════
    
    # Target: Will price go UP in next period?
    df = df.withColumn("price_next", F.lead("price", 1).over(window))
    df = df.withColumn("price_change_next", 
        F.col("price_next") - F.col("price"))
    
    # Binary classification: 1 = UP, 0 = DOWN
    df = df.withColumn("target",
        F.when(F.col("price_change_next") > 0, 1)
        .when(F.col("price_change_next") < 0, 0)
        .otherwise(None))  # Remove ties
    
    # ══════════════════════════════════════
    # SELECT FINAL FEATURES
    # ══════════════════════════════════════
    
    feature_cols = [
        # Identifiers
        "symbol",
        "timestamp_parsed",
        "price",
        
        # Price features
        "price_change_pct",
        "price_ma_5",
        "price_ma_10",
        "price_ma_20",
        "price_std_5",
        "price_std_10",
        "momentum_5",
        "price_change_pct_1",
        
        # Technical indicators
        "rsi",
        "bb_position",
        
        # Volume features
        "volume",
        "volume_ratio",
        
        # Sentiment
        "sentiment_score",
        "news_count",
        
        # Time features
        "hour",
        "day_of_week",
        "is_market_open_hour",
        
        # Target
        "target",
        "price_next"
    ]
    
    ml_data = df.select(feature_cols).filter(F.col("target").isNotNull())
    
    # ══════════════════════════════════════
    # SAVE TO DELTA
    # ══════════════════════════════════════
    
    logger.info("💾 Saving ML dataset...")
    
    ml_data.write \
        .format("delta") \
        .mode("overwrite") \
        .save(os.path.join(ML_PATH, "features"))
    
    logger.info(f"✅ ML features created: {ml_data.count()} samples")
    
    # Show sample
    logger.info("\nSample features:")
    ml_data.select("symbol", "price", "rsi", "sentiment_score", "target") \
        .show(10, truncate=False)
    
    # Class distribution
    logger.info("\nTarget distribution:")
    ml_data.groupBy("target").count().show()
    
    return ml_data

def main():
    logger.info("🚀 Starting Feature Engineering")
    logger.info("=" * 60)
    
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    create_ml_features(spark)
    
    logger.info("✅ Feature engineering complete!")
    spark.stop()

if __name__ == "__main__":
    os.makedirs(ML_PATH, exist_ok=True)
    main()
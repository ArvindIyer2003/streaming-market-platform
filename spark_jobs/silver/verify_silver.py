"""
Verify Silver Layer Data
"""
import os
import sys

ROOT_DIR = os.path.dirname(
    os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )
)
sys.path.append(ROOT_DIR)

SILVER_PATH = os.path.join(ROOT_DIR, "data", "delta", "silver")

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("VerifySilver") \
    .master("local[*]") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*60)
print("SILVER LAYER VERIFICATION")
print("="*60)

# Stocks
print("\n📈 STOCK PRICES ENRICHED:")
stocks = spark.read.format("delta") \
    .load(os.path.join(SILVER_PATH, "stock_prices_enriched"))
print(f"Total rows: {stocks.count()}")
print("\nTechnical Indicators Sample:")
stocks.select(
    "symbol",
    "price",
    "volatility",
    "price_direction",
    "is_price_spike",
    "is_high_volume",
    "is_anomaly",
    "timestamp_parsed"
).show(5, truncate=False)

print("\nAnomalies Detected:")
anomalies = stocks.filter("is_anomaly = true")
print(f"Total anomalies: {anomalies.count()}")
if anomalies.count() > 0:
    anomalies.select(
        "symbol",
        "price",
        "price_change_pct",
        "is_price_spike",
        "is_high_volume",
        "volatility",
        "price_direction"
    ).show(5, truncate=False)

# Crypto
print("\n🪙 CRYPTO PRICES ENRICHED:")
crypto = spark.read.format("delta") \
    .load(os.path.join(SILVER_PATH, "crypto_prices_enriched"))
print(f"Total rows: {crypto.count()}")
crypto.select(
    "symbol",
    "price",
    "volatility",
    "price_direction",
    "is_price_spike",
    "is_high_volume",
    "is_anomaly",
    "timestamp_parsed"
).show(5, truncate=False)

# News
print("\n📰 NEWS SENTIMENT CLEAN:")
news = spark.read.format("delta") \
    .load(os.path.join(SILVER_PATH, "news_sentiment_clean"))
print(f"Total rows: {news.count()}")
news.select(
    "symbol", "sentiment_label", "sentiment_score", "headline"
).show(5, truncate=False)

print("\n✅ Silver verification complete!")
spark.stop()
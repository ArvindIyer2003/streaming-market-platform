"""
Verify Bronze Layer Data
Location: spark_jobs/bronze/verify_bronze.py
"""
import os
import sys

# Go up 2 levels to project root
ROOT_DIR = os.path.dirname(
    os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )
)
sys.path.append(ROOT_DIR)

BRONZE_PATH = os.path.join(ROOT_DIR, "data", "delta", "bronze")

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("VerifyBronze") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*60)
print("BRONZE LAYER VERIFICATION")
print("="*60)

# Stocks
print("\n📈 STOCK PRICES:")
stocks = spark.read.format("delta") \
    .load(os.path.join(BRONZE_PATH, "stock_prices"))
print(f"Total rows: {stocks.count()}")
stocks.select("symbol", "price", "price_change_pct",
              "sector", "timestamp").show(5, truncate=False)

# Crypto
print("\n🪙 CRYPTO PRICES:")
crypto = spark.read.format("delta") \
    .load(os.path.join(BRONZE_PATH, "crypto_prices"))
print(f"Total rows: {crypto.count()}")
crypto.select("symbol", "price",
              "market_cap", "timestamp").show(5, truncate=False)

# News
print("\n📰 NEWS SENTIMENT:")
news = spark.read.format("delta") \
    .load(os.path.join(BRONZE_PATH, "news_sentiment"))
print(f"Total rows: {news.count()}")
news.select("symbol", "sentiment_label",
            "sentiment_score", "headline").show(5, truncate=False)

print("\n✅ Bronze verification complete!")
spark.stop()

"""
Setup Snowflake database and tables
"""
import snowflake.connector
import os
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
}

def execute_sql(cursor, sql, description):
    """Execute SQL and log result"""
    try:
        logger.info(f"📝 {description}...")
        cursor.execute(sql)
        logger.info(f"   ✅ Success")
        return True
    except Exception as e:
        if "already exists" in str(e).lower():
            logger.info(f"   ⚠️ Already exists")
            return True
        else:
            logger.error(f"   ❌ Error: {e}")
            return False

def main():
    logger.info("🚀 Setting up Snowflake...")
    logger.info("=" * 60)
    
    # Connect
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    logger.info("✅ Connected to Snowflake")
    
    # 1. Create Database
    execute_sql(cursor, 
        "CREATE DATABASE IF NOT EXISTS MARKET_ANALYTICS",
        "Creating database")
    
    # 2. Use Database
    execute_sql(cursor, 
        "USE DATABASE MARKET_ANALYTICS",
        "Using database")
    
    # 3. Create Warehouse
    execute_sql(cursor,
        """CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH
           WAREHOUSE_SIZE = 'XSMALL'
           AUTO_SUSPEND = 60
           AUTO_RESUME = TRUE
           INITIALLY_SUSPENDED = TRUE""",
        "Creating warehouse")
    
    # 4. Use Warehouse
    execute_sql(cursor,
        "USE WAREHOUSE ANALYTICS_WH",
        "Using warehouse")
    
    # 5. Create Schemas
    for schema in ['RAW', 'STAGING', 'MARTS', 'ML']:
        execute_sql(cursor,
            f"CREATE SCHEMA IF NOT EXISTS {schema}",
            f"Creating schema {schema}")
    
    # 6. Create RAW Tables
    logger.info("\n📊 Creating RAW tables...")
    
    # Stock Trades
    execute_sql(cursor, """
        CREATE TABLE IF NOT EXISTS RAW.STOCK_TRADES (
            SYMBOL VARCHAR(20),
            TIMESTAMP_PARSED TIMESTAMP_NTZ,
            TRADE_DATE DATE,
            SECTOR VARCHAR(50),
            EXCHANGE VARCHAR(20),
            CURRENCY VARCHAR(10),
            PRICE FLOAT,
            VOLUME BIGINT,
            PRICE_CHANGE FLOAT,
            PRICE_CHANGE_PCT FLOAT,
            IS_ANOMALY BOOLEAN,
            IS_PRICE_SPIKE BOOLEAN,
            IS_HIGH_VOLUME BOOLEAN,
            PRICE_DIRECTION VARCHAR(10),
            VOLATILITY FLOAT,
            SOURCE VARCHAR(50),
            LOADED_AT TIMESTAMP_NTZ
        )
    """, "RAW.STOCK_TRADES")
    
    # OHLC Candles
    execute_sql(cursor, """
        CREATE TABLE IF NOT EXISTS RAW.OHLC_CANDLES (
            SYMBOL VARCHAR(20),
            WINDOW_START TIMESTAMP_NTZ,
            WINDOW_END TIMESTAMP_NTZ,
            SECTOR VARCHAR(50),
            EXCHANGE VARCHAR(20),
            CURRENCY VARCHAR(10),
            OPEN FLOAT,
            HIGH FLOAT,
            LOW FLOAT,
            CLOSE FLOAT,
            VOLUME BIGINT,
            AVG_PRICE FLOAT,
            TICK_COUNT INT,
            MAX_CHANGE_PCT FLOAT,
            MIN_CHANGE_PCT FLOAT,
            ANOMALY_COUNT INT,
            CANDLE_COLOR VARCHAR(10),
            CANDLE_RANGE_PCT FLOAT,
            LOADED_AT TIMESTAMP_NTZ
        )
    """, "RAW.OHLC_CANDLES")
    
    # Stock Sentiment
    execute_sql(cursor, """
        CREATE TABLE IF NOT EXISTS RAW.STOCK_SENTIMENT (
            SYMBOL VARCHAR(20),
            TIMESTAMP_PARSED TIMESTAMP_NTZ,
            PRICE FLOAT,
            PRICE_CHANGE_PCT FLOAT,
            NEWS_SENTIMENT FLOAT,
            NEWS_COUNT INT,
            POSITIVE_NEWS_COUNT INT,
            NEGATIVE_NEWS_COUNT INT,
            LOADED_AT TIMESTAMP_NTZ
        )
    """, "RAW.STOCK_SENTIMENT")
    
    # Dim Stocks
    execute_sql(cursor, """
        CREATE TABLE IF NOT EXISTS RAW.DIM_STOCKS (
            SYMBOL VARCHAR(20) PRIMARY KEY,
            NAME VARCHAR(100),
            SECTOR VARCHAR(50),
            EXCHANGE VARCHAR(20),
            CURRENCY VARCHAR(10),
            IS_CURRENT BOOLEAN,
            VALID_FROM TIMESTAMP_NTZ,
            VALID_TO TIMESTAMP_NTZ
        )
    """, "RAW.DIM_STOCKS")
    
    # 7. Create ML Tables
    logger.info("\n🤖 Creating ML tables...")
    
    # ML Features
    execute_sql(cursor, """
        CREATE TABLE IF NOT EXISTS ML.FEATURES (
            SYMBOL VARCHAR(20),
            TIMESTAMP_PARSED TIMESTAMP_NTZ,
            PRICE FLOAT,
            PRICE_CHANGE_PCT FLOAT,
            PRICE_MA_5 FLOAT,
            PRICE_MA_10 FLOAT,
            PRICE_MA_20 FLOAT,
            PRICE_STD_5 FLOAT,
            PRICE_STD_10 FLOAT,
            MOMENTUM_5 FLOAT,
            PRICE_CHANGE_PCT_1 FLOAT,
            RSI FLOAT,
            BB_POSITION FLOAT,
            VOLUME BIGINT,
            VOLUME_RATIO FLOAT,
            SENTIMENT_SCORE FLOAT,
            NEWS_COUNT INT,
            HOUR INT,
            DAY_OF_WEEK INT,
            IS_MARKET_OPEN_HOUR INT,
            TARGET INT,
            PRICE_NEXT FLOAT
        )
    """, "ML.FEATURES")
    
    # ML Predictions
    execute_sql(cursor, """
        CREATE TABLE IF NOT EXISTS ML.PREDICTIONS (
            SYMBOL VARCHAR(20),
            TIMESTAMP_PARSED TIMESTAMP_NTZ,
            PRICE FLOAT,
            PREDICTION INT,
            PREDICTION_LABEL VARCHAR(10),
            PROBABILITY_DOWN FLOAT,
            PROBABILITY_UP FLOAT,
            CONFIDENCE FLOAT,
            ACTUAL INT,
            CORRECT BOOLEAN,
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """, "ML.PREDICTIONS")
    
    # 8. Verify
    logger.info("\n🔍 Verifying setup...")
    cursor.execute("SHOW TABLES IN SCHEMA RAW")
    raw_tables = cursor.fetchall()
    logger.info(f"   RAW schema has {len(raw_tables)} tables")
    
    cursor.execute("SHOW TABLES IN SCHEMA ML")
    ml_tables = cursor.fetchall()
    logger.info(f"   ML schema has {len(ml_tables)} tables")
    
    cursor.close()
    conn.close()
    
    logger.info("=" * 60)
    logger.info("✅ Snowflake setup complete!")
    logger.info(f"   Database: MARKET_ANALYTICS")
    logger.info(f"   Schemas: RAW, STAGING, MARTS, ML")
    logger.info(f"   Tables: {len(raw_tables) + len(ml_tables)} created")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
"""
Crypto Price Producer
Fetches Bitcoin, Ethereum, BNB, Solana prices
Sends to Kafka crypto-prices topic every 10 seconds
Crypto markets are 24/7 - always data available!
"""
import json
import time
import yfinance as yf
from kafka import KafkaProducer
from datetime import datetime
from loguru import logger
import sys
from config import (
    KAFKA_CONFIG,
    CRYPTO_SYMBOLS,
    STOCK_METADATA,
    CRYPTO_POLL_INTERVAL
)

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}",
    level="INFO"
)
logger.add("logs/crypto_producer.log", rotation="1 day")

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: x.encode('utf-8'),
        retries=3,
        acks='all'
    )

def fetch_crypto_price(symbol: str) -> dict | None:
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.fast_info
        price = info.last_price

        if not price:
            return None

        meta = STOCK_METADATA.get(symbol, {})

        return {
            "symbol":           symbol,
            "name":             meta.get('name', symbol),
            "sector":           "Crypto",
            "exchange":         "Crypto",
            "currency":         "USD",
            "price":            round(float(price), 4),
            "day_high":         round(float(info.day_high or 0), 4),
            "day_low":          round(float(info.day_low or 0), 4),
            "prev_close":       round(float(info.previous_close or 0), 4),
            "volume":           int(info.shares or 0),
            "market_cap":       int(info.market_cap or 0),
            "price_change":     round(float(price - (info.previous_close or price)), 4),
            "price_change_pct": round(
                float((price - (info.previous_close or price)) /
                (info.previous_close or price) * 100), 4
            ) if info.previous_close else 0,
            "timestamp":        datetime.now().isoformat(),
            "date":             datetime.now().strftime('%Y-%m-%d'),
            "market_open":      True,  # Crypto never closes!
            "source":           "yfinance"
        }
    except Exception as e:
        logger.error(f"❌ Error fetching crypto {symbol}: {e}")
        return None

def run_crypto_producer():
    logger.info("🚀 Starting Crypto Price Producer")
    logger.info(f"🪙 Tracking: {', '.join(CRYPTO_SYMBOLS)}")
    logger.info(f"⏱️ Polling every {CRYPTO_POLL_INTERVAL} seconds")

    producer = create_producer()
    topic = KAFKA_CONFIG['topics']['crypto']
    messages_sent = 0

    try:
        while True:
            logger.info(f"🔄 Fetching crypto prices... [{datetime.now().strftime('%H:%M:%S')}]")

            for symbol in CRYPTO_SYMBOLS:
                message = fetch_crypto_price(symbol)
                if message:
                    producer.send(
                        topic=topic,
                        key=symbol,
                        value=message
                    )
                    messages_sent += 1
                    logger.info(
                        f"✅ {symbol:<10} "
                        f"${message['price']:>12.4f} "
                        f"Change: {message['price_change_pct']:>+.2f}%"
                    )

            producer.flush()
            logger.info(f"💤 Total sent: {messages_sent} | Sleeping {CRYPTO_POLL_INTERVAL}s...")
            logger.info("─" * 60)
            time.sleep(CRYPTO_POLL_INTERVAL)

    except KeyboardInterrupt:
        logger.info("⏹️ Crypto producer stopped")
    finally:
        producer.close()

if __name__ == "__main__":
    import os
    os.makedirs("logs", exist_ok=True)
    run_crypto_producer()
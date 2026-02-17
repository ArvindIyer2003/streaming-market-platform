"""
Stock Price Producer
Fetches real-time prices for Indian + US stocks
Sends to Kafka stock-prices topic every 5 seconds
"""
import json
import time
import yfinance as yf
from kafka import KafkaProducer
from datetime import datetime
from loguru import logger
import sys

# Import our config

from config import (
    KAFKA_CONFIG,
    INDIAN_STOCKS,
    US_STOCKS,
    STOCK_METADATA,
    STOCK_POLL_INTERVAL
)

# ================================
# LOGGING SETUP
# ================================
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}",
    level="INFO"
)
logger.add(
    "logs/stock_producer.log",
    rotation="1 day",
    retention="7 days",
    level="DEBUG"
)

# ================================
# KAFKA PRODUCER SETUP
# ================================
def create_producer():
    """Create and return Kafka producer"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            # Serialize messages to JSON bytes
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            # Use symbol as key for partition routing
            key_serializer=lambda x: x.encode('utf-8'),
            # Retry settings
            retries=3,
            retry_backoff_ms=1000,
            # Performance settings
            batch_size=16384,
            linger_ms=10,
            # Acknowledgment (wait for Kafka to confirm)
            acks='all'
        )
        logger.success("✅ Kafka producer connected!")
        return producer
    except Exception as e:
        logger.error(f"❌ Failed to connect to Kafka: {e}")
        raise

# ================================
# FETCH STOCK PRICE
# ================================
def fetch_stock_price(symbol: str) -> dict | None:
    """
    Fetch current price for a stock symbol
    Returns dict with price data or None if failed
    """
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.fast_info

        # Get current price
        price = info.last_price
        if price is None or price == 0:
            logger.warning(f"⚠️ No price data for {symbol}")
            return None

        # Get metadata
        meta = STOCK_METADATA.get(symbol, {})

        # Build message
        message = {
            # Identification
            "symbol":        symbol,
            "name":          meta.get('name', symbol),
            "sector":        meta.get('sector', 'Unknown'),
            "exchange":      meta.get('exchange', 'Unknown'),
            "currency":      meta.get('currency', 'USD'),

            # Price data
            "price":         round(float(price), 2),
            "open":          round(float(info.open or 0), 2),
            "day_high":      round(float(info.day_high or 0), 2),
            "day_low":       round(float(info.day_low or 0), 2),
            "prev_close":    round(float(info.previous_close or 0), 2),
            "volume":        int(info.shares or 0),

            # Calculated fields
            "price_change":  round(float(price - (info.previous_close or price)), 2),
            "price_change_pct": round(
                float((price - (info.previous_close or price)) /
                (info.previous_close or price) * 100), 4
            ) if info.previous_close else 0,

            # Timestamps
            "timestamp":     datetime.now().isoformat(),
            "date":          datetime.now().strftime('%Y-%m-%d'),
            "market_hours":  is_market_open(meta.get('exchange', 'NYSE')),

            # Source
            "source": "yfinance"
        }
        return message

    except Exception as e:
        logger.error(f"❌ Error fetching {symbol}: {e}")
        return None

# ================================
# MARKET HOURS CHECK
# ================================
def is_market_open(exchange: str) -> bool:
    """
    Simple check if market is likely open
    NSE: 9:15 AM - 3:30 PM IST (Mon-Fri)
    NYSE: 9:30 AM - 4:00 PM EST (Mon-Fri)
    Crypto: Always open
    """
    now = datetime.now()

    # Weekend check
    if now.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return False

    if exchange == 'Crypto':
        return True

    hour = now.hour
    if exchange == 'NSE':
        # IST: 9:15 - 15:30
        return (9 <= hour < 16)
    else:
        # EST approximation
        return (9 <= hour < 17)

# ================================
# SEND TO KAFKA
# ================================
def send_to_kafka(producer, message: dict, topic: str):
    """Send message to Kafka topic"""
    try:
        future = producer.send(
            topic=topic,
            key=message['symbol'],
            value=message
        )
        # Wait for confirmation
        record_metadata = future.get(timeout=10)
        logger.debug(
            f"📤 Sent {message['symbol']} "
            f"price={message['price']} "
            f"→ partition={record_metadata.partition} "
            f"offset={record_metadata.offset}"
        )
        return True
    except Exception as e:
        logger.error(f"❌ Failed to send {message['symbol']}: {e}")
        return False

# ================================
# MAIN PRODUCER LOOP
# ================================
def run_stock_producer():
    """
    Main loop - fetches prices and sends to Kafka
    Runs forever until interrupted
    """
    logger.info("🚀 Starting Stock Price Producer")
    logger.info(f"📊 Tracking {len(INDIAN_STOCKS)} Indian stocks + {len(US_STOCKS)} US stocks")
    logger.info(f"⏱️ Polling every {STOCK_POLL_INTERVAL} seconds")

    # Create Kafka producer
    producer = create_producer()

    # Track statistics
    stats = {
        'messages_sent': 0,
        'messages_failed': 0,
        'start_time': datetime.now()
    }

    # All stocks to track
    all_stocks = INDIAN_STOCKS + US_STOCKS
    topic = KAFKA_CONFIG['topics']['stocks']

    logger.info(f"📈 Stocks: {', '.join(all_stocks)}")
    logger.info("─" * 60)

    try:
        while True:
            cycle_start = time.time()
            logger.info(f"🔄 Fetching prices... [{datetime.now().strftime('%H:%M:%S')}]")

            # Fetch and send each stock
            for symbol in all_stocks:
                message = fetch_stock_price(symbol)

                if message:
                    success = send_to_kafka(producer, message, topic)
                    if success:
                        stats['messages_sent'] += 1
                        logger.info(
                            f"✅ {symbol:<15} "
                            f"Price: {message['currency']} {message['price']:>10.2f} "
                            f"Change: {message['price_change_pct']:>+.2f}%"
                        )
                    else:
                        stats['messages_failed'] += 1
                else:
                    stats['messages_failed'] += 1

            # Flush all messages
            producer.flush()

            # Print stats every 10 cycles
            if stats['messages_sent'] % (len(all_stocks) * 10) == 0:
                runtime = (datetime.now() - stats['start_time']).seconds
                logger.info(
                    f"📊 Stats: Sent={stats['messages_sent']} "
                    f"Failed={stats['messages_failed']} "
                    f"Runtime={runtime}s"
                )

            # Wait for next cycle
            cycle_time = time.time() - cycle_start
            sleep_time = max(0, STOCK_POLL_INTERVAL - cycle_time)
            logger.info(f"💤 Sleeping {sleep_time:.1f}s...")
            logger.info("─" * 60)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("⏹️ Producer stopped by user")
    finally:
        producer.close()
        logger.info(
            f"📊 Final Stats: "
            f"Sent={stats['messages_sent']} "
            f"Failed={stats['messages_failed']}"
        )

# ================================
# ENTRY POINT
# ================================
if __name__ == "__main__":
    # Create logs directory
    import os
    os.makedirs("logs", exist_ok=True)
    os.makedirs("../data/delta", exist_ok=True)

    run_stock_producer()
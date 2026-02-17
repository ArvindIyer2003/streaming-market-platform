"""
Run all 3 producers simultaneously
Uses threading to run in parallel
"""
import threading
import time
from loguru import logger
from .stock_producer import run_stock_producer
from .crypto_producer import run_crypto_producer
from .news_producer import run_news_producer

def run_in_thread(func, name):
    """Run a producer function in a thread"""
    thread = threading.Thread(
        target=func,
        name=name,
        daemon=True
    )
    thread.start()
    logger.info(f"🧵 Started thread: {name}")
    return thread

if __name__ == "__main__":
    logger.info("🚀 Starting ALL producers...")
    logger.info("=" * 60)

    # Start all producers in parallel threads
    threads = [
        run_in_thread(run_stock_producer,  "StockProducer"),
        run_in_thread(run_crypto_producer, "CryptoProducer"),
        run_in_thread(run_news_producer,   "NewsProducer"),
    ]

    logger.info("=" * 60)
    logger.info("✅ All producers running!")
    logger.info("📊 Check Kafka UI at http://localhost:8080")
    logger.info("Press Ctrl+C to stop all producers")

    try:
        # Keep main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("⏹️ Stopping all producers...")
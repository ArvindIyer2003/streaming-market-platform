"""
News Sentiment Producer
Fetches financial news headlines
Calculates sentiment score using simple NLP
Sends to Kafka news-sentiment topic every 5 minutes
"""
import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime
from loguru import logger
import sys
from config import (
    KAFKA_CONFIG,
    NEWS_API_KEY,
    NEWS_KEYWORDS,
    ALL_SYMBOLS,
    NEWS_POLL_INTERVAL
)

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}",
    level="INFO"
)
logger.add("logs/news_producer.log", rotation="1 day")

# ================================
# SIMPLE SENTIMENT ANALYSIS
# ================================
# Positive and negative financial keywords
POSITIVE_WORDS = [
    'surge', 'jump', 'gain', 'rise', 'rally', 'growth',
    'profit', 'beat', 'record', 'strong', 'bullish',
    'upgrade', 'buy', 'positive', 'up', 'high', 'win',
    'success', 'expand', 'revenue', 'earnings beat'
]

NEGATIVE_WORDS = [
    'fall', 'drop', 'decline', 'loss', 'crash', 'weak',
    'miss', 'concern', 'risk', 'sell', 'bearish', 'down',
    'low', 'fail', 'cut', 'downgrade', 'negative', 'fear',
    'uncertainty', 'layoff', 'debt', 'earnings miss'
]

def calculate_sentiment(text: str) -> dict:
    if not text:
        return {'score': 0, 'label': 'neutral', 'positive': 0, 'negative': 0}

    text_lower = text.lower()

    positive_count = sum(1 for word in POSITIVE_WORDS if word in text_lower)
    negative_count = sum(1 for word in NEGATIVE_WORDS if word in text_lower)
    total_words = len(text_lower.split())

    # NEW: Normalize by total words (not just sentiment words)
    if total_words == 0:
        score = 0.0
    else:
        score = round((positive_count - negative_count) / (total_words * 0.1), 4)
        score = max(-1.0, min(1.0, score))  # Clamp between -1 and +1

    if score > 0.1:
        label = 'positive'
    elif score < -0.1:
        label = 'negative'
    else:
        label = 'neutral'

    return {
        'score':    score,
        'label':    label,
        'positive': positive_count,
        'negative': negative_count
    }

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: x.encode('utf-8'),
        retries=3,
        acks='all'
    )

def fetch_news_for_symbol(symbol: str) -> list:
    """
    Fetch news articles for a stock symbol
    Returns list of news messages with sentiment
    """
    keywords = NEWS_KEYWORDS.get(symbol, [symbol])
    query = ' OR '.join(keywords)

    try:
        url = 'https://newsapi.org/v2/everything'
        params = {
            'q':        query,
            'apiKey':   NEWS_API_KEY,
            'language': 'en',
            'sortBy':   'publishedAt',
            'pageSize': 5  # 5 articles per stock
        }

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        articles = response.json().get('articles', [])

        messages = []
        for article in articles:
            # Combine title + description for sentiment
            text = f"{article.get('title', '')} {article.get('description', '')}"
            sentiment = calculate_sentiment(text)

            message = {
                "symbol":           symbol,
                "headline":         article.get('title', ''),
                "description":      article.get('description', ''),
                "source":           article.get('source', {}).get('name', ''),
                "url":              article.get('url', ''),
                "published_at":     article.get('publishedAt', ''),
                "sentiment_score":  sentiment['score'],
                "sentiment_label":  sentiment['label'],
                "positive_words":   sentiment['positive'],
                "negative_words":   sentiment['negative'],
                "timestamp":        datetime.now().isoformat(),
                "date":             datetime.now().strftime('%Y-%m-%d')
            }
            messages.append(message)

        return messages

    except Exception as e:
        logger.error(f"❌ News fetch failed for {symbol}: {e}")
        return []

def run_news_producer():
    logger.info("🚀 Starting News Sentiment Producer")
    logger.info(f"📰 Tracking news for {len(ALL_SYMBOLS)} symbols")
    logger.info(f"⏱️ Polling every {NEWS_POLL_INTERVAL/60:.0f} minutes")

    producer = create_producer()
    topic = KAFKA_CONFIG['topics']['news']
    messages_sent = 0

    try:
        while True:
            logger.info(f"🔄 Fetching news... [{datetime.now().strftime('%H:%M:%S')}]")

            for symbol in ALL_SYMBOLS:
                articles = fetch_news_for_symbol(symbol)

                for article in articles:
                    producer.send(
                        topic=topic,
                        key=symbol,
                        value=article
                    )
                    messages_sent += 1
                    logger.info(
                        f"📰 {symbol:<15} "
                        f"Sentiment: {article['sentiment_label']:<10} "
                        f"Score: {article['sentiment_score']:>+.2f} "
                        f"| {article['headline'][:50]}..."
                    )

                # Small delay to avoid rate limiting
                time.sleep(1)

            producer.flush()
            logger.info(f"✅ Total news messages sent: {messages_sent}")
            logger.info(f"💤 Sleeping {NEWS_POLL_INTERVAL/60:.0f} minutes...")
            logger.info("─" * 60)
            time.sleep(NEWS_POLL_INTERVAL)

    except KeyboardInterrupt:
        logger.info("⏹️ News producer stopped")
    finally:
        producer.close()

if __name__ == "__main__":
    import os
    os.makedirs("logs", exist_ok=True)
    run_news_producer()
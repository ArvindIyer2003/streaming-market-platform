"""
Central configuration for all producers
Reads from .env file
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ================================
# KAFKA CONFIG
# ================================
KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'topics': {
        'stocks': os.getenv('KAFKA_STOCK_TOPIC', 'stock-prices'),
        'crypto': os.getenv('KAFKA_CRYPTO_TOPIC', 'crypto-prices'),
        'news': os.getenv('KAFKA_NEWS_TOPIC', 'news-sentiment')
    }
}

# ================================
# SYMBOLS
# ================================
INDIAN_STOCKS = os.getenv('INDIAN_STOCKS', '').split(',')
US_STOCKS = os.getenv('US_STOCKS', '').split(',')
CRYPTO_SYMBOLS = os.getenv('CRYPTO_SYMBOLS', '').split(',')
ALL_SYMBOLS = os.getenv('ALL_SYMBOLS', '').split(',')

# ================================
# POLLING INTERVALS
# ================================
STOCK_POLL_INTERVAL = int(os.getenv('STOCK_POLL_INTERVAL', 5))
CRYPTO_POLL_INTERVAL = int(os.getenv('CRYPTO_POLL_INTERVAL', 10))
NEWS_POLL_INTERVAL = int(os.getenv('NEWS_POLL_INTERVAL', 300))

# ================================
# NEWS API
# ================================
NEWS_API_KEY = os.getenv('NEWS_API_KEY', '')

# ================================
# STOCK METADATA
# ================================
STOCK_METADATA = {
    # Indian IT
    'TCS.NS':       {'name': 'Tata Consultancy Services', 'sector': 'IT',      'exchange': 'NSE', 'currency': 'INR'},
    'INFY.NS':      {'name': 'Infosys',                   'sector': 'IT',      'exchange': 'NSE', 'currency': 'INR'},
    'WIPRO.NS':     {'name': 'Wipro',                     'sector': 'IT',      'exchange': 'NSE', 'currency': 'INR'},
    'TECHM.NS':     {'name': 'Tech Mahindra',             'sector': 'IT',      'exchange': 'NSE', 'currency': 'INR'},
    # Indian Banking
    'HDFCBANK.NS':  {'name': 'HDFC Bank',                 'sector': 'Banking', 'exchange': 'NSE', 'currency': 'INR'},
    'ICICIBANK.NS': {'name': 'ICICI Bank',                'sector': 'Banking', 'exchange': 'NSE', 'currency': 'INR'},
    'SBIN.NS':      {'name': 'State Bank of India',       'sector': 'Banking', 'exchange': 'NSE', 'currency': 'INR'},
    # Indian Others
    'RELIANCE.NS':  {'name': 'Reliance Industries',       'sector': 'Energy',  'exchange': 'NSE', 'currency': 'INR'},
    'ADANIENT.NS':  {'name': 'Adani Enterprises',         'sector': 'Energy',  'exchange': 'NSE', 'currency': 'INR'},
    'TATAMOTORS.NS':{'name': 'Tata Motors',               'sector': 'Auto',    'exchange': 'NSE', 'currency': 'INR'},
    # US Tech
    'AAPL':         {'name': 'Apple',                     'sector': 'Tech',    'exchange': 'NYSE','currency': 'USD'},
    'GOOGL':        {'name': 'Alphabet',                  'sector': 'Tech',    'exchange': 'NYSE','currency': 'USD'},
    'MSFT':         {'name': 'Microsoft',                 'sector': 'Tech',    'exchange': 'NYSE','currency': 'USD'},
    'NVDA':         {'name': 'Nvidia',                    'sector': 'Tech',    'exchange': 'NYSE','currency': 'USD'},
    'AMZN':         {'name': 'Amazon',                    'sector': 'Tech',    'exchange': 'NYSE','currency': 'USD'},
    'META':         {'name': 'Meta',                      'sector': 'Tech',    'exchange': 'NYSE','currency': 'USD'},
    # Crypto
    'BTC-USD':      {'name': 'Bitcoin',                   'sector': 'Crypto',  'exchange': 'Crypto', 'currency': 'USD'},
    'ETH-USD':      {'name': 'Ethereum',                  'sector': 'Crypto',  'exchange': 'Crypto', 'currency': 'USD'},
    'BNB-USD':      {'name': 'Binance Coin',              'sector': 'Crypto',  'exchange': 'Crypto', 'currency': 'USD'},
    'SOL-USD':      {'name': 'Solana',                    'sector': 'Crypto',  'exchange': 'Crypto', 'currency': 'USD'},
}

# ================================
# NEWS KEYWORDS (for each stock)
# ================================
NEWS_KEYWORDS = {
    'TCS.NS':        ['TCS', 'Tata Consultancy'],
    'INFY.NS':       ['Infosys', 'INFY'],
    'WIPRO.NS':      ['Wipro'],
    'TECHM.NS':      ['Tech Mahindra'],
    'HDFCBANK.NS':   ['HDFC Bank'],
    'ICICIBANK.NS':  ['ICICI Bank'],
    'SBIN.NS':       ['SBI', 'State Bank'],
    'RELIANCE.NS':   ['Reliance Industries', 'Jio'],
    'ADANIENT.NS':   ['Adani'],
    'TATAMOTORS.NS': ['Tata Motors'],
    'AAPL':          ['Apple', 'iPhone'],
    'GOOGL':         ['Google', 'Alphabet'],
    'MSFT':          ['Microsoft'],
    'NVDA':          ['Nvidia', 'NVDA'],
    'AMZN':          ['Amazon', 'AWS'],
    'META':          ['Meta', 'Facebook'],
    'BTC-USD':       ['Bitcoin', 'BTC'],
    'ETH-USD':       ['Ethereum', 'ETH'],
    'BNB-USD':       ['Binance', 'BNB'],
    'SOL-USD':       ['Solana', 'SOL'],
}
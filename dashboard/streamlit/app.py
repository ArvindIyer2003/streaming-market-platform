"""
Real-Time Financial Analytics Dashboard
Connects to Gold Delta Lake tables and displays live data
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import time
import os
import sys

# Add root to path
ROOT_DIR = os.path.dirname(
    os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )
)
sys.path.append(ROOT_DIR)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ================================
# SPARK SESSION
# ================================
@st.cache_resource
def get_spark():
    """Create Spark session (cached)"""
    spark = SparkSession.builder \
        .appName("Dashboard") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# ================================
# DATA LOADING FUNCTIONS
# ================================
GOLD_PATH = os.path.join(ROOT_DIR, "data", "delta", "gold")

def load_latest_trades(spark, limit=1000):
    """Load latest stock trades"""
    path = os.path.join(GOLD_PATH, "fact_stock_trades")
    
    df = spark.read.format("delta").load(path) \
        .limit(limit) \
        .toPandas()
    
    return df

def load_ohlc_candles(spark, symbol=None, hours=24):
    """Load OHLC candles"""
    path = os.path.join(GOLD_PATH, "fact_ohlc_candles")
    
    df = spark.read.format("delta").load(path)
    
    # Filter by time
    cutoff = datetime.now() - timedelta(hours=hours)
    df = df.filter(F.col("window_start") >= cutoff)
    
    # Filter by symbol if provided
    if symbol:
        df = df.filter(F.col("symbol") == symbol)
    
    return df.orderBy("window_start").toPandas()

def load_hourly_metrics(spark, hours=24):
    """Load hourly aggregated metrics"""
    path = os.path.join(GOLD_PATH, "hourly_metrics")
    
    cutoff = datetime.now() - timedelta(hours=hours)
    
    df = spark.read.format("delta").load(path) \
        .filter(F.col("hour_start") >= cutoff) \
        .orderBy("hour_start") \
        .toPandas()
    
    return df

def load_stock_sentiment(spark, limit=500):
    """Load stock prices with sentiment"""
    path = os.path.join(GOLD_PATH, "stock_with_sentiment")
    
    df = spark.read.format("delta").load(path) \
        .orderBy(F.col("timestamp_parsed").desc()) \
        .limit(limit) \
        .toPandas()
    
    return df

def load_dim_stocks(spark):
    """Load stock dimension"""
    path = os.path.join(GOLD_PATH, "dim_stocks")
    return spark.read.format("delta").load(path).toPandas()

# ================================
# DASHBOARD CONFIG
# ================================
st.set_page_config(
    page_title="Real-Time Market Analytics",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .big-metric {
        font-size: 2.5rem;
        font-weight: bold;
    }
    .positive {
        color: #00ff00;
    }
    .negative {
        color: #ff0000;
    }
    .neutral {
        color: #ffff00;
    }
</style>
""", unsafe_allow_html=True)

# ================================
# SIDEBAR
# ================================
st.sidebar.title("📊 Market Analytics")
st.sidebar.markdown("---")

# Auto-refresh toggle
auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=True)

if auto_refresh:
    st.sidebar.info("Dashboard refreshing every 30 seconds")
    time.sleep(30)
    st.rerun()

# Page selection
page = st.sidebar.radio(
    "Navigate",
    ["🏠 Live Overview", "📈 Technical Charts", "📰 News Sentiment", "📊 Analytics"]
)

st.sidebar.markdown("---")
st.sidebar.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")

# ================================
# MAIN APP
# ================================
spark = get_spark()

# ================================
# PAGE 1: LIVE OVERVIEW
# ================================
if page == "🏠 Live Overview":
    st.title("🏠 Live Market Overview")
    st.markdown("---")
    
    # Load latest data
    trades_df = load_latest_trades(spark, limit=100)
    
    if trades_df.empty:
        st.warning("No data available yet. Make sure the pipeline is running!")
    else:
        # Get latest price per symbol
        latest_prices = trades_df.sort_values('trade_timestamp').groupby('symbol').last().reset_index()
        
        # Top metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Stocks",
                len(latest_prices),
                delta=None
            )
        
        with col2:
            avg_change = latest_prices['price_change_pct'].mean()
            st.metric(
                "Avg Change %",
                f"{avg_change:.2f}%",
                delta=f"{avg_change:.2f}%"
            )
        
        with col3:
            gainers = len(latest_prices[latest_prices['price_change_pct'] > 0])
            st.metric(
                "Gainers",
                gainers,
                delta=None
            )
        
        with col4:
            losers = len(latest_prices[latest_prices['price_change_pct'] < 0])
            st.metric(
                "Losers",
                losers,
                delta=None
            )
        
        st.markdown("---")
        
        # Top gainers and losers
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🔥 Top Gainers")
            gainers_df = latest_prices.nlargest(5, 'price_change_pct')[
                ['symbol', 'price', 'price_change_pct', 'sector']
            ]
            
            for _, row in gainers_df.iterrows():
                st.markdown(f"""
                <div style='padding: 10px; margin: 5px; background-color: #1e3a1e; border-radius: 5px;'>
                    <strong>{row['symbol']}</strong> ({row['sector']})<br>
                    <span class='big-metric positive'>${row['price']:.2f}</span>
                    <span class='positive'>+{row['price_change_pct']:.2f}%</span>
                </div>
                """, unsafe_allow_html=True)
        
        with col2:
            st.subheader("📉 Top Losers")
            losers_df = latest_prices.nsmallest(5, 'price_change_pct')[
                ['symbol', 'price', 'price_change_pct', 'sector']
            ]
            
            for _, row in losers_df.iterrows():
                st.markdown(f"""
                <div style='padding: 10px; margin: 5px; background-color: #3a1e1e; border-radius: 5px;'>
                    <strong>{row['symbol']}</strong> ({row['sector']})<br>
                    <span class='big-metric negative'>${row['price']:.2f}</span>
                    <span class='negative'>{row['price_change_pct']:.2f}%</span>
                </div>
                """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Heatmap
        st.subheader("🌡️ Market Heatmap")
        
        fig = px.treemap(
            latest_prices,
            path=['sector', 'symbol'],
            values='volume',
            color='price_change_pct',
            color_continuous_scale=['red', 'yellow', 'green'],
            color_continuous_midpoint=0,
            title="Stock Performance by Sector (size=volume, color=% change)"
        )
        
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
        
        # Live price table
        st.subheader("📊 All Stocks - Live Prices")
        
        display_df = latest_prices[[
            'symbol', 'sector', 'price', 'price_change', 'price_change_pct',
            'day_high', 'day_low', 'volume', 'is_anomaly'
        ]].sort_values('price_change_pct', ascending=False)
        
        # Color code the dataframe
        def color_change(val):
            color = 'green' if val > 0 else 'red' if val < 0 else 'gray'
            return f'color: {color}'
        
        st.dataframe(
            display_df.style.applymap(color_change, subset=['price_change_pct']),
            use_container_width=True,
            height=400
        )

# ================================
# PAGE 2: TECHNICAL CHARTS
# ================================
elif page == "📈 Technical Charts":
    st.title("📈 Technical Analysis")
    st.markdown("---")
    
    # Stock selector
    dim_stocks = load_dim_stocks(spark)
    stock_symbols = sorted(dim_stocks['symbol'].tolist())
    
    selected_symbol = st.selectbox("Select Stock", stock_symbols, index=0)
    
    hours = st.slider("Time Range (hours)", 1, 24, 6)
    
    # Load OHLC data
    ohlc_df = load_ohlc_candles(spark, symbol=selected_symbol, hours=hours)
    
    if ohlc_df.empty:
        st.warning(f"No OHLC data for {selected_symbol}")
    else:
        # Candlestick chart
        st.subheader(f"{selected_symbol} - Candlestick Chart")
        
        fig = go.Figure(data=[go.Candlestick(
            x=ohlc_df['window_start'],
            open=ohlc_df['open'],
            high=ohlc_df['high'],
            low=ohlc_df['low'],
            close=ohlc_df['close'],
            name=selected_symbol
        )])
        
        fig.update_layout(
            xaxis_title="Time",
            yaxis_title="Price",
            height=500,
            xaxis_rangeslider_visible=False
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Volume chart
        st.subheader("Volume")
        
        fig_vol = px.bar(
            ohlc_df,
            x='window_start',
            y='volume',
            color='candle_color',
            color_discrete_map={'GREEN': 'green', 'RED': 'red', 'DOJI': 'gray'}
        )
        
        fig_vol.update_layout(height=300)
        st.plotly_chart(fig_vol, use_container_width=True)
        
        # Stats
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Current Price", f"${ohlc_df.iloc[-1]['close']:.2f}")
        
        with col2:
            st.metric("24h High", f"${ohlc_df['high'].max():.2f}")
        
        with col3:
            st.metric("24h Low", f"${ohlc_df['low'].min():.2f}")
        
        with col4:
            st.metric("Total Volume", f"{ohlc_df['volume'].sum():,.0f}")

# ================================
# PAGE 3: NEWS SENTIMENT
# ================================
elif page == "📰 News Sentiment":
    st.title("📰 News Sentiment Analysis")
    st.markdown("---")
    
    # Load sentiment data
    sentiment_df = load_stock_sentiment(spark, limit=500)
    
    if sentiment_df.empty:
        st.warning("No sentiment data available")
    else:
        # Sentiment distribution
        st.subheader("Sentiment Distribution")
        
        sentiment_counts = sentiment_df.groupby(['symbol', 'news_sentiment']).size().reset_index(name='count')
        
        fig = px.bar(
            sentiment_counts,
            x='symbol',
            y='count',
            color='news_sentiment',
            color_continuous_scale=['red', 'yellow', 'green'],
            title="News Sentiment by Stock"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Sentiment vs Price correlation
        st.subheader("Sentiment vs Price Movement")
        
        fig_scatter = px.scatter(
            sentiment_df,
            x='news_sentiment',
            y='price_change_pct',
            color='symbol',
            size='news_count',
            hover_data=['symbol', 'price', 'timestamp_parsed'],
            title="Does positive news correlate with price increases?"
        )
        
        st.plotly_chart(fig_scatter, use_container_width=True)
        
        # Latest prices with sentiment
        st.subheader("Stock Prices with Sentiment Scores")
        
        latest_sentiment = sentiment_df.sort_values('timestamp_parsed').groupby('symbol').last().reset_index()
        
        display_df = latest_sentiment[[
            'symbol', 'price', 'price_change_pct', 
            'news_sentiment', 'news_count',
            'positive_news_count', 'negative_news_count'
        ]].sort_values('news_sentiment', ascending=False)
        
        st.dataframe(display_df, use_container_width=True, height=400)

# ================================
# PAGE 4: ANALYTICS
# ================================
elif page == "📊 Analytics":
    st.title("📊 Detailed Analytics")
    st.markdown("---")
    
    # Load hourly metrics
    hourly_df = load_hourly_metrics(spark, hours=24)
    
    if hourly_df.empty:
        st.warning("No hourly metrics available")
    else:
        st.subheader("Hourly Trading Activity")
        
        # Trading activity over time
        fig = px.line(
            hourly_df,
            x='hour_start',
            y='tick_count',
            color='symbol',
            title="Trading Activity (ticks per hour)"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Volatility ranking
        st.subheader("Volatility Rankings (24h)")
        
        latest_hourly = hourly_df.sort_values('hour_start').groupby('symbol').last().reset_index()
        volatility_rank = latest_hourly.nlargest(10, 'price_stddev')[
            ['symbol', 'sector', 'price_stddev', 'max_change_pct', 'min_change_pct']
        ]
        
        fig_vol = px.bar(
            volatility_rank,
            x='symbol',
            y='price_stddev',
            color='sector',
            title="Most Volatile Stocks (by price std deviation)"
        )
        
        st.plotly_chart(fig_vol, use_container_width=True)
        
        # Anomaly detection
        st.subheader("Anomaly Detection Timeline")
        
        anomaly_df = hourly_df[hourly_df['anomaly_count'] > 0]
        
        if not anomaly_df.empty:
            fig_anom = px.scatter(
                anomaly_df,
                x='hour_start',
                y='symbol',
                size='anomaly_count',
                color='anomaly_count',
                title="Anomalies Detected (size = count)"
            )
            
            st.plotly_chart(fig_anom, use_container_width=True)
        else:
            st.info("No anomalies detected in the last 24 hours")

# ================================
# FOOTER
# ================================
st.markdown("---")
st.caption("Real-Time Financial Analytics Platform | Data refreshes every 5 seconds")
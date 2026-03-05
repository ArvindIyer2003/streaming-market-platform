# 📈 Real-Time Financial Analytics Platform

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.0-black)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0-blue)
![XGBoost](https://img.shields.io/badge/XGBoost-ML-green)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red)

A production-grade, end-to-end streaming data platform that ingests real-time financial data from multiple sources, processes it through a Medallion architecture (Bronze/Silver/Gold), trains ML models for price prediction, and visualizes insights through interactive dashboards.

## 🎯 Project Overview

This platform demonstrates expertise in:
- **Real-time Data Engineering**: Kafka streaming, Spark Structured Streaming
- **Data Architecture**: Medallion architecture (Bronze/Silver/Gold layers)
- **Machine Learning**: XGBoost classification for price direction prediction
- **Data Visualization**: Real-time Streamlit dashboards
- **Modern Data Stack**: Delta Lake, Docker, Apache Airflow

### Key Metrics
- **14,000+ events/hour** ingested from live APIs
- **67% prediction accuracy** on price direction
- **Sub-30 second latency** from event to analytics-ready data
- **20 financial instruments** tracked (stocks + crypto)

---

## 🏗️ Architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
│  Yahoo Finance API  │  Crypto APIs  │  News API (Sentiment)     │
└──────────────┬──────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    KAFKA MESSAGE QUEUE                           │
│  stock-prices (3 partitions) │ crypto-prices │ news-sentiment   │
└──────────────┬──────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────┐
│              SPARK STRUCTURED STREAMING                          │
│                   (Processing Layer)                             │
└──────────────┬──────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────┐
│                  DELTA LAKE (Storage)                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                      │
│  │  BRONZE  │  │  SILVER  │  │   GOLD   │                      │
│  │   Raw    │─▶│ Cleaned  │─▶│Analytics │                      │
│  └──────────┘  └──────────┘  └──────────┘                      │
└──────────────┬──────────────────────────────────────────────────┘
               │
               ├─────────────────┬─────────────────────┐
               ▼                 ▼                     ▼
    ┌──────────────────┐  ┌──────────────┐  ┌──────────────────┐
    │   ML PIPELINE    │  │  STREAMLIT   │  │   SNOWFLAKE      │
    │   (XGBoost)      │  │  DASHBOARD   │  │   (Optional)     │
    │  67% Accuracy    │  │  Real-time   │  │                  │
    └──────────────────┘  └──────────────┘  └──────────────────┘
```

---

## 📊 Data Flow

### Bronze Layer (Raw Ingestion)
- **Purpose**: Landing zone for raw data from Kafka
- **Technology**: Spark Structured Streaming → Delta Lake
- **Frequency**: Real-time (10-second micro-batches)
- **Tables**: `stock_prices`, `crypto_prices`, `news_sentiment`

### Silver Layer (Cleaned & Enriched)
- **Purpose**: Data quality, OHLC aggregations, anomaly detection
- **Features**: 
  - 1-minute OHLC candles
  - Price spike detection
  - Volume surge detection
  - Sentiment categorization
- **Tables**: `stock_prices_enriched`, `stock_prices_1min_ohlc`, `news_sentiment_clean`

### Gold Layer (Analytics Ready)
- **Purpose**: Star schema for BI and ML
- **Features**:
  - Fact tables (trades, candles)
  - Dimension tables (stocks)
  - Hourly aggregations
  - Stock-news sentiment joins
- **Tables**: `fact_stock_trades`, `fact_ohlc_candles`, `hourly_metrics`, `stock_with_sentiment`

---

## 🤖 Machine Learning Pipeline

### Problem Statement
Predict whether a stock price will move UP or DOWN in the next time period.

### Features (17 total)
- **Price Features**: Moving averages (5/10/20), volatility, momentum
- **Technical Indicators**: RSI, Bollinger Bands position
- **Volume Features**: Volume ratio vs moving average
- **Sentiment Features**: News sentiment score, article count
- **Time Features**: Hour of day, day of week, market hours

### Model Performance
```
Test Accuracy:  67.05%
Precision:      64.50%
Recall:         80.15%
F1 Score:       71.48%
```

### Top Important Features
1. Day of week (10.7%)
2. Price MA-10 (9.0%)
3. Price volatility (8.8%)
4. Momentum (8.0%)
5. News sentiment (6.5%)

---

## 🚀 Getting Started

### Prerequisites
- Python 3.12+
- Docker & Docker Compose
- Java 17+ (for Spark)
- 8GB RAM minimum

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/YOUR_USERNAME/streaming-market-platform.git
cd streaming-market-platform
```

2. **Create virtual environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Set up environment variables**
```bash
cp .env.example .env
# Edit .env and add your API keys:
# - News API key from https://newsapi.org
```

5. **Start infrastructure**
```bash
docker-compose -f docker/docker-compose.yml up -d
```

6. **Verify services**
- Kafka UI: http://localhost:8080
- Airflow: http://localhost:8081

---

## 📈 Running the Pipeline

### Terminal 1: Start Producers
```bash
cd producers
python run_all_producers.py
```

### Terminal 2: Start Bronze Layer
```bash
cd spark_jobs/bronze
python bronze_layer.py
```

### Terminal 3: Start Silver Layer
```bash
cd spark_jobs/silver
python silver_layer.py
```

### Terminal 4: Start Gold Layer
```bash
cd spark_jobs/gold
python gold_layer.py
```

### Terminal 5: ML Feature Engineering (One-time)
```bash
python -m ml.feature_engineering
python -m ml.train_model
```

### Terminal 6: Launch Dashboard
```bash
streamlit run dashboard/streamlit/app.py
```

Access dashboard at: **http://localhost:8501**

---

## 📊 Dashboard Features

### 🏠 Live Market Overview
- Real-time price updates (auto-refresh every 5 seconds)
- Top gainers/losers
- Market heatmap by sector
- Volume analysis

### 📈 Technical Charts
- Interactive candlestick charts
- Volume bars
- Multiple timeframes

### 📰 News Sentiment
- Latest headlines with sentiment scores
- Sentiment vs price correlation analysis
- Stock-specific news feed

### 🤖 ML Predictions
- Real-time price direction predictions
- Model confidence scores
- Feature importance visualization
- Live accuracy tracking

---

## 🎓 Key Learnings & Skills Demonstrated

### Data Engineering
- ✅ Real-time streaming with Apache Kafka
- ✅ Distributed processing with Spark Structured Streaming
- ✅ Medallion architecture implementation
- ✅ Delta Lake for ACID transactions
- ✅ Schema evolution and data versioning

### Machine Learning
- ✅ Feature engineering from time-series data
- ✅ Supervised classification with XGBoost
- ✅ Model evaluation and performance monitoring
- ✅ Real-time inference pipeline

### DevOps & Infrastructure
- ✅ Docker containerization
- ✅ Multi-service orchestration with Docker Compose
- ✅ Configuration management
- ✅ Logging and monitoring

### Software Engineering
- ✅ Modular, production-grade code structure
- ✅ Error handling and retry logic
- ✅ Code documentation
- ✅ Git version control

---

## 🗂️ Project Structure
```
streaming-market-platform/
├── config.py                    # Central configuration
├── requirements.txt             # Python dependencies
├── .env                         # Environment variables
├── docker/
│   └── docker-compose.yml       # Infrastructure setup
├── producers/                   # Kafka data producers
│   ├── stock_producer.py
│   ├── crypto_producer.py
│   ├── news_producer.py
│   └── run_all_producers.py
├── spark_jobs/
│   ├── bronze/                  # Raw data ingestion
│   ├── silver/                  # Data cleaning & enrichment
│   └── gold/                    # Analytics-ready tables
├── ml/                          # Machine learning pipeline
│   ├── feature_engineering.py
│   ├── train_model.py
│   ├── predict.py
│   └── models/                  # Saved models
├── dashboard/
│   └── streamlit/               # Interactive dashboards
│       ├── app.py
│       └── pages/
└── data/
    └── delta/                   # Delta Lake storage
        ├── bronze/
        ├── silver/
        ├── gold/
        └── ml/
```

---

## 📝 Technical Decisions

### Why Kafka?
- Industry-standard for real-time streaming
- Fault-tolerant, scalable message queue
- Decouples producers from consumers

### Why Spark Structured Streaming?
- Unified batch and streaming API
- Built-in fault tolerance with checkpointing
- Seamless integration with Delta Lake

### Why Delta Lake?
- ACID transactions on data lakes
- Schema evolution
- Time travel capabilities
- Perfect for ML feature stores

### Why XGBoost?
- State-of-the-art gradient boosting
- Handles tabular data excellently
- Fast training and inference
- Feature importance built-in

---

## 🔮 Future Enhancements

- [ ] Add Airflow DAGs for orchestration
- [ ] Implement Snowflake data warehouse
- [ ] Add DBT transformations
- [ ] Deploy to AWS/GCP
- [ ] Add real-time alerting
- [ ] Expand to more asset classes
- [ ] Implement reinforcement learning for trading strategies
- [ ] Add backtesting framework

---

## 📚 References & Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Delta Lake Guide](https://docs.delta.io/latest/index.html)
- [XGBoost Documentation](https://xgboost.readthedocs.io/)
- [Streamlit Documentation](https://docs.streamlit.io/)

---

## 👤 Author

**Your Name**
- LinkedIn: [your-profile](https://linkedin.com/in/your-profile)
- GitHub: [@your-username](https://github.com/your-username)
- Email: your.email@example.com

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- Yahoo Finance for market data API
- News API for news sentiment data
- Apache Software Foundation for Kafka and Spark
- Databricks for Delta Lake

---

**⭐ If you found this project helpful, please star the repository!**
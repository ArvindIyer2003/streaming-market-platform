"""
ML Predictions Dashboard Page
Shows real-time price direction predictions
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import os
import sys
import joblib

# Add root to path
ROOT_DIR = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )
    )
)
sys.path.append(ROOT_DIR)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Paths
ML_PATH = os.path.join(ROOT_DIR, "data", "delta", "ml")
MODEL_PATH = os.path.join(ROOT_DIR, "ml", "models")

# ================================
# SPARK SESSION
# ================================
@st.cache_resource
def get_spark():
    spark = SparkSession.builder \
        .appName("Dashboard-ML") \
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
# LOAD MODEL
# ================================
@st.cache_resource
def load_model():
    model_file = os.path.join(MODEL_PATH, "price_predictor.pkl")
    features_file = os.path.join(MODEL_PATH, "feature_columns.pkl")
    metrics_file = os.path.join(MODEL_PATH, "metrics.pkl")
    
    model = joblib.load(model_file)
    feature_cols = joblib.load(features_file)
    metrics = joblib.load(metrics_file)
    
    return model, feature_cols, metrics

# ================================
# LOAD PREDICTIONS
# ================================
def load_predictions(spark, model, feature_cols, limit=100):
    """Load latest features and make predictions"""
    
    df = spark.read.format("delta") \
        .load(os.path.join(ML_PATH, "features")) \
        .orderBy("timestamp_parsed", ascending=False) \
        .limit(limit)
    
    pdf = df.toPandas()
    
    if pdf.empty:
        return pd.DataFrame()
    
    # Prepare features
    X = pdf[feature_cols].fillna(0)
    
    # Predict
    predictions = model.predict(X)
    probabilities = model.predict_proba(X)
    
    # Add to dataframe
    pdf['prediction'] = predictions
    pdf['prediction_label'] = pdf['prediction'].map({0: 'DOWN ↓', 1: 'UP ↑'})
    pdf['probability_down'] = probabilities[:, 0]
    pdf['probability_up'] = probabilities[:, 1]
    pdf['confidence'] = pdf[['probability_down', 'probability_up']].max(axis=1)
    
    # Calculate if prediction was correct
    pdf['actual_label'] = pdf['target'].map({0: 'DOWN ↓', 1: 'UP ↑', None: 'PENDING'})
    pdf['correct'] = (pdf['prediction'] == pdf['target']).fillna(False)
    
    return pdf

# ================================
# DASHBOARD
# ================================
st.set_page_config(page_title="ML Predictions", page_icon="🤖", layout="wide")

st.title("🤖 ML Price Predictions")
st.markdown("---")

# Load model and data
spark = get_spark()
model, feature_cols, metrics = load_model()
predictions_df = load_predictions(spark, model, feature_cols, limit=100)

if predictions_df.empty:
    st.warning("No predictions available. Run feature engineering first!")
    st.stop()

# ================================
# METRICS ROW
# ================================
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Model Test Accuracy",
        f"{metrics['test_accuracy']:.1%}",
        delta=None
    )

with col2:
    st.metric(
        "Precision",
        f"{metrics['precision']:.1%}",
        delta=None
    )

with col3:
    st.metric(
        "Recall",
        f"{metrics['recall']:.1%}",
        delta=None
    )

with col4:
    # Live accuracy
    with_target = predictions_df.dropna(subset=['target'])
    if len(with_target) > 0:
        live_accuracy = with_target['correct'].mean()
        st.metric(
            "Live Accuracy",
            f"{live_accuracy:.1%}",
            delta=f"{(live_accuracy - metrics['test_accuracy'])*100:.1f}%"
        )
    else:
        st.metric("Live Accuracy", "N/A")

st.markdown("---")

# ================================
# LATEST PREDICTIONS TABLE
# ================================
st.subheader("📊 Latest Predictions")

# Format for display
display_cols = ['symbol', 'timestamp_parsed', 'price', 'prediction_label', 
                'confidence', 'actual_label', 'correct']

display_df = predictions_df[display_cols].head(20).copy()
display_df['timestamp_parsed'] = pd.to_datetime(display_df['timestamp_parsed']).dt.strftime('%Y-%m-%d %H:%M:%S')
display_df['price'] = display_df['price'].apply(lambda x: f"${x:.2f}")
display_df['confidence'] = display_df['confidence'].apply(lambda x: f"{x:.1%}")
display_df['correct'] = display_df['correct'].map({True: '✅', False: '❌', None: '⏳'})

display_df.columns = ['Symbol', 'Timestamp', 'Price', 'Predicted', 'Confidence', 'Actual', 'Result']

# Color code predictions
def color_prediction(row):
    if row['Predicted'] == 'UP ↑':
        return ['background-color: #d4edda; color: black'] * len(row)
    else:
        return ['background-color: #f8d7da; color: black'] * len(row)

st.dataframe(
    display_df.style.apply(color_prediction, axis=1),
    use_container_width=True,
    height=500
)

# ================================
# PREDICTION DISTRIBUTION
# ================================
col1, col2 = st.columns(2)

with col1:
    st.subheader("🎯 Prediction Distribution")
    
    pred_counts = predictions_df['prediction_label'].value_counts()
    
    fig = go.Figure(data=[go.Pie(
        labels=pred_counts.index,
        values=pred_counts.values,
        marker_colors=['#ff6b6b', '#51cf66'],
        hole=0.4
    )])
    
    fig.update_layout(height=350)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("📈 Confidence Distribution")
    
    fig = px.histogram(
        predictions_df,
        x='confidence',
        nbins=20,
        color='prediction_label',
        color_discrete_map={'UP ↑': '#51cf66', 'DOWN ↓': '#ff6b6b'},
        title='Model Confidence Levels'
    )
    
    fig.update_layout(height=350)
    st.plotly_chart(fig, use_container_width=True)

# ================================
# FEATURE IMPORTANCE
# ================================
st.markdown("---")
st.subheader("⭐ Feature Importance")

feature_importance = metrics['feature_importance'].head(10)

fig = px.bar(
    feature_importance,
    x='importance',
    y='feature',
    orientation='h',
    title='Top 10 Most Important Features'
)

fig.update_layout(height=400, yaxis={'categoryorder':'total ascending'})
st.plotly_chart(fig, use_container_width=True)

# ================================
# ACCURACY BY SYMBOL
# ================================
st.markdown("---")
st.subheader("📊 Accuracy by Symbol")

with_target = predictions_df.dropna(subset=['target'])
if len(with_target) > 0:
    symbol_accuracy = with_target.groupby('symbol').agg({
        'correct': ['sum', 'count', 'mean']
    }).reset_index()
    
    symbol_accuracy.columns = ['symbol', 'correct', 'total', 'accuracy']
    symbol_accuracy = symbol_accuracy[symbol_accuracy['total'] >= 5]  # At least 5 predictions
    symbol_accuracy = symbol_accuracy.sort_values('accuracy', ascending=False)
    
    fig = px.bar(
        symbol_accuracy,
        x='symbol',
        y='accuracy',
        text='accuracy',
        title='Prediction Accuracy by Stock (min 5 predictions)'
    )
    
    fig.update_traces(texttemplate='%{text:.1%}', textposition='outside')
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Not enough data with actual outcomes yet")

# ================================
# CONFUSION MATRIX
# ================================
st.markdown("---")
st.subheader("🎯 Confusion Matrix (Test Set)")

cm = metrics['confusion_matrix']

fig = go.Figure(data=go.Heatmap(
    z=cm,
    x=['Predicted DOWN', 'Predicted UP'],
    y=['Actual DOWN', 'Actual UP'],
    colorscale='RdYlGn',
    text=cm,
    texttemplate='%{text}',
    textfont={"size": 20}
))

fig.update_layout(
    title='Model Performance on Test Set',
    height=400
)

st.plotly_chart(fig, use_container_width=True)

# ================================
# FOOTER
# ================================
st.markdown("---")
st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')} | Model: XGBoost | Features: {len(feature_cols)}")
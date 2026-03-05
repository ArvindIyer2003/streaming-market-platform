"""
Real-time Price Prediction
Load saved model and make predictions on new data
"""
import os
import sys
import joblib
import pandas as pd
from pyspark.sql import SparkSession
from loguru import logger

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

ML_PATH = os.path.join(ROOT_DIR, "data", "delta", "ml")
MODEL_PATH = os.path.join(ROOT_DIR, "ml", "models")

def load_model():
    """Load trained model and feature columns"""
    model_file = os.path.join(MODEL_PATH, "price_predictor.pkl")
    features_file = os.path.join(MODEL_PATH, "feature_columns.pkl")
    
    model = joblib.load(model_file)
    feature_cols = joblib.load(features_file)
    
    return model, feature_cols

def predict_on_latest_features(model, feature_cols, limit=100):
    """Make predictions on latest features"""
    
    spark = SparkSession.builder \
        .appName("ML-Predict") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Load latest features
    df = spark.read.format("delta") \
        .load(os.path.join(ML_PATH, "features")) \
        .orderBy("timestamp_parsed", ascending=False) \
        .limit(limit)
    
    pdf = df.toPandas()
    
    # Prepare features
    X = pdf[feature_cols].fillna(0)
    
    # Predict
    predictions = model.predict(X)
    probabilities = model.predict_proba(X)
    
    # Add predictions to dataframe
    pdf['prediction'] = predictions
    pdf['prediction_label'] = pdf['prediction'].map({0: 'DOWN', 1: 'UP'})
    pdf['probability_down'] = probabilities[:, 0]
    pdf['probability_up'] = probabilities[:, 1]
    
    # Add confidence
    pdf['confidence'] = pdf[['probability_down', 'probability_up']].max(axis=1)
    
    spark.stop()
    
    return pdf

def main():
    logger.info("🔮 Making predictions on latest data...")
    
    # Load model
    model, feature_cols = load_model()
    logger.info("✅ Model loaded!")
    
    # Predict
    predictions = predict_on_latest_features(model, feature_cols, limit=50)
    
    # Show results
    logger.info("\n📊 Latest Predictions:")
    result = predictions[['symbol', 'timestamp_parsed', 'price', 'prediction_label', 
                          'confidence', 'target']].head(20)
    
    for _, row in result.iterrows():
        actual = 'UP' if row['target'] == 1 else 'DOWN' if row['target'] == 0 else 'N/A'
        correct = '✅' if row['prediction_label'] == actual else '❌' if actual != 'N/A' else '⏳'
        
        logger.info(
            f"{correct} {row['symbol']:12s} | "
            f"Price: ${row['price']:8.2f} | "
            f"Pred: {row['prediction_label']:4s} ({row['confidence']:.1%}) | "
            f"Actual: {actual}"
        )
    
    # Calculate accuracy on this batch
    if 'target' in predictions.columns:
        with_target = predictions.dropna(subset=['target'])
        if len(with_target) > 0:
            accuracy = (with_target['prediction'] == with_target['target']).mean()
            logger.info(f"\n🎯 Accuracy on this batch: {accuracy:.2%}")

if __name__ == "__main__":
    main()
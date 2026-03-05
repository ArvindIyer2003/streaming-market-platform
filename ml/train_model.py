"""
Train XGBoost Model for Price Direction Prediction
"""
import os
import sys
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
import xgboost as xgb
import joblib
from loguru import logger

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

ML_PATH = os.path.join(ROOT_DIR, "data", "delta", "ml")
MODEL_PATH = os.path.join(ROOT_DIR, "ml", "models")

def create_spark():
    return SparkSession.builder \
        .appName("ML-Training") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def load_features(spark):
    """Load features from Delta Lake"""
    logger.info("📥 Loading ML features...")
    
    df = spark.read.format("delta") \
        .load(os.path.join(ML_PATH, "features"))
    
    # Convert to pandas
    pdf = df.toPandas()
    
    logger.info(f"Loaded {len(pdf)} samples")
    return pdf

def prepare_data(df):
    """Prepare features and target for training"""
    logger.info("🔧 Preparing training data...")
    
    # Feature columns
    feature_cols = [
        'price_change_pct',
        'price_ma_5',
        'price_ma_10',
        'price_ma_20',
        'price_std_5',
        'price_std_10',
        'momentum_5',
        'price_change_pct_1',
        'rsi',
        'bb_position',
        'volume',
        'volume_ratio',
        'sentiment_score',
        'news_count',
        'hour',
        'day_of_week',
        'is_market_open_hour'
    ]
    
    # Remove rows with NaN
    df_clean = df[feature_cols + ['target']].dropna()
    
    logger.info(f"Clean samples: {len(df_clean)} (removed {len(df) - len(df_clean)} NaN rows)")
    
    X = df_clean[feature_cols]
    y = df_clean['target']
    
    logger.info(f"Features shape: {X.shape}")
    logger.info(f"Target shape: {y.shape}")
    
    return X, y, feature_cols

def train_model(X, y):
    """Train XGBoost model"""
    logger.info("🎯 Splitting data...")
    
    # 80-20 train-test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    logger.info(f"Train: {len(X_train)} | Test: {len(X_test)}")
    
    # Train XGBoost
    logger.info("🚀 Training XGBoost model...")
    
    model = xgb.XGBClassifier(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.1,
        random_state=42,
        eval_metric='logloss'
    )
    
    model.fit(X_train, y_train)
    
    logger.info("✅ Training complete!")
    
    return model, X_train, X_test, y_train, y_test

def evaluate_model(model, X_train, X_test, y_train, y_test, feature_cols):
    """Evaluate model performance"""
    logger.info("📊 Evaluating model...")
    
    # Predictions
    y_train_pred = model.predict(X_train)
    y_test_pred = model.predict(X_test)
    
    # Metrics
    train_acc = accuracy_score(y_train, y_train_pred)
    test_acc = accuracy_score(y_test, y_test_pred)
    
    precision = precision_score(y_test, y_test_pred)
    recall = recall_score(y_test, y_test_pred)
    f1 = f1_score(y_test, y_test_pred)
    
    logger.info("=" * 60)
    logger.info("MODEL PERFORMANCE")
    logger.info("=" * 60)
    logger.info(f"Train Accuracy: {train_acc:.4f} ({train_acc*100:.2f}%)")
    logger.info(f"Test Accuracy:  {test_acc:.4f} ({test_acc*100:.2f}%)")
    logger.info(f"Precision:      {precision:.4f}")
    logger.info(f"Recall:         {recall:.4f}")
    logger.info(f"F1 Score:       {f1:.4f}")
    logger.info("=" * 60)
    
    # Confusion matrix
    cm = confusion_matrix(y_test, y_test_pred)
    logger.info("\nConfusion Matrix:")
    logger.info(f"              Predicted")
    logger.info(f"              DOWN  UP")
    logger.info(f"Actual DOWN   {cm[0][0]:4d}  {cm[0][1]:4d}")
    logger.info(f"       UP     {cm[1][0]:4d}  {cm[1][1]:4d}")
    
    # Feature importance
    logger.info("\nTop 10 Most Important Features:")
    feature_importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    for idx, row in feature_importance.head(10).iterrows():
        logger.info(f"  {row['feature']:20s}: {row['importance']:.4f}")
    
    return {
        'train_accuracy': train_acc,
        'test_accuracy': test_acc,
        'precision': precision,
        'recall': recall,
        'f1_score': f1,
        'confusion_matrix': cm,
        'feature_importance': feature_importance
    }

def save_model(model, feature_cols, metrics):
    """Save trained model"""
    logger.info("💾 Saving model...")
    
    os.makedirs(MODEL_PATH, exist_ok=True)
    
    # Save model
    model_file = os.path.join(MODEL_PATH, "price_predictor.pkl")
    joblib.dump(model, model_file)
    
    # Save feature columns
    features_file = os.path.join(MODEL_PATH, "feature_columns.pkl")
    joblib.dump(feature_cols, features_file)
    
    # Save metrics
    metrics_file = os.path.join(MODEL_PATH, "metrics.pkl")
    joblib.dump(metrics, metrics_file)
    
    logger.info(f"✅ Model saved to: {model_file}")
    logger.info(f"✅ Features saved to: {features_file}")
    logger.info(f"✅ Metrics saved to: {metrics_file}")

def main():
    logger.info("🚀 Starting Model Training")
    logger.info("=" * 60)
    
    # Create Spark session
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load data
    df = load_features(spark)
    
    # Prepare features
    X, y, feature_cols = prepare_data(df)
    
    # Train model
    model, X_train, X_test, y_train, y_test = train_model(X, y)
    
    # Evaluate
    metrics = evaluate_model(model, X_train, X_test, y_train, y_test, feature_cols)
    
    # Save
    save_model(model, feature_cols, metrics)
    
    logger.info("=" * 60)
    logger.info("✅ Training pipeline complete!")
    logger.info("=" * 60)
    
    spark.stop()

if __name__ == "__main__":
    main()
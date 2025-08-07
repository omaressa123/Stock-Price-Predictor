# Placeholder file to keep directory structure
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from cassandra.cluster import Cluster
import pandas as pd
import numpy as np
from joblib import load

def evaluate_model(symbol, model_path, test_size=0.2):
    # Load data
    cluster = Cluster(['cassandra'])
    session = cluster.connect('market_data')
    
    query = f"SELECT timestamp, price FROM stock_prices WHERE symbol = '{symbol}' ORDER BY timestamp"
    rows = session.execute(query)
    df = pd.DataFrame(list(rows))
    
    # Prepare data
    df = df.rename(columns={'timestamp': 'ds', 'price': 'y'})
    df = df.sort_values('ds')
    
    # Split data
    split_idx = int(len(df) * (1 - test_size))
    train = df.iloc[:split_idx]
    test = df.iloc[split_idx:]
    
    # Load model
    model = load(model_path)
    
    # Make predictions
    future = model.make_future_dataframe(periods=len(test))
    forecast = model.predict(future)
    
    # Align predictions with test data
    preds = forecast.set_index('ds').join(test.set_index('ds'))['yhat']
    
    # Calculate metrics
    y_true = test.set_index('ds')['y']
    mse = mean_squared_error(y_true, preds)
    mae = mean_absolute_error(y_true, preds)
    r2 = r2_score(y_true, preds)
    
    # Save metrics to PostgreSQL
    from psycopg2 import connect
    conn = connect(
        user="airflow",
        password="airflow",
        host="postgres",
        database="airflow"
    )
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO model_metrics (symbol, model_type, mse, mae, r2)
        VALUES (%s, 'prophet', %s, %s, %s)
    """, (symbol, mse, mae, r2))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return {
        'symbol': symbol,
        'mse': mse,
        'mae': mae,
        'r2': r2,
        'test_size': len(test)
    }
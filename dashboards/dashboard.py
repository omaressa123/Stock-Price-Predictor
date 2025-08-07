import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import streamlit as st
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from prophet import Prophet
from config.config_manager import ConfigManager
import xgboost as xgb
import numpy as np
import logging
import pickle
from io import BytesIO
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dashboard")

# Set page config
st.set_page_config(
    page_title="Stock Prediction Dashboard",
    page_icon="📈",
    layout="wide"
)

@st.cache_resource
def get_cassandra_session():
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    import socket

    try:
        # Verify DNS resolution
        print(f"Attempting to resolve 'cassandra'...")
        print(f"Resolved IPs: {socket.gethostbyname_ex('cassandra')}")
        
        cluster = Cluster(
            ['cassandra'],  # Docker service name
            port=9042,
            auth_provider=PlainTextAuthProvider(
                username='cassandra',
                password='cassandra'
            ),
            protocol_version=4,
            connect_timeout=30,  # Increased timeout
            idle_heartbeat_interval=0  # Disable heartbeat for debugging
        )
        return cluster.connect("your_keyspace")
    except Exception as e:
        print(f"Connection error: {str(e)}")
        raise

def get_stock_data(symbol, hours=24):
    session = get_cassandra_session()
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=hours)
    
    query = """
    SELECT symbol, price, timestamp 
    FROM stock_prices 
    WHERE symbol = %s AND timestamp >= %s AND timestamp <= %s
    ORDER BY timestamp DESC
    """
    rows = session.execute(query, (symbol, start_time, end_time))
    return pd.DataFrame(list(rows))

def get_model_metrics():
    config = ConfigManager()
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    return pg_hook.get_pandas_df("""
        SELECT symbol, model_type, mse, mae, r2, training_date 
        FROM model_metrics 
        ORDER BY training_date DESC
    """)

def predict_with_prophet(df, periods=30):
    prophet_df = df.rename(columns={'timestamp': 'ds', 'price': 'y'})
    model = Prophet(
        daily_seasonality=True,
        yearly_seasonality=True,
        changepoint_prior_scale=0.05
    )
    model.fit(prophet_df)
    future = model.make_future_dataframe(periods=periods)
    return model.predict(future)

def predict_with_xgboost(df, periods=7):
    # Feature engineering
    df = df.sort_values('timestamp')
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['day_of_month'] = df['timestamp'].dt.day
    df['month'] = df['timestamp'].dt.month
    
    # Create lag features
    for lag in [1, 2, 3, 5, 7]:
        df[f'lag_{lag}'] = df['price'].shift(lag)
    
    # Prepare data
    X = df.dropna().drop(['price', 'timestamp'], axis=1)
    y = df.dropna()['price']
    
    # Train model
    model = xgb.XGBRegressor(
        objective='reg:squarederror',
        n_estimators=100,
        max_depth=3,
        learning_rate=0.1
    )
    model.fit(X, y)
    
    # Create future data
    last_date = df['timestamp'].max()
    future_dates = [last_date + timedelta(days=i) for i in range(1, periods+1)]
    future_df = pd.DataFrame({'timestamp': future_dates})
    
    # Add features to future data
    future_df['day_of_week'] = future_df['timestamp'].dt.dayofweek
    future_df['day_of_month'] = future_df['timestamp'].dt.day
    future_df['month'] = future_df['timestamp'].dt.month
    
    # Add lags (using last known values)
    for lag in [1, 2, 3, 5, 7]:
        future_df[f'lag_{lag}'] = df['price'].iloc[-lag]
    
    # Predict
    X_future = future_df.drop('timestamp', axis=1)
    future_df['prediction'] = model.predict(X_future)
    
    return future_df

def main():
    config = ConfigManager()
    st.title("📈 Real-Time Stock Prediction Dashboard")
    
    # Sidebar controls
    st.sidebar.header("Controls")
    symbol = st.sidebar.selectbox(
        "Select Stock", 
        config.get("stocks.symbols")
    )
    time_range = st.sidebar.slider(
        "Hours of Historical Data", 
        1, 72, 24
    )
    model_type = st.sidebar.radio(
        "Prediction Model",
        ["Prophet", "XGBoost"]
    )
    
    # Get data
    df = get_stock_data(symbol, time_range)
    
    if not df.empty:
        # Raw data
        st.subheader(f"Latest Data for {symbol}")
        st.dataframe(df.head(10))
        
        # Price chart
        st.subheader(f"Price History for {symbol}")
        fig = px.line(
            df, 
            x='timestamp', 
            y='price', 
            title=f"{symbol} Price Over Time"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Model prediction
        st.subheader(f"{model_type} Prediction")
        
        if model_type == "Prophet":
            forecast = predict_with_prophet(df[['timestamp', 'price']])
            
            fig2 = px.line(
                forecast, 
                x='ds', 
                y='yhat', 
                title='30-Day Forecast'
            )
            fig2.add_scatter(
                x=forecast['ds'], 
                y=forecast['yhat_lower'], 
                name='Lower Bound',
                line=dict(color='red', dash='dash')
            )
            fig2.add_scatter(
                x=forecast['ds'], 
                y=forecast['yhat_upper'], 
                name='Upper Bound',
                line=dict(color='red', dash='dash')
            )
            st.plotly_chart(fig2, use_container_width=True)
            
        else:  # XGBoost
            forecast = predict_with_xgboost(df[['timestamp', 'price']])
            
            fig2 = px.line(
                forecast, 
                x='timestamp', 
                y='prediction', 
                title='7-Day Forecast'
            )
            st.plotly_chart(fig2, use_container_width=True)
        
        # Latest metrics
        latest = df.iloc[0]
        st.metric(
            "Current Price", 
            f"${latest['price']:.2f}",
            delta=f"{(latest['price'] - df.iloc[1]['price']):.2f} from previous"
        )
        
        # Model metrics
        st.subheader("Model Performance")
        metrics = get_model_metrics()
        st.dataframe(
            metrics[metrics['symbol'] == symbol].sort_values(
                'training_date', 
                ascending=False
            ).head(5)
        )
    else:
        st.warning("No data available for the selected stock")

if __name__ == "__main__":
    main()
# airflow_dags/stock_prediction_pipeline_v2.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from prophet import Prophet
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import numpy as np
import logging
from io import StringIO

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def get_logger():
    log_stream = StringIO()
    handler = logging.StreamHandler(log_stream)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger, log_stream

def train_prophet(**kwargs):
    logger, log_stream = get_logger()
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        symbol = kwargs['symbol']
        
        df = pg_hook.get_pandas_df(
            sql="SELECT timestamp as ds, price as y FROM stock_prices WHERE symbol = %s ORDER BY ds",
            parameters=[symbol]
        )
        
        if len(df) < 20:
            raise ValueError(f"Insufficient data for {symbol} (only {len(df)} records)")
        
        model = Prophet(daily_seasonality=True, yearly_seasonality=True)
        model.fit(df)
        
        # Evaluate model
        future = model.make_future_dataframe(periods=int(len(df)*0.2))
        forecast = model.predict(future)
        
        # Calculate metrics
        test_size = int(len(df)*0.2)
        y_true = df['y'].values[-test_size:]
        y_pred = forecast['yhat'].values[-test_size:]
        
        mse = mean_squared_error(y_true, y_pred)
        logger.info(f"Prophet model for {symbol} trained with MSE: {mse:.4f}")
        
        # Save model
        model_path = f"/tmp/prophet_{symbol}.model"
        model.save(model_path)
        
        return model_path
    except Exception as e:
        logger.error(f"Failed to train Prophet for {symbol}: {str(e)}")
        raise

def train_xgboost(**kwargs):
    logger, log_stream = get_logger()
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        symbol = kwargs['symbol']
        
        df = pg_hook.get_pandas_df(
            sql="SELECT timestamp, price FROM stock_prices WHERE symbol = %s ORDER BY timestamp",
            parameters=[symbol]
        )
        
        if len(df) < 50:
            raise ValueError(f"Insufficient data for XGBoost ({len(df)} records)")
        
        # Feature engineering
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['day_of_month'] = df['timestamp'].dt.day
        df['month'] = df['timestamp'].dt.month
        
        # Create lag features
        for lag in [1, 2, 3, 5, 7]:
            df[f'lag_{lag}'] = df['price'].shift(lag)
        
        df = df.dropna()
        
        X = df.drop(['price', 'timestamp'], axis=1)
        y = df['price']
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
        
        model = xgb.XGBRegressor(
            objective='reg:squarederror',
            n_estimators=100,
            max_depth=3,
            learning_rate=0.1
        )
        
        model.fit(X_train, y_train)
        
        # Evaluate
        y_pred = model.predict(X_test)
        mse = mean_squared_error(y_test, y_pred)
        logger.info(f"XGBoost model for {symbol} trained with MSE: {mse:.4f}")
        
        # Save model
        model_path = f"/tmp/xgboost_{symbol}.model"
        model.save_model(model_path)
        
        # Save metrics
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO model_metrics (symbol, model_type, mse)
            VALUES (%s, 'xgboost', %s)
        """, (symbol, mse))
        conn.commit()
        cursor.close()
        conn.close()
        
        return model_path
    except Exception as e:
        logger.error(f"Failed to train XGBoost for {symbol}: {str(e)}")
        raise

def send_failure_email(context):
    error = context.get('exception')
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    
    email_op = EmailOperator(
        task_id='send_failure_email',
        to='admin@example.com',
        subject=f'DAG {dag_id} Failed - Task {task_id}',
        html_content=f"""
        <h3>Task Failed</h3>
        <p>DAG: {dag_id}</p>
        <p>Task: {task_id}</p>
        <p>Error: {str(error)}</p>
        """
    )
    email_op.execute(context)

with DAG(
    'stock_prediction_v2',
    default_args=default_args,
    description='Enhanced stock prediction pipeline',
    schedule_interval='@daily',
    catchup=False,
    on_failure_callback=send_failure_email
) as dag:
    
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
    
    for symbol in symbols:
        prophet_task = PythonOperator(
            task_id=f'train_prophet_{symbol.lower()}',
            python_callable=train_prophet,
            op_kwargs={'symbol': symbol},
            dag=dag,
        )
        
        xgboost_task = PythonOperator(
            task_id=f'train_xgboost_{symbol.lower()}',
            python_callable=train_xgboost,
            op_kwargs={'symbol': symbol},
            dag=dag,
        )
        
        alert_task = PythonOperator(
            task_id=f'check_alerts_{symbol.lower()}',
            python_callable=PriceAlert().check_alerts,
            op_kwargs={'symbol': symbol, 'threshold_pct': 0.05},
            dag=dag,
        )
        
        prophet_task >> xgboost_task >> alert_task
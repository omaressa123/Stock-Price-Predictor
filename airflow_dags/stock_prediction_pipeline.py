import sys
import os
sys.path.append(os.path.dirname(__file__))

from datetime import datetime, timedelta # For handling dates and time intervals
from airflow import DAG #Core Airflow components for creating workflows
from airflow.operators.python import PythonOperator #Airflow hooks for database connections
from airflow.operators.email import EmailOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook 
from airflow.providers.cassandra.hooks.cassandra import CassandraHook
from prophet import Prophet #Facebook's time series forecasting library
import pandas as pd #Data manipulation
import xgboost as xgb #Gradient boosting library
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score #Model evaluation metrics
from io import BytesIO 
import matplotlib.pyplot as plt #For creating and saving plots
import logging # For logging operations
from config.config_manager import ConfigManager #Custom configuration manager
#Default Arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
}
#Logger Setup
def get_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    return logger

#Task Functions
def prepare_data(symbol, **context):
    logger = get_logger()
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        df = pg_hook.get_pandas_df(
            sql="""
            SELECT timestamp, price 
            FROM stock_prices 
            WHERE symbol = %s 
            ORDER BY timestamp
            """,
            parameters=[symbol]
        )
        
        if len(df) < 50:
            raise ValueError(f"Insufficient data for {symbol} (only {len(df)} records)")
        
        # Split into train/test
        test_size = int(len(df) * 0.2)
        train = df.iloc[:-test_size]
        test = df.iloc[-test_size:]
        
        context['ti'].xcom_push(key='train_data', value=train.to_json())
        context['ti'].xcom_push(key='test_data', value=test.to_json())
        
        return True
    except Exception as e:
        logger.error(f"Data preparation failed for {symbol}: {str(e)}")
        raise
    
#Train Prophet Model
def train_prophet(**context):
    logger = get_logger()
    symbol = context['params']['symbol']
    
    try:
        train_data = pd.read_json(context['ti'].xcom_pull(key='train_data'))
        test_data = pd.read_json(context['ti'].xcom_pull(key='test_data'))
        
        # Train model
        train_data = train_data.rename(columns={'timestamp': 'ds', 'price': 'y'})
        model = Prophet(
            daily_seasonality=True,
            yearly_seasonality=True,
            changepoint_prior_scale=0.05
        )
        model.fit(train_data)
        
        # Make predictions
        future = model.make_future_dataframe(periods=len(test_data))
        forecast = model.predict(future)
        
        # Calculate metrics
        y_true = test_data['price'].values
        y_pred = forecast['yhat'].values[-len(test_data):]
        
        mse = mean_squared_error(y_true, y_pred)
        mae = mean_absolute_error(y_true, y_pred)
        r2 = r2_score(y_true, y_pred)
        
        # Save model
        model_path = f"/tmp/prophet_{symbol}.model"
        with open(model_path, 'wb') as f:
            pickle.dump(model, f)
        
        # Save metrics
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO model_metrics (symbol, model_type, mse, mae, r2)
            VALUES (%s, 'prophet', %s, %s, %s)
        """, (symbol, mse, mae, r2))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Generate and save forecast plot
        fig = model.plot(forecast)
        plt.title(f"{symbol} Prophet Forecast")
        buf = BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        
        # Save to Cassandra
        cassandra_hook = CassandraHook(cassandra_conn_id='cassandra_default')
        session = cassandra_hook.get_conn()
        
        session.execute(
            """
            INSERT INTO market_data.reports 
            (symbol, report_date, model_type, report_image) 
            VALUES (%s, %s, %s, %s)
            """,
            (symbol, datetime.now(), 'prophet', buf.read())
        )
        
        logger.info(f"Prophet model trained for {symbol} with MSE: {mse:.4f}")
        return model_path
    except Exception as e:
        logger.error(f"Prophet training failed for {symbol}: {str(e)}")
        raise

def train_xgboost(**context):
    logger = get_logger()
    symbol = context['params']['symbol']
    
    try:
        train_data = pd.read_json(context['ti'].xcom_pull(key='train_data'))
        test_data = pd.read_json(context['ti'].xcom_pull(key='test_data'))
        
        # Feature engineering
        def create_features(df):
            df = df.copy()
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['day_of_week'] = df['timestamp'].dt.dayofweek
            df['day_of_month'] = df['timestamp'].dt.day
            df['month'] = df['timestamp'].dt.month
            
            for lag in [1, 2, 3, 5, 7]:
                df[f'lag_{lag}'] = df['price'].shift(lag)
            
            return df.dropna()
        
        train_feat = create_features(train_data)
        test_feat = create_features(test_data)
        
        X_train = train_feat.drop(['price', 'timestamp'], axis=1)
        y_train = train_feat['price']
        X_test = test_feat.drop(['price', 'timestamp'], axis=1)
        y_test = test_feat['price']
        
        # Train model
        model = xgb.XGBRegressor(
            objective='reg:squarederror',
            n_estimators=100,
            max_depth=3,
            learning_rate=0.1,
            early_stopping_rounds=10
        )
        
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            verbose=False
        )
        
        # Predictions
        y_pred = model.predict(X_test)
        
        # Metrics
        mse = mean_squared_error(y_test, y_pred)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        
        # Save model
        model_path = f"/tmp/xgboost_{symbol}.model"
        model.save_model(model_path)
        
        # Save metrics
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO model_metrics (symbol, model_type, mse, mae, r2)
            VALUES (%s, 'xgboost', %s, %s, %s)
        """, (symbol, mse, mae, r2))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Generate feature importance plot
        fig, ax = plt.subplots(figsize=(10, 6))
        xgb.plot_importance(model, ax=ax)
        plt.title(f"{symbol} XGBoost Feature Importance")
        buf = BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        
        # Save to Cassandra
        cassandra_hook = CassandraHook(cassandra_conn_id='cassandra_default')
        session = cassandra_hook.get_conn()
        
        session.execute(
            """
            INSERT INTO market_data.reports 
            (symbol, report_date, model_type, report_image) 
            VALUES (%s, %s, %s, %s)
            """,
            (symbol, datetime.now(), 'xgboost', buf.read())
        )
        
        logger.info(f"XGBoost model trained for {symbol} with MSE: {mse:.4f}")
        return model_path
    except Exception as e:
        logger.error(f"XGBoost training failed for {symbol}: {str(e)}")
        raise

def compare_models(**context):
    logger = get_logger()
    symbol = context['params']['symbol']
    
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        metrics = pg_hook.get_pandas_df("""
            SELECT model_type, mse, mae, r2 
            FROM model_metrics 
            WHERE symbol = %s 
            ORDER BY training_date DESC 
            LIMIT 2
        """, parameters=[symbol])
        
        if len(metrics) < 2:
            logger.warning(f"Not enough models to compare for {symbol}")
            return
        
        best_model = metrics.iloc[0]['model_type']
        best_mse = metrics.iloc[0]['mse']
        
        # Generate comparison report
        report = f"""
        Model Comparison Report for {symbol}
        ---------------------------------
        Best Model: {best_model} (MSE: {best_mse:.4f})
        
        Detailed Metrics:
        {metrics.to_markdown()}
        """
        
        # Save report to Cassandra
        cassandra_hook = CassandraHook(cassandra_conn_id='cassandra_default')
        session = cassandra_hook.get_conn()
        
        session.execute(
            """
            INSERT INTO market_data.reports 
            (symbol, report_date, report_text) 
            VALUES (%s, %s, %s)
            """,
            (symbol, datetime.now(), report)
        )
        
        logger.info(f"Model comparison completed for {symbol}")
        return report
    except Exception as e:
        logger.error(f"Model comparison failed for {symbol}: {str(e)}")
        raise

def send_success_email(**context):
    symbol = context['params']['symbol']
    report = context['ti'].xcom_pull(task_ids=f'compare_models_{symbol.lower()}')
    
    email_op = EmailOperator(
        task_id=f'send_success_email_{symbol.lower()}',
        to=ConfigManager().get("alerts.email_to"),
        subject=f'Daily Model Training Complete - {symbol}',
        html_content=f"""
        <h3>Model Training Successful</h3>
        <p>Symbol: {symbol}</p>
        <p>Report:</p>
        <pre>{report}</pre>
        """
    )
    email_op.execute(context)

with DAG(
    'stock_prediction_pipeline',
    default_args=default_args,
    description='Automated stock prediction pipeline',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['stocks', 'prediction'],
) as dag:
    
    for symbol in ConfigManager().get("stocks.symbols"):
        prepare_task = PythonOperator(
            task_id=f'prepare_data_{symbol.lower()}',
            python_callable=prepare_data,
            op_kwargs={'symbol': symbol},
            params={'symbol': symbol},
            dag=dag,
        )
        
        prophet_task = PythonOperator(
            task_id=f'train_prophet_{symbol.lower()}',
            python_callable=train_prophet,
            params={'symbol': symbol},
            dag=dag,
        )
        
        xgboost_task = PythonOperator(
            task_id=f'train_xgboost_{symbol.lower()}',
            python_callable=train_xgboost,
            params={'symbol': symbol},
            dag=dag,
        )
        
        compare_task = PythonOperator(
            task_id=f'compare_models_{symbol.lower()}',
            python_callable=compare_models,
            params={'symbol': symbol},
            dag=dag,
        )
        
        email_task = PythonOperator(
            task_id=f'send_email_{symbol.lower()}',
            python_callable=send_success_email,
            params={'symbol': symbol},
            dag=dag,
        )
        
        prepare_task >> [prophet_task, xgboost_task] >> compare_task >> email_task
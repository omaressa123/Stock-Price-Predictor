FROM python:3.9-slim

WORKDIR /app

# Copy the entire project (so that it takes the config folder etc.)
COPY ../ /app

RUN pip install --upgrade pip && \
    pip install streamlit pandas plotly matplotlib prophet cassandra-driver xgboost psycopg2-binary apache-airflow

CMD ["streamlit", "run", "dashboards/dashboard.py"]
# Placeholder file to keep directory structure
from kafka import KafkaProducer
import requests
import json
import time
from datetime import datetime
import random

# Configuration
KAFKA_BROKER = 'localhost:29092'
TOPIC_NAME = 'stock_prices'
SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
API_KEY = 'YOUR_ALPHA_VANTAGE_API_KEY'  # Replace with actual API key

def fetch_stock_price(symbol):
    """Fetch real stock price from Alpha Vantage API"""
    try:
        url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={API_KEY}"
        response = requests.get(url)
        data = response.json()
        
        if 'Global Quote' in data:
            price = float(data['Global Quote']['05. price'])
            volume = float(data['Global Quote']['06. volume'])
            return price, volume
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
    
    # Fallback to random data if API fails
    return round(random.uniform(100, 500), random.randint(1000, 10000)

def produce_stock_prices():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    try:
        while True:
            for symbol in SYMBOLS:
                price, volume = fetch_stock_price(symbol)
                
                message = {
                    'symbol': symbol,
                    'price': price,
                    'volume': volume,
                    'timestamp': datetime.now().isoformat()
                }
                
                producer.send(TOPIC_NAME, value=message)
                print(f"Sent: {message}")
            
            time.sleep(30)  # Send every 30 seconds
    except KeyboardInterrupt:
        producer.close()

if __name__ == '__main__':
    produce_stock_prices()
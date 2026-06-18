from airflow.sdk import task, dag
from datetime import datetime, timedelta
import json
import requests
import websocket

@dag(
    dag_id = 'crypto_prices_dag',
    start_date = datetime(2026, 06, 18),
    schedule = timedelta(minutes =5),
    catchup = False
)

def crypto_market_prices():
    @task()
    def get_trading_pairs(limit=20):
        
        url = "https://api.kraken.com/0/public/AssetPairs"
        
        response =  requests.get(url)
        data = response.json()
        
        pairs = []
        
        
        for symbol, info in data['result'].items():
         #Selecting only USD quote pairs
         if info.get('quote') == 'ZUSD':
             
             pairs.append(info['altname'])
             
             if len(pairs) == limit:
                break
        return pairs
    
    @task
    def get_historical_prices(pairs):
        
        historical_data = []
        
        for pair in pairs:
            
            yesterday = datetime.today() - timedelta(days=1)
            
            since = yesterday.timestamp()
            
            url = f"https://api.kraken.com/0/public/OHLC?pair={pair}&interval=1440&since={since}"
            
            response = requests.get(url)
            
            response_data =  response.json()
            
            historical_data.append(response_data)
            
            return historical_data

    
    pairs = get_trading_pairs()
    print(pairs)
    
    historical_prices = get_historical_prices(pairs)
    print(historical_prices)

raw_data = crypto_market_prices()
print(raw_data)
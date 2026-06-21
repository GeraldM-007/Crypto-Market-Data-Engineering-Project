from airflow.sdk import task, dag
import json
import requests
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

@dag(
    dag_id = 'realtime_stocks_data_dag',
    start_date = datetime(2026, 6, 21),
    schedule = timedelta(minutes=1),
    catchup = False
)

def reatime_stocks_prices():
    @task()
    def extract_realtime_prices():
        
        now = datetime.utcnow() + timedelta(hours = 3)
        date = now.strftime('%Y-%m-%d %H:%M:%S.%f')
        
        pairs = ['BTC/USD', 'ETH/USD', 'DOGE/USD']
        
        realtime_data = []
        
        for pair in pairs:
            
            url = f'https://api.kraken.com/0/public/Ticker?pair={pair}'
            
            response = requests.get(url)
            
            data = response.json()
            
            result = list(data['result'].values())[0]
            
            realtime_data.append({
                'pair' : pair,
                'price' : float(result['c'][0]),
                'timestamp' : date
            })
        return realtime_data
    
    @task()
    def load_data(realtime_data):
        
        #database connection details from .env file
        USER = os.getenv('USER')
        HOST = os.getenv('HOST')
        PASSWORD = os.getenv('PASSWORD')
        PORT = os.getenv('PORT')
        DB_NAME = os.getenv('DATABASE')
        
        engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}')
        
        with engine.begine() as conn:
            
            for item in realtime_data:
                
                conn.execute(
                    text("""INSERT INTO realtime_crpto_prices(data) VALUES (:data)"""), {"data": json.dumps(item)}
                )
    
    realtime_data = extract_realtime_prices()
    
    load_data(realtime_data)

dag = reatime_stocks_prices()


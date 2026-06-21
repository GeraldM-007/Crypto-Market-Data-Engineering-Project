from airflow.sdk import task, dag
import json
import requests
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

@dag(
    dag_id = 'historical_crypto_prices_dag',
    start_date = datetime(2026, 6, 18),
    schedule = timedelta(minutes =1),
    catchup = False
)

def crypto_market_prices():
    @task()
    def get_trading_pairs(limit=10):
        
        url = "https://api.kraken.com/0/public/AssetPairs"
        
        #query the API
        response =  requests.get(url)
        
        #converting the response into a python dictionary
        data = response.json()
        #store the pairs in a list
        pairs = []
        
        #looping through the response to select only 10 ZUSD pairs
        
        for symbol, info in data['result'].items():
            
            #Selecting only USD quote pairs
            if info.get('quote') == 'ZUSD':
                
                #append the selected pairs into the list
                pairs.append(info['altname'])
                
                #stop the lop once the lenght(10) has been reached
                if len(pairs) == limit:
                    break
        
        return pairs
    
    @task()
    def get_historical_prices(pairs):
        
        #store ouput data in list
        historical_data = []
        
        for pair in pairs:
            
            #historical data is only data from the previous day
            #Calculating and converting the timestamp to yesterday using current timestamp
            yesterday = datetime.today() - timedelta(days=1)
            
            since = yesterday.timestamp()
            
            #kraken api url
            url = f"https://api.kraken.com/0/public/OHLC?pair={pair}&interval=1440&since={since}"
            
            #query the API
            response = requests.get(url)
            
            #convert the json reponse into a python dictionary
            response_data =  response.json()
            
            #append the data into a list
            historical_data.append(response_data)
            
        return historical_data
        
    @task()
    def load_data(pairs, historical_data):
        #database connection details from .env file
        USER = os.getenv('USER')
        HOST = os.getenv('HOST')
        PASSWORD = os.getenv('PASSWORD')
        PORT = os.getenv('PORT')
        DB_NAME = os.getenv('DATABASE')
            
        #connect to the database            
        engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}')
            
        with engine.begin() as conn:
            conn.execute(
                text("""INSERT INTO kraken_pairs(pairs) VALUES (:pairs)"""), {"pairs":json.dumps({"pairs":pairs})}
            )
                
        with engine.begin() as conn:
            
            for item in historical_data:
                
                conn.execute(
                    text("""INSERT INTO kraken_ohlc(data) VALUES (:data)"""), {"data": json.dumps(item)}
                )

    #call the trading pairs function
    pairs = get_trading_pairs()
    
    #call the ohlc function
    historical_data = get_historical_prices(pairs)
    
    #call the load function
    load_data(pairs, historical_data)

#call the outer function
dag = crypto_market_prices()




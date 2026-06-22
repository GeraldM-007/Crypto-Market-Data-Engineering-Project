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

def realtime_stocks_prices():
    #task one, get trading pairs
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
    
    #task 2, extract prices for the trading pairs
    @task()
    def extract_realtime_prices(pairs):
        
        #historical data is only data from the previous day
        #Calculating and converting the timestamp to current local time using current utcnow
        now = datetime.utcnow() + timedelta(hours = 3)
        date = now.strftime('%Y-%m-%d %H:%M:%S.%f')
        
        #store the collected realtime data
        realtime_data = []
        
        #loop through each trading pair collected
        for pair in pairs:
            
            #kraken API url
            url = f'https://api.kraken.com/0/public/Ticker?pair={pair}'
            
            #query the API
            response = requests.get(url)
            
            #convert the json response into a python dictionary
            data = response.json()
            
            realtime_data.append(data)
    
        return realtime_data
    
    @task()
    def load_data(realtime_data):
        
        #database connection details from .env file
        USER = os.getenv('USER')
        HOST = os.getenv('HOST')
        PASSWORD = os.getenv('PASSWORD')
        PORT = os.getenv('PORT')
        DB_NAME = os.getenv('DATABASE')
        
        #define database connection
        engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}')
        
        #connect to the database
        with engine.begin() as conn:
            
            #loop through each item of the json output
            for item in realtime_data:
                
                #write the data into the database table
                conn.execute(
                    text("""INSERT INTO realtime_crpto_prices(data) VALUES (:data)"""), {"data": json.dumps(item)}
                )
    
    #call the trading pairs function and store output
    pairs = get_trading_pairs()
    
    #call the realtime prices function
    realtime_data = extract_realtime_prices(pairs)
    
    #call the loading function
    load_data(realtime_data)

#call the outer function
dag = realtime_stocks_prices()


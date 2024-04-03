from datetime import datetime, timedelta, timezone
from pandas.api.types import is_numeric_dtype
from functools import lru_cache
from threading import Thread
import financedatabase as fd
from threading import Lock
import WatchList as wl
import yfinance as yf
import requests_cache
from tqdm import tqdm
import pandas as pd
import numpy as np
import Gmail as g
import time
import sys
import os
 
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter
class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass

session = CachedLimiterSession(
    limiter=Limiter(RequestRate(2, Duration.SECOND*5)),  # max 2 requests per 5 seconds
    bucket_class=MemoryQueueBucket,
    backend=SQLiteCache("yfinance.cache"),
)
 
def clear_file(file_in):
    """By Ricardo Kazuo"""
    try:
        file_to_delete = open(file_in,'w')
        file_to_delete.close()
    except:
        pass

def locking_task(historical,lock_in):
    with lock_in:
        historical["Date"] = pd.to_datetime(historical["Date"])
        #for y in historical['year'].unique():
        #    print("-------------------------------->"+str(y))
        #fullpath = os.path.join("./Stocks-US/", str(datetime.now().year)+".csv")
        #historical[historical['Date'].dt.year == datetime.now().year].to_csv(fullpath, mode='a',index=False, header=False)
        historical.to_csv("2024.csv", mode='a',index=False, header=False)

def fetch_stock(ticker_in,lock_in):
    """By Ricardo Kazuo"""
    #Fetches all the historically available values for one ticker, received as parameter using yfinance
    #File parameter is where the values will be stored
    try:
        #Let's use yfinance as yf to fetch the data
        ticker= yf.Ticker(ticker_in)
        #Let's set the initial date as 1900-01-01 and the end date now, with a daily interval
        historical = ticker.history(start="2024-04-01", end=datetime.now(), interval="1d")
        #Let's us add a column with the ticker name
        historical.insert(0, "Ticker", ticker_in, True)
        #Let's reset the index
        historical.reset_index(inplace=True)
        #Let's format the date to a propepr format
        historical["Date"]=historical["Date"].dt.tz_localize(None)
        #historical = historical.sort_values(by="Date")
        historical = historical[historical["Close"] != "Close"]
        historical['Daily Return'] = historical['Close'].pct_change()
        #Returns a DataFrame or Series of the same size containing the cumulative product
        historical['Cumulative Return'] = (1 + historical['Daily Return']).cumprod() - 1
        try:
            historical.drop(columns=['Capital Gains'],inplace=True)
        except:
            pass
        historical = historical.dropna()
        locking_task(historical,lock_in)
    #except Exception as e: print(e)
    except:
        df_ticker = pd.DataFrame([ticker_in],columns=['Missing Ticker'])
        df_ticker.to_csv("Missing_Ticker.csv", mode='a',index=False)

def threading1(country_in,lock_in):
    equities = fd.Equities()
    df_symbol=equities.select(country=country_in)
    df_symbol.reset_index(inplace=True)
    count = 0
    for index,row in tqdm(df_symbol.iterrows(),total=df_symbol.shape[0],file=sys.stdout):
        fetch_stock(row['symbol'],lock_in)
        print(f'{row["symbol"]}')
        count = count + 1
        if count > (len(df_symbol.index)/2)+1:
            break

def threading2(country_in,lock_in):
    equities = fd.Equities()
    df_symbol=equities.select(country=country_in)
    df_symbol.reset_index(inplace=True)
    df_symbol = df_symbol.reindex(index=df_symbol.index[::-1])
    count = len(df_symbol.index)
    for index,row in tqdm(df_symbol.iterrows(),total=df_symbol.shape[0],file=sys.stdout):
        fetch_stock(row['symbol'],lock_in)
        print(f'{row["symbol"]}')
        count = count - 1
        if count < (len(df_symbol.index)/2)-1:
            break

def finance_db(country_in,lock_in):
    # create two new threads
    #clear_file(path+country_in+append)
    t1 = Thread(target=threading1, args=(country_in,lock_in))
    t2 = Thread(target=threading2, args=(country_in,lock_in))
    # start the threads
    t1.start()
    t2.start()
    # wait for the threads to complete
    t1.join()
    t2.join()

def main():
    """By Ricardo Kazuo"""
    lock = Lock()
    clear_file("2024.csv")
    clear_file("Missing_Ticker.csv")
    #fetch_stock("AAPL","AAPL_.csv","./",lock,"")
    #wl.fetch_watchlists()
    #wl.fetch_data_watchlist()
    
    
    g.send_message(subject="Started Stocks-US",body="Started of Stocks-US")
    finance_db("United States",lock)
    g.send_message(subject="Finished Stocks-US",body="End of Stocks-US")
    
    g.send_message(subject="Started Stocks-France",body="Started of Stocks-France")
    finance_db("France",lock)
    g.send_message(subject="Finished Stocks-France",body="End of Stocks-France")
    
    g.send_message(subject="Started Stocks-Germany",body="Started of Stocks-Germany")
    finance_db("Germany",lock)
    g.send_message(subject="Finished Stocks-Germany",body="End of Stocks-Germany")
    
    g.send_message(subject="Started Stocks-Australia",body="Started of Stocks-Australia")
    finance_db("Australia",lock)
    g.send_message(subject="Finished Stocks-Australia",body="End of Stocks-Australia")
    
    g.send_message(subject="Started Stocks-Canada",body="Started of Stocks-Canada")
    finance_db("Canada",lock)
    g.send_message(subject="Finished Stocks-Canada",body="End of Stocks-Canada")
    
    g.send_message(subject="Started Stocks-Brazil",body="Started of Stocks-Brazil")
    finance_db("Brazil",lock)
    g.send_message(subject="Finished Stocks-Brazil",body="End of Stocks-Brazil")
    """
    """
    
if __name__ == '__main__':
    main()

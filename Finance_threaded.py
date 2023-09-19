import financedatabase as fd
import pandas as pd
import yfinance as yf
from ta.volatility import BollingerBands
import matplotlib.pyplot as plt
from functools import lru_cache
from tqdm import tqdm
import datetime
from datetime import datetime, timedelta, timezone
import numpy as np
from threading import Thread
from time import sleep, perf_counter

@lru_cache(maxsize=128, typed=False)
def fetch_on_stock(ticker_in):
    """By Ricardo Kazuo"""
#Fetches all the historically available values for one ticker, received as parameter using yfinance
#File parameter is where the values will be stored
    try:
        #Let's use yfinance as yf to fetch the data
        ticker= yf.Ticker(ticker_in)
        historical = ticker.history(start="1900-01-01", end=datetime.now(), interval="1d")
        print(historical.head())
    except:
        pass

def threading1(country_in,arg_not_used):
    equities = fd.Equities()
    df=equities.select(country=country_in)
    df.reset_index(inplace=True)
    count = 0
    for index,row in tqdm(df.iterrows(),total=df.shape[0]):
        fetch_on_stock(row['symbol'])
        print(f'{row["symbol"]}')
        if count > (len(df.index)/2)+1:
            break
        count = count + 1

def threading2(country_in,arg_not_used):
    equities = fd.Equities()
    df=equities.select(country=country_in)
    df.reset_index(inplace=True)
    df = df.reindex(index=df.index[::-1])
    count = len(df.index)
    for index,row in tqdm(df.iterrows(),total=df.shape[0]):
        fetch_on_stock(row['symbol'])
        print(f'{row["symbol"]}')
        if count < (len(df.index)/2)-1:
            break
        count = count - 1

def main():
    start_time = perf_counter()
    country = "Brazil"
    # create two new threads
    t1 = Thread(target=threading1, args=(country,True))
    t2 = Thread(target=threading2, args=(country,True))
    # start the threads
    t1.start()
    t2.start()
    # wait for the threads to complete
    t1.join()
    t2.join()
    end_time = perf_counter()
    print(f'It took {end_time- start_time: 0.2f} second(s) to complete.')


if __name__ == '__main__':
    main()
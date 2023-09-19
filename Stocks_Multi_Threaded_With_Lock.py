import os
import sys
from datetime import datetime, timedelta, timezone
import numpy as np
import pandas as pd
import yfinance as yf
from pandas.api.types import is_numeric_dtype
from tqdm import tqdm
from functools import lru_cache
from threading import Thread
from threading import Lock
import financedatabase as fd
from time import sleep, perf_counter

def locking_task(dataframe,file,lock_in):
        with lock_in:
            dataframe.to_csv(file, mode='a',index=False, header=False)

def fetch_on_stock(ticker_in,file_in,historical_in,path,lock_in):
    """By Ricardo Kazuo"""
#Fetches all the historically available values for one ticker, received as parameter using yfinance
#File parameter is where the values will be stored
    try:
        fullpath = os.path.join(path, file_in)
        #Let's use yfinance as yf to fetch the data
        ticker= yf.Ticker(ticker_in)
        #Let's set the initial date as 1900-01-01 and the end date now, with a daily interval
        #
        #historical = ticker.history(start="2019-11-11", end=datetime.now(), interval="1d")
        if historical_in == "Full":
            historical = ticker.history(start="1900-01-01", end=datetime.now(), interval="1d")
            writemode = 'a'
        else:
            historical = ticker.history(
                                        start=datetime.now() - timedelta(days=30),
                                        end=datetime.now(),
                                        interval="1d")
            writemode = 'a'
        #Let's drop unwanted columns
        historical.drop(columns=['High', 'Low', 'Volume', 'Dividends', 'Stock Splits'],inplace=True)
        #Let's us add a column with the ticker name
        historical.insert(0, "Ticker", ticker_in, True)
        #Let's reset the index
        historical.reset_index(inplace=True)
        #Let's format the date to a propepr format
        historical['Date']=historical['Date'].dt.tz_localize(None)
        historical = historical.sort_values(by="Date")
        historical = historical[historical["Close"] != "Close"]
        try:
            historical.drop(columns=['Capital Gains'],inplace=True)
        except:
            pass
        historical = historical.dropna()
        #Let's save to a csv file
        locking_task(historical,fullpath,lock_in)
    except:
        df_ticker = pd.DataFrame([ticker_in],columns=['Missing Ticker'])
        df_ticker.to_csv("Missing_Ticker.csv", mode='a',index=False)

def clear_file(file_in):
    """By Ricardo Kazuo"""
    try:
        file_to_delete = open(file_in,'w')
        file_to_delete.close()
    except:
        pass

def threading1(country_in,append,historical,path,lock_in):
    equities = fd.Equities()
    df_symbol=equities.select(country=country_in)
    df_symbol.reset_index(inplace=True)
    count = 0
    for index,row in tqdm(df_symbol.iterrows(),total=df_symbol.shape[0],file=sys.stdout):
        fetch_on_stock(row['symbol'],country_in+append,historical,path,lock_in)
        print(f'{row["symbol"]}')
        count = count + 1
        if count > (len(df_symbol.index)/2)+1:
            break

def threading2(country_in,append,historical,path,lock_in):
    equities = fd.Equities()
    df_symbol=equities.select(country=country_in)
    df_symbol.reset_index(inplace=True)
    df_symbol = df_symbol.reindex(index=df_symbol.index[::-1])
    count = len(df_symbol.index)
    for index,row in tqdm(df_symbol.iterrows(),total=df_symbol.shape[0],file=sys.stdout):
        fetch_on_stock(row['symbol'],country_in+append,historical,path,lock_in)
        print(f'{row["symbol"]}')
        count = count - 1
        if count < (len(df_symbol.index)/2)-1:
            break

def finance_db(country_in,append,historical,path,lock_in):
    # create two new threads
    clear_file(path+country_in+append)
    t1 = Thread(target=threading1, args=(country_in,append,historical,path,lock_in))
    t2 = Thread(target=threading2, args=(country_in,append,historical,path,lock_in))
    # start the threads
    t1.start()
    t2.start()
    # wait for the threads to complete
    t1.join()
    t2.join()

def main():
    lock = Lock()
    start_time = perf_counter()
    finance_db("Brazil","FinancialDataStocksIncremental.csv","Full","./FinFiles/",lock)
    #finance_db("Japan","FinancialDataStocksIncremental.csv","Full","./FinFiles/",lock)
    end_time = perf_counter()
    print(f'It took {end_time- start_time: 0.2f} second(s) to complete.')

if __name__ == '__main__':
    main()

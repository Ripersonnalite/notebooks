import os
import csv
import sys
import time
import inspect
import datetime
import WatchList as wl
from multiprocessing import Process
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import requests
import polars as pl
import numpy as np
import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup as bs
import matplotlib.pyplot as plt
from pandas.api.types import is_numeric_dtype
import plotly.express as px
import financedatabase as fd
import plotly.graph_objects as go
from geopy.geocoders import Nominatim
import helper
from tqdm import tqdm
from functools import lru_cache
from threading import Thread

def tictoc(func):
    """By Ricardo Kazuo"""
    def wrapper():
        time1 = time.time()
        func()
        time2 = time.time() - time1
        print(f'{func.__name__} ran in {time2} seconds')
    return wrapper

def fetch_on_stock(ticker_in,file_in,historical_in,path):
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
        historical["Log_Python"] = np.log2(historical["Close"])
        historical["Log_Diff"] = historical["Log_Python"].diff(1)
        try:
            historical.drop(columns=['Capital Gains'],inplace=True)
        except:
            pass
        historical = historical.dropna()
        #Let's save to a csv file
        historical.to_csv(fullpath, mode=writemode,index=False, header=False)
    except:
        df_ticker = pd.DataFrame([ticker_in],columns=['Missing Ticker'])
        df_ticker.to_csv("Missing_Ticker.csv", mode='a',index=False)

def remove_dup_polars_no_header(file_in,path):
    """By Ricardo Kazuo"""
#Combines ticker and date and then remove duplicates
    fullpath = os.path.join(path, file_in)
    df_local = pl.read_csv(fullpath,has_header = False)
    df_local = df_local.with_columns((pl.col("column_1") + pl.col("column_2") ).alias("Duplicate"))
    df_local = df_local.unique(subset=["Duplicate"], keep='first')
    df_local = df_local.drop('Duplicate')

def remove_header_polars(file_in):
    """By Ricardo Kazuo"""
#Combines ticker and date and then remove duplicates
    df_local = pl.read_csv(file_in,has_header = True)
    print(df_local)
    df_local.write_csv("NoHeader"+file_in,has_header=False)

def test_fun(file_in):
    """By Ricardo Kazuo"""
    try:
        #Let's read the first csv file
        df_local = pd.read_csv((file_in),on_bad_lines='skip')
        print(df_local.info())
        df_local = df_local.drop(columns=['Normalized', 'Log_Price'])
        print(df_local.info())
    except:
        pass

def select_contain_polars(file_in):
    """By Ricardo Kazuo"""
#Combines ticker and date and then remove duplicates
    df_local = pl.read_csv(file_in,has_header = True)
    df_local = df_local.filter(pl.col("Date").str.contains('2022-12'))
    df_local.write_csv(file_in)
    #filter_df = df_local.filter(pl.col("Log_Diff") < 0.00)
    #print(filter_df)
    #df_local.write_csv("NoHeader"+file_in,has_header=False)

@tictoc
def fetch_one():
    """By Ricardo Kazuo"""
#Fetches all the historically available values for one ticker, received as parameter using yfinance
#File parameter is where the values will be stored
    try:
        #Let's use yfinance as yf to fetch the data
        ticker= yf.Ticker("AAPL")
        #Let's set the initial date as 1900-01-01 and the end date now, with a daily interval
        historical = ticker.history(start=datetime.now()- timedelta(days=7),
                                    end=datetime.now(),
                                    interval="1d")
        #Let's drop unwanted columns
        historical.drop( \
            columns=['Open', 'High', 'Low', 'Volume', 'Dividends', 'Stock Splits'],inplace=True)
        #Let's us add a column with the ticker name
        historical.insert(0, "Ticker", "AAPL", True)
        #Let's reset the index
        historical.reset_index(inplace=True)
        #Let's format the date to a propepr format
        historical['Date']=historical['Date'].dt.tz_localize(None)
        historical = historical.sort_values(by="Date")
        historical = historical[historical["Close"] != "Close"]
        historical["Log_Python"] = np.log2(historical["Close"])
        historical["Log_Diff"] = historical["Log_Python"].diff(1)
        try:
            historical.drop(columns=['Capital Gains'],inplace=True)
        except:
            pass
        historical = historical.dropna()
        print(historical)
        #print(historical.filter(pl.all(pl.col(pl.Float32, pl.Float64).is_not_nan())).drop_nulls())
        #Let's save to a csv file
    except:
        pass

def timenow():
    """By Ricardo Kazuo"""
    try:
        print(f"{datetime.now()- timedelta(days=7):%Y-%m-%d}")
    except:
        pass

def split_into_dates(file_in):
    """By Ricardo Kazuo"""
    colnames=['Date', 'Ticker', 'Val1', 'Val2', 'Val3', 'Val4']
    df_local = pd.read_csv(file_in, names=colnames,on_bad_lines='skip')
    df_local.sort_values(by='Date', inplace = True)
    df_local[(df_local['Date'] > '2023-03-31')&(df_local['Date'] < '2023-04-04')] \
        .to_csv("Start_1" + file_in, header=False,index=False)
    df_local[(df_local['Date'] > '2023-04-03')&(df_local['Date'] < '2023-04-05')] \
        .to_csv("Start_2" + file_in, header=False,index=False)
    df_local[(df_local['Date'] > '2023-04-04')&(df_local['Date'] < '2023-04-06')] \
        .to_csv("Start_3" + file_in, header=False,index=False)
    df_local[(df_local['Date'] > '2023-04-05')&(df_local['Date'] < '2023-04-07')] \
        .to_csv("Start_4" + file_in, header=False,index=False)
    df_local[(df_local['Date'] > '2023-04-06')&(df_local['Date'] < '2023-04-08')] \
        .to_csv("Start_5" + file_in, header=False,index=False)
    df_local[(df_local['Date'] > '2023-04-07')].to_csv("Start_6" + file_in, header=False,index=False)

def clear_file(file_in):
    """By Ricardo Kazuo"""
    try:
        file_to_delete = open(file_in,'w')
        file_to_delete.close()
    except:
        pass

def fetch_by_date(file_in):
    """
    https://sparkbyexamples.com/pandas/pandas-select-dataframe-rows-between-two-dates/?expand_article=1
    """
    df = pd.read_csv(file_in,on_bad_lines='skip')
    df.columns =['Date', 'C1', 'C2', 'C3', 'C4', 'C5']
    start_date1 = '1900-01-01'
    end_date1 = '2012-12-31'
    start_date2 = '2013-01-01'
    end_date2 = '2032-12-31'
    #mask = (df.columns[0] <= "01-01-1980")
    #print(df.loc[mask])
    #print(df[df.columns[0]])
    #df2 = df.loc[df["Date"].between("1900-01-01", "1980-01-01")]
    df1 = df.query('Date > @start_date1 and Date < @end_date1')
    df1.to_csv(file_in+"_P1", mode="a",index=False, header=False)
    df2 = df.query('Date > @start_date2 and Date < @end_date2')
    df2.to_csv(file_in+"_P2", mode="a",index=False, header=False)

def finance_db_incremental():
    """By Ricardo Kazuo"""
    df_symbol = pd.read_csv("FinancialData.csv",on_bad_lines='skip')
    df_symbol = df_symbol[df_symbol['country'] == "United States"]
    df_symbol = df_symbol[df_symbol['sector'].isin(["Financials","Information Technology"])]
    count = len(df_symbol.index)
    for i,row in df_symbol.iterrows():
        fetch_on_stock(row['symbol'],"Incremental United States.csv","Full","./")
        #print(row['symbol'] + " - " +str(count))
        #print(f'{i}')
        count = count - 1

def task():
    print("Doing task" , helper.get_time())        

def clear_missing_ticker():
    a = pd.read_csv("FinancialData.csv",on_bad_lines='skip')
    a = a[a['country'] == "Brazil"]
    b = pd.read_csv("Missing_Ticker.csv",on_bad_lines='skip')
    df = (pd.merge(a,b, indicator=True, how='outer',left_on='symbol', right_on='Missing Ticker')
         .query('_merge=="left_only"')
         .drop('_merge', axis=1))
    df.drop(columns=['Missing Ticker'])

def threading1(country_in,append,historical,path):
    print(f"{country_in}-{append}-{path}")
    df_symbol = pd.read_csv("FinancialData.csv",on_bad_lines='skip')
    df_symbol = df_symbol[df_symbol['country'] == country_in]
    count = 0
    for index,row in tqdm(df_symbol.iterrows(),total=df_symbol.shape[0],file=sys.stdout):
        fetch_on_stock(row['symbol'],country_in+append,historical,path)
        #print(row['symbol'] + " - " +str(count))
        print(f'{row["symbol"]}')
        count = count + 1
        if count > (len(df_symbol.index)/2)+1:
            break
    df_symbol = df_symbol.reindex(index=df_symbol.index[::-1])

def threading2(country_in,append,historical,path):
    print(f"{country_in}-{append}-{path}")
    df_symbol = pd.read_csv("FinancialData.csv",on_bad_lines='skip')
    df_symbol = df_symbol[df_symbol['country'] == country_in]
    count = len(df_symbol.index)
    for index,row in tqdm(df_symbol.iterrows(),total=df_symbol.shape[0],file=sys.stdout):
        fetch_on_stock(row['symbol'],country_in+append,historical,path)
        #print(row['symbol'] + " - " +str(count))
        print(f'{row["symbol"]}')
        count = count - 1
        if count < (len(df_symbol.index)/2)-1:
            break

def finance_db(country_in,append,historical,path):
    # create two new threads
    clear_file(path+country_in+str(1)+append)
    clear_file(path+country_in+str(2)+append)
    t1 = Thread(target=threading1, args=(country_in,str(1)+append,historical,path))
    t2 = Thread(target=threading2, args=(country_in,str(2)+append,historical,path))
    # start the threads
    t1.start()
    t2.start()
    # wait for the threads to complete
    t1.join()
    t2.join()

def main():
    wl.fetch_watchlists()
    wl.fetch_data_watchlist()
    finance_db("United States","FinancialDataStocks.csv","Full","./")
    finance_db("Brazil","FinancialDataStocksIncremental.csv","Full","./FinFiles/")
    finance_db("France","FinancialDataStocksIncremental.csv","Full","./FinFiles/")
    finance_db("Germany","FinancialDataStocksIncremental.csv","Full","./FinFiles/")
    finance_db("Japan","FinancialDataStocksIncremental.csv","Full","./FinFiles/")



if __name__ == '__main__':
    main()

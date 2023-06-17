"""By Ricardo Kazuo"""
import os
import csv
import time
import inspect
import datetime
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
from pandas_datareader import data
from pandas.api.types import is_numeric_dtype
import plotly.express as px
import financedatabase as fd
import plotly.graph_objects as go
from geopy.geocoders import Nominatim

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
    print(file_in)
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
                                        start=datetime.now() - timedelta(days=7),
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
        print(historical)
        #Let's save to a csv file
        historical.to_csv(fullpath, mode=writemode,index=False, header=False)
    except:
        pass

def fetch_by_country(country_in):
    """By Ricardo Kazuo"""
#Fetches the data by country, using the parameter to filter in the dataframe
    #Let's read the first csv file
    symbol_info = pd.read_csv(("Symbol_Info.csv"),on_bad_lines='skip')
    #Let's read the secpmd csv file
    df_symbol = pd.read_csv(("Symbol.csv"),on_bad_lines='skip')
    #Let's merge both files
    df_result = symbol_info.merge(df_symbol,on="Symbol", how="outer")
    #Let's add "Other" to blank countries
    df_result = df_result.fillna("Other")
    #If the parameter Country is "Other", then all the tickers different
    #from "United States", "Japan", "Brazil" wil be fetched
    if country_in == "Others":
        rows = df_result[  (df_result['Country'] != "United  States") &
                        (df_result['Country'] != "Brazil") &
                        (df_result['Country'] != "Japan") ]
    if country_in == "United  States":
    #Else it will fetch the tickers of the specified parameter Country
        rows = df_result[df_result['Country'] == country_in]
    if country_in == "Japan":
    #Else it will fetch the tickers of the specified parameter Country
        rows = df_result[(df_result['Country'] == "Japan")]
    if country_in=="Brazil":
    #Else it will fetch the tickers of the specified parameter Country
        rows=df_result[(df_result['Country'] == "Brazil")]
    #Let's iterate on all returned tickers calling Fetch()
    for row in rows.iterrows():
        fetch_on_stock(row['Symbol'],country_in+".csv")
    remove_dup_polars(country_in+".csv")

def fetch_data_watchlist():
    """By Ricardo Kazuo"""
#Inputs:WatchlistTicker.csv
#Inputs:Symbol.csv
#Outputs: Symbol.csv
    try:
        #Let's read the first csv file
        df_watchlist = pd.read_csv(('WatchlistTicker.csv'),on_bad_lines='skip')
        #Let's select the column "Symbol"
        df_watchlist = df_watchlist['Symbol']
        #Let's read the second csv file
        df_symbol = pd.read_csv(('Symbol.csv'),on_bad_lines='skip')
        #Let's select the column "Symbol"
        df_symbol = df_symbol['Symbol']
        frames = [df_watchlist, df_symbol]
        #Let's concatenate both dataframes
        df_result = pd.concat(frames, sort=True)
        #Let's drop the duplicate entries
        df_result = df_result.drop_duplicates()
        #Let's sort the entries
        df_result = df_result.sort_values()
        #Let's save to a file
        df_result.to_csv('Symbol.csv', mode='w',index=False, header=True)
    except:
        pass

def str_trim(x_in):
    """By Ricardo Kazuo"""
    try:
        if len(x_in) == 2:
            return x_in[:1]
        elif len(x_in) == 4:
            return x_in[:2]
        elif len(x_in) == 6:
            return x_in[:3]
        elif len(x_in) == 8:
            return x_in[:4]
        elif len(x_in) == 10:
            return x_in[:5]
        elif len(x_in) == 12:
            return x_in[:6]
        elif len(x_in) == 14:
            return x_in[:7]
        elif len(x_in) == 16:
            return x_in[:8]
        elif len(x_in) == 18:
            return x_in[:9]
        else:
            return x_in
    except:
        return x_in

def fetch_watchlists():
    """By Ricardo Kazuo"""
#Fetches all the ticker by category
    headers={'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36'}
    data = pd.DataFrame()
    address = {
    "https://finance.yahoo.com/u/yahoo-finance/watchlists/video-game-stocks",
    "https://finance.yahoo.com/u/yahoo-finance/watchlists/420_stocks",
    "https://finance.yahoo.com/u/yahoo-finance/watchlists/tech-stocks-that-move-the-market",
    "https://finance.yahoo.com/u/yahoo-finance/watchlists/new-family-economy",
    "https://finance.yahoo.com/u/yahoo-finance/watchlists/sinful-stocks",
    "https://finance.yahoo.com/u/yahoo-finance/watchlists/romance-industry",
    "https://finance.yahoo.com/u/yahoo-finance/watchlists/bank-and-financial-services-stocks",
    "https://finance.yahoo.com/u/yahoo-finance/watchlists/cash-rich-companies-stocks",
    "https://finance.yahoo.com/u/yahoo-finance/watchlists/biotech-and-drug-stocks",
    "https://finance.yahoo.com/u/yahoo-finance/watchlists/semiconductor-stocks",
    "https://finance.yahoo.com/u/yahoo-finance/watchlists/oil-and-gas-stocks",
    "https://finance.yahoo.com/u/yahoo-finance/watchlists/china-tech-and-internet-stocks",
    "https://finance.yahoo.com/u/yahoo-finance/watchlists/most-watched",
    "https://finance.yahoo.com/u/yahoo-finance/watchlists/women-at-the-helm",
    "https://finance.yahoo.com/u/yahoo-finance/watchlists/healthy-living",
    "https://finance.yahoo.com/u/yahoo-finance/watchlists/electronic-trading-stocks",
    "https://finance.yahoo.com/u/trea/watchlists/the-fight-against-covid19",
    "https://finance.yahoo.com/u/motley-fool/watchlists/dividend-growth-market-leaders",
    "https://finance.yahoo.com/u/motley-fool/watchlists/8-buffett-buys",
    "https://finance.yahoo.com/u/motley-fool/watchlists/brands-consumers-love",
    "https://finance.yahoo.com/u/motley-fool/watchlists/stocks-fueling-internet-of-things",
    "https://finance.yahoo.com/world-indices",
    "https://finance.yahoo.com/commodities",
	"https://finance.yahoo.com/currencies"
    }
    address1 = {
    "https://finance.yahoo.com/screener/predefined/insurance_life",
    "https://finance.yahoo.com/screener/predefined/aerospace_defense",
    "https://finance.yahoo.com/screener/predefined/auto_manufacturers",
    "https://finance.yahoo.com/screener/predefined/growth_technology_stocks",
    "https://finance.yahoo.com/screener/predefined/most_shorted_stocks",
    "https://finance.yahoo.com/losers",
    "https://finance.yahoo.com/gainers",
    "https://finance.yahoo.com/mutualfunds"
    }
    for addres in address:
        try:
            file = addres.partition('s/')[2] or \
                    addres.partition('d/')[2] or \
                    addres.partition('m/')[2]
            print(file)
            temp = requests.get(addres,headers=headers).text
            soup = bs(temp, 'html.parser')
            table=soup.findAll("table", {"class": "cwl-symbols"})
            if len(table)==0 :
                table=soup.findAll("table", {"class": "W(100%)"})
            try:
                temp =pd.read_html(str(table))[0]
                temp.insert(1, "Type", file, True)
                temp = temp.iloc[:, [0,1]]
                data = pd.concat([data,temp], ignore_index=True, sort=False)
            except:
                pass
        except:
            pass
    for addres in address1:
        try:
            file = addres.partition('s/')[2] or \
                    addres.partition('d/')[2] or \
                    addres.partition('m/')[2]
            print(file)
            temp = requests.get(addres,headers=headers).text
            soup = bs(temp, 'html.parser')
            table=soup.findAll("table", {"class": "cwl-symbols"})
            if len(table)==0 :
                table=soup.findAll("table", {"class": "W(100%)"})
            try:
                temp=pd.read_html(str(table))[0]
                temp.insert(1, "Type", file, True)
                temp.iloc[:,0]=temp.iloc[:,0].str.replace(r'Select ', '')
                temp.iloc[:,0]=temp.iloc[:,0].apply(lambda x: str_trim(x))
                temp.rename(columns={ temp.columns[0]: "Symbol" }, inplace = True)
                temp=temp.iloc[:,[0,1]]
                data=pd.concat([data,temp], ignore_index=True, sort=False)
            except:
                pass
        except:
            pass
    data.iloc[:, :3].to_csv("WatchlistTicker.csv",mode='w',index=False,header=True)
    data = pd.DataFrame()

def min_max_polars(file_in):
    """By Ricardo Kazuo"""
#Groups the min and max values and its dates
    #Let's read the data
    df_local = pl.read_csv(file_in,has_header = True, infer_schema_length=0)
    #Making sure we don't have unwanted rows
    df_local = df_local.filter(pl.col("Date") != "Date")
    #Start the group by
    df_group_by = df_local.lazy().groupby("Ticker").agg(
        [
        #Here we have the max value
        pl.col("Close").max().alias("Max"),
        #Here we have the dates of the occurring max value, it brings a list of strings
        pl.col("Date").filter(pl.col("Close")==pl.col("Close").max()).alias("Max_Date"),
        #Here we have the min value
        pl.col("Close").min().alias("Min"),
        #Here we have the dates of the occurring min value, it brings a list of strings
        pl.col("Date").filter(pl.col("Close")==pl.col("Close").min()).alias("Min_Date"),
        ]).collect()
    #Here we make sure it is stored as float
    df_group_by = df_group_by.with_column(pl.col("Min").cast(pl.Float64, strict=False).alias("Min"))
    #Here we make sure it is stored as float
    df_group_by = df_group_by.with_column(pl.col("Max").cast(pl.Float64, strict=False).alias("Max"))
    #Here we explode the list of min dates to remove the duplicates laetr
    df_group_by = df_group_by.explode('Min_Date')
    #Here we explode the list of max dates to remove the duplicates laetr
    df_group_by = df_group_by.explode('Max_Date')
    #Here we remove the duplicates
    df_group_by = df_group_by.unique(subset=["Ticker"], keep='first')
    df_group_by.write_csv("MinMax" + file_in)

def remove_dup_polars_no_header(file_in,path):
    """By Ricardo Kazuo"""
#Combines ticker and date and then remove duplicates
    fullpath = os.path.join(path, file_in)
    df_local = pl.read_csv(fullpath,has_header = False)
    df_local = df_local.with_columns((pl.col("column_1") + pl.col("column_2") ).alias("Duplicate"))
    df_local = df_local.unique(subset=["Duplicate"], keep='first')
    df_local = df_local.drop('Duplicate')

def remove_dup_polars(file_in):
    """By Ricardo Kazuo"""
#Combines ticker and date and then remove duplicates
    df_local = pl.read_csv(file_in,has_header = True)
    df_local = df_local.with_column((pl.col("Date") + pl.col("Ticker") ).alias("Duplicate"))
    df_local = df_local.unique(subset=["Duplicate"], keep='first')
    df_local = df_local.drop('Duplicate')
    df_local.write_csv(file_in)

def remove_header_polars(file_in):
    """By Ricardo Kazuo"""
#Combines ticker and date and then remove duplicates
    df_local = pl.read_csv(file_in,has_header = True)
    print(df_local)
    df_local.write_csv("NoHeader"+file_in,has_header=False)

def average_polars(file_in):
    """By Ricardo Kazuo"""
#Normalize all the values to fit into a visualization bringing it to a better order of magnitude
    df_local = pl.read_csv(file_in,has_header = True, infer_schema_length=0)
    df_local = df_local.filter(pl.col("Date") != "Date")
    df_local = df_local.with_column(pl.col("Close").cast(pl.Float64, strict=False).alias("Close"))
    df_local = df_local.with_column(pl.col("Close").cast(pl.Float64, strict=False).alias("MeanTicker"))
    df_local = df_local.with_column(pl.col("Close").cast(pl.Float64, strict=False).alias("MeanAll"))
    df_local = df_local.with_column(pl.col("Close").cast(pl.Float64, strict=False).alias("Normalized"))
    try:
        df_local = df_local.drop("Normalized")
    except:
        pass
    try:
        df_local = df_local.drop("MeanTicker")
    except:
        pass
    try:
        df_local = df_local.drop("MeanAll")
    except:
        pass
    print(df_local)
    df_group_by = df_local.lazy().groupby("Ticker").agg([pl.col("Close").mean().alias("MeanTicker")]).collect()
    df_inner_join = df_local.join(df_group_by, on="Ticker", how="inner")
    df_inner_join = df_inner_join.with_column(pl.col("Close").mean().alias("MeanAll"))
    df_inner_join = df_inner_join.with_column((pl.col("MeanAll")-pl.col("MeanTicker")+pl.col("Close")).alias("Normalized"))
    try:
        df_inner_join = df_inner_join.drop("MeanTicker_right")
    except:
        pass
    try:
        df_inner_join = df_inner_join.drop("MeanTicker")
    except:
        pass
    try:
        df_inner_join = df_inner_join.drop("MeanAll")
    except:
        pass
    df_inner_join.write_csv(file_in)

def rate_return(file_in):
    """By Ricardo Kazuo"""
    df_local = pd.read_csv(file_in,on_bad_lines='skip')
    df_local["Date"]= pd.to_datetime(df_local["Date"])
    df_local = df_local[df_local["Date"] != "Date"]
    ticker = df_local
    #ticker = df_local[(df_local["Ticker"]=="AAPL")]
    ticker = ticker["Ticker"]
    ticker = ticker.drop_duplicates()
    ticker = ticker.reset_index()
    #ticker = ticker.drop(columns=['Normalized', 'Log_Price'])
    try:
        ticker = ticker.drop(columns=['Normalized', 'Log_Python'])
    except:
        pass
    main = pd.DataFrame()
    for row in ticker.iterrows():
        print(row["Ticker"])
        inner_rows = df_local[df_local["Ticker"] == row["Ticker"]]
        inner_rows = inner_rows.sort_values(by="Date")
        #inner_rows = inner_rows.tail(60)
        inner_rows = inner_rows[inner_rows["Close"] != "Close"]
        inner_rows = inner_rows.drop(columns=["Ticker"])
        inner_rows["Log_Python"] = np.log2(inner_rows["Close"])
        inner_rows["Log_Diff"] = inner_rows["Log_Python"].diff(1)
        inner_rows["Ticker"] = row["Ticker"]
        try:
            inner_rows = inner_rows.drop(columns=['Normalized'])
        except:
            pass
        inner_rows.drop(index=inner_rows.index[0], axis=0, inplace=True)
        print(inner_rows.info())
        main = pd.concat([main, inner_rows])
    main.to_csv("Log_" + file_in , mode='w',index=False, header=True)
    remove_dup_polars("Log_" + file_in)
    #print(main)

def fetch_all():
    """By Ricardo Kazuo"""
    process1 = Process(target=fetch_by_country, args=('Japan',))
    process1.start()
    process2 = Process(target=fetch_by_country, args=('United  States',))
    process2.start()
    process3 = Process(target=fetch_by_country, args=('Others',))
    process3.start()
    process1.join()
    process2.join()
    process3.join()

def isweekday():
    """By Ricardo Kazuo"""
#From Tokyo time, it checks the day of the week and the current time to fetch for an specific market
    weekno = datetime.today().weekday()
    #now = datetime.now().time().hour
    hour = datetime.now().time().hour
    #hour = 17
    #weekno = 4
    print(f"{weekno} - {hour}")
    if weekno < 5 and ( hour> 15 and hour < 18 ):
        fetch_by_country('Japan')
    elif weekno < 5 and ( hour> 18 and hour < 20 ):
        fetch_watchlists()
        fetch_data_watchlist()
    elif weekno < 5 and ( hour> 5 and hour < 7 ):
        fetch_by_country('United  States')
        fetch_by_country('Brazil')
    elif weekno < 5 and ( hour> 7 and hour < 10 ):
        fetch_by_country('Others')

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

def fetch_lat_long():
    """By Ricardo Kazuo"""
    df_local = pd.read_csv("Symbol_Info.csv",on_bad_lines='skip')
    df_local = df_local.drop(columns=['Symbol', 'LongName'])
    df_local = df_local.drop_duplicates()
    df_local = df_local.reset_index(drop=True)

    # Initialize Nominatim API
    geolocator = Nominatim(user_agent="MyApp")
    lat=[]
    long=[]
    for row in df_local.iterrows():
        location = geolocator.geocode(row["Country"])
        lat.append(location.latitude)
        long.append(location.longitude)
    df_local["Lat"] = lat
    df_local["Long"] = long
    df_local.to_csv("Country_Info.csv" , mode='w',index=False, header=True)

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

def finance_db(country_in,append,historical,path):
    """
    #Fetch all
    for country in equities.options('country'):
        try:
            df_local=equities.select(country=country)
            df_local.to_csv("FinancialData.csv", mode="a", header=False)
        except ValueError as error:
            print(error)
    """
    print(f"{country_in}-{append}-{path}")
    df_symbol = pd.read_csv("FinancialData.csv",on_bad_lines='skip')
    df_symbol = df_symbol[df_symbol['country'] == country_in]
    count = len(df_symbol.index)
    for index,row in df_symbol.iterrows():
        fetch_on_stock(row['symbol'],country_in+append,historical,path)
        print(row['symbol'] + " - " +str(count))
        count = count - 1

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
    file_to_delete = open(file_in,'w')
    file_to_delete.close()

def main():
    fetch_watchlists()
    fetch_data_watchlist()
    clear_file("./United StatesFinancialDataStocks.csv")
    clear_file("./FinFiles/BrazilFinancialDataStocksIncremental.csv")
    clear_file("./FinFiles/FranceFinancialDataStocksIncremental.csv")
    clear_file("./FinFiles/GermanyFinancialDataStocksIncremental.csv")
    clear_file("./FinFiles/JapanFinancialDataStocksIncremental.csv")
    finance_db("United States","FinancialDataStocks.csv","Full","./")
    finance_db("Brazil","FinancialDataStocksIncremental.csv","Full","./FinFiles/")
    finance_db("France","FinancialDataStocksIncremental.csv","Full","./FinFiles/")
    finance_db("Germany","FinancialDataStocksIncremental.csv","Full","./FinFiles/")
    finance_db("Japan","FinancialDataStocksIncremental.csv","Full","./FinFiles/")

if __name__ == '__main__':
    main()

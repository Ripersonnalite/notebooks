import csv
import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
from datetime import datetime, timedelta, timezone

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

def timenow():
    """By Ricardo Kazuo"""
    try:
        print(f"{datetime.now()- timedelta(days=7):%Y-%m-%d}")
    except:
        pass

def clear_file(file_in):
    """By Ricardo Kazuo"""
    try:
        file_to_delete = open(file_in,'w')
        file_to_delete.close()
    except:
        pass

def fetch_trading():
    """By Ricardo Kazuo"""
    #Fetches all the ticker by category
    clear_file("./Trading/crypto_0.csv")
    clear_file("./Trading/crypto_1.csv")
    clear_file("./Trading/crypto_2.csv")
    clear_file("./Trading/matrix_0.csv")
    clear_file("./Trading/commodity_0.csv")
    clear_file("./Trading/commodity_1.csv")
    clear_file("./Trading/commodity_2.csv")
    clear_file("./Trading/commodity_3.csv")
    clear_file("./Trading/commodity_4.csv")
    clear_file("./Trading/commodity_5.csv")
    clear_file("./Trading/government-bond-10y_0.csv")
    header = {
  "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
  "X-Requested-With": "XMLHttpRequest"
}
    data = pd.DataFrame()
    address = {
    "https://tradingeconomics.com/currencies",
	"https://tradingeconomics.com/stocks",
    "https://tradingeconomics.com/country-list/interest-rate",
    "https://tradingeconomics.com/country-list/inflation-rate",
    "https://tradingeconomics.com/country-list/unemployment-rate",
    "https://tradingeconomics.com/country-list/gdp",
    "https://tradingeconomics.com/country-list/gdp-per-capita",
    "https://tradingeconomics.com/country-list/current-account-to-gdp",
    "https://tradingeconomics.com/country-list/rating",
    "https://tradingeconomics.com/country-list/wage-growth",
    "https://tradingeconomics.com/country-list/gold-reserves",
    "https://tradingeconomics.com/country-list/government-debt-to-gdp",
    "https://tradingeconomics.com/country-list/crude-oil-production",
    "https://tradingeconomics.com/country-list/gasoline-prices",
    "https://tradingeconomics.com/matrix",
    "https://tradingeconomics.com/forecast/government-bond-10y",
    "https://tradingeconomics.com/forecast/commodity",
    "https://tradingeconomics.com/forecast/crypto",
    "https://tradingeconomics.com/forecast/government-bond-10y",
    "https://tradingeconomics.com/forecast/commodity",
    "https://tradingeconomics.com/country-list/coronavirus-cases",
    "https://tradingeconomics.com/country-list/coronavirus-deaths",
    "https://tradingeconomics.com/country-list/coronavirus-vaccination-rate",
    "https://tradingeconomics.com/country-list/coronavirus-vaccination-total"
    }
    for addres in address:
        try:
            file = addres.rpartition('/')
            print(file[2])
            
            r = requests.get(addres, headers=header)
            # Read the HTML tables from the site into a list of DataFrames
            dfs = pd.read_html(r.text)
            
            for index, df in enumerate(dfs):
                print(index, df)
                df.to_csv("./Trading/"+file[2]+"_"+str(index)+".csv", mode="a",index=False)

        except:
            pass

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
                temp =pd.read_html(table)[0]
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
                temp=pd.read_html(table)[0]
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
    #data.iloc[:, :3].to_csv("WatchlistTicker.csv",mode='w',index=False,header=True)
    data = pd.DataFrame()

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


def main():
    fetch_watchlists()
    fetch_data_watchlist()


if __name__ == '__main__':
    main()

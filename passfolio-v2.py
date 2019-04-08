
# coding: utf-8


import datetime as dt
import matplotlib.pyplot as plt
from matplotlib import style
import pandas as pd
import pandas_datareader.data as web
from mpl_finance import candlestick_ohlc
import matplotlib.dates as mdates
import bs4 as bs
import requests
import numpy as np
import os

#%matplotlib inline 


def save_sp500():
    '''
    Get list of SP500 Companies
    '''
    resp = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'id': 'constituents'})
    tickers = []
    names = []
    sectors = []
    sp_500 = pd.DataFrame()
    
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[1].text.replace('.','-')
        name = row.findAll('td')[0].text
        sector = row.findAll('td')[3].text
        tickers.append(ticker)
        names.append(name)
        sectors.append(sector)
    
    sp_500['ticker'] = tickers
    sp_500['name'] = names
    sp_500['sector'] = sectors
    
    if not os.path.exists('data'):
        os.makedirs('data')
        
    sp_500.to_csv('data/sp500.csv')
    return sp_500.ticker.tolist()

_ = save_sp500()



def save_ibovespa():
    '''
    Get list of Ibovespa companies
    '''
    resp = requests.get('https://en.wikipedia.org/wiki/List_of_companies_listed_on_Ibovespa')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'id': 'constituents'})
    tickers = []
    names = []
    sectors = []
    ibovespa = pd.DataFrame()
    
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[1].text.replace('.','-')
        name = row.findAll('td')[0].text
        sector = row.findAll('td')[2].text
        tickers.append(ticker)
        names.append(name)
        sectors.append(sector)
    
    ibovespa['ticker'] = tickers
    ibovespa['name'] = names
    ibovespa['sector'] = sectors
    
    
    ibovespa = ibovespa.set_index(['name', 'sector'])['ticker'].str.split('/').apply(pd.Series).stack()
    ibovespa = ibovespa.reset_index()
    ibovespa.columns = ['name','sector','sample_num','ticker']
    ibovespa = ibovespa.drop('sample_num', axis=1)
    
    if not os.path.exists('data'):
        os.makedirs('data')
        
    ibovespa.to_csv('data/ibovespa.csv')
    return ibovespa.ticker.tolist()

_ = save_ibovespa()
    


def get_data_from_yahoo_sp(reload_sp_500=False):
    '''
    Get stock prices of SP 500 companies
    '''
    if reload_sp_500:
        tickers = save_sp500()
    else:
        sp = pd.read_csv('data/sp500.csv')
        tickers = sp.ticker.tolist()
        
    if not os.path.exists('data/sp500_stocks'):
        os.makedirs('data/sp500_stocks')
    
    start = dt.datetime(2007,1,1)
    end = dt.datetime(2019,4,5)
    
    for ticker in tickers:
        #print(ticker)
        if not os.path.exists('data/sp500_stocks/{}.csv'.format(ticker)):
            df = web.DataReader(ticker, 'yahoo', start, end)
            df.to_csv('data/sp500_stocks/{}.csv'.format(ticker))
        else:
            print('Already have {}'.format(ticker))


def get_data_from_yahoo_ibovespa(reload_ibovespa=False):
    '''
    Get stock prices of Ibovspa companies
    '''
    if reload_ibovespa:
        tickers = save_ibovespa()
    else:
        ibovespa = pd.read_csv('data/ibovespa.csv')
        tickers = ibovespa.ticker.tolist()
        
    if not os.path.exists('data/ibovespa_stocks'):
        os.makedirs('data/ibovespa_stocks')
    
    start = dt.datetime(2007,1,1)
    end = dt.datetime(2019,4,5)
    
    for ticker in tickers:
        #print(ticker)
        if not os.path.exists('data/ibovespa_stocks/{}.csv'.format(ticker)):
            try:
                df = web.DataReader(ticker+'.SA', 'yahoo', start, end)
                df.to_csv('data/ibovespa_stocks/{}.csv'.format(ticker))
            except:
                #continue
                print('Could not find {}'.format(ticker))
        else:
            print('Already have {}'.format(ticker))


get_data_from_yahoo_sp()

get_data_from_yahoo_ibovespa()


def compile_data_sp():
    '''
    Compile SP 500 companies to one list
    '''
    sp = pd.read_csv('data/sp500.csv')
    tickers = sp.ticker.tolist()
    
    main_df = pd.DataFrame()
    
    for ticker in tickers:
        df = pd.read_csv('data/sp500_stocks/{}.csv'.format(ticker))
        df.set_index('Date', inplace=True)
        
        df.rename(columns = {'Adj Close': ticker}, inplace=True)
        df.drop(['Open', 'High', 'Close', 'Low', 'Volume'], 1, inplace=True)
        
        if main_df.empty:
            main_df = df
        else:
            main_df = main_df.join(df, how='outer')

    main_df.to_csv('data/sp500_joined.csv')


compile_data_sp()


def compile_data_ibovespa():
    '''
    Compile Ibovespa companies to one list
    '''
    ibovespa = pd.read_csv('data/ibovespa.csv')
    tickers = ibovespa.ticker.tolist()
    
    main_df = pd.DataFrame()
    
    for ticker in tickers:
        try:
            df = pd.read_csv('data/ibovespa_stocks/{}.csv'.format(ticker))
            df.set_index('Date', inplace=True)

            df.rename(columns = {'Adj Close': ticker}, inplace=True)
            df.drop(['Open', 'High', 'Close', 'Low', 'Volume'], 1, inplace=True)

            if main_df.empty:
                main_df = df
            else:
                main_df = main_df.join(df, how='outer')
        except:
            #continue
            print('Could not find {}'.format(ticker))
    
    main_df.to_csv('data/ibovespa_joined.csv')


compile_data_ibovespa()

def calculate_pct_change(price_file,end,start,name_file):
    '''
    Calculate profit change in closing prices
    '''
    sp_500 = pd.read_csv(price_file, index_col='Date').T
    sp_500.index.names = ['ticker']
    sp_500['Percentage Change'] = (sp_500[end]-sp_500[start])*100/sp_500[start]
    top_pct = sp_500.nlargest(10, 'Percentage Change')['Percentage Change']
    top_pct = top_pct.to_frame()
    sp500_name_df = pd.read_csv(name_file)
    sp500_name_df = sp500_name_df.join(top_pct, on='ticker', how='inner').sort_values('Percentage Change', ascending=False).drop('Unnamed: 0', axis=1)
    return sp500_name_df


print('--- 10 years ago, top 10 investments across the US stock markets ---')
print(calculate_pct_change('data/sp500_joined.csv','2019-04-05','2009-01-02','data/sp500.csv'))
print('\n')
print('--- 10 years ago, top 10 investments across the Brazil stock markets ---')
print(calculate_pct_change('data/ibovespa_joined.csv','2019-04-05','2009-01-02','data/ibovespa.csv'))
print('\n')
print('\n')
print('--- 5 Years Ago, Top 10 Investments Across the US Stock Markets ---')
print(calculate_pct_change('data/sp500_joined.csv','2019-04-05','2014-01-02','data/sp500.csv'))
print('\n')
print('--- 5 years ago, top 10 investments across the Brazil stock markets ---')
print(calculate_pct_change('data/ibovespa_joined.csv','2019-04-05','2014-01-02','data/ibovespa.csv'))
print('\n--- ---')

def plot_high(exchange,ticker=''):
    '''
    Plot monthly opening, closing, high and low values
    '''
    if ticker:
        df_high= pd.read_csv('data/{}_stocks/{}.csv'.format(exchange,ticker), parse_dates=True, index_col = 0)
    else:
        df_high= pd.read_csv('data/{}.csv'.format(exchange), parse_dates=True, index_col = 0)
    df_ohlc = df_high['Adj Close'].resample('BMS').ohlc()
    df_volume = df_high['Volume'].resample('BMS').sum()
    df_ohlc.reset_index(inplace=True)
    df_ohlc['Date'] = df_ohlc['Date'].map(mdates.date2num)
    plt.figure(figsize=(10,8)).suptitle('{} {}'.format(exchange,ticker))
    #plt.ion()
    #plt.figure().suptitle('{} {}'.format(exchange,ticker))
    ax1 = plt.subplot2grid((8,1),(0,0), rowspan = 5, colspan =1)
    ax2 = plt.subplot2grid((7,1),(5,0), rowspan = 5, colspan =1, sharex=ax1)
    ax1.xaxis_date()
    ax2.fill_between(df_volume.index.map(mdates.date2num),df_volume.values,0);
    candlestick_ohlc(ax1, df_ohlc.values, width =2, colorup='g');
    #plt.show()
    plt.savefig(fname='{} {}'.format(exchange,ticker))
    plt.pause(0.001)
    input("Press [enter] to continue.")

plot_high('sp500','NFLX')


plot_high('ibovespa','LREN3')


def get_data_from_yahoo_index(ticker):
    '''
    Get index data
    '''
    if not os.path.exists('data/'):
        os.makedirs('data/')
    
    start = dt.datetime(2007,1,1)
    end = dt.datetime(2012,12,31)
    

    #print(ticker)
    if not os.path.exists('data/{}.csv'.format(ticker)):
        df = web.DataReader(ticker, 'yahoo', start, end)
        df.to_csv('data/{}.csv'.format(ticker))
    else:
        print('Already have {}'.format(ticker))



get_data_from_yahoo_index('^GSPC')


get_data_from_yahoo_index('^BVSP')


plot_high('^GSPC')


plot_high('^BVSP')

print('\n--- ---')
print('From the graphs, we can see that Brazilian stocks were a better investment during global recession (2007-2012)')
print('\n--- ---')

nvda = pd.read_csv('data/sp500_stocks/nvda.csv', index_col='Date')
nvda_low = nvda[nvda.index == '2014-01-02']['Low'].values.tolist()[0]
nvda_close = nvda[nvda.index == '2019-04-05']['Close'].values.tolist()[0]


amd = pd.read_csv('data/sp500_stocks/amd.csv', index_col='Date')
amd_low = amd[amd.index == '2014-01-02']['Low'].values.tolist()[0]
amd_close = amd[amd.index == '2019-04-05']['Close'].values.tolist()[0]

print('\n--- ---')
print('Maximum returns over the last 5 years')
print('\n--- ---')
print('Value of R$100 5 years ago: $43')
print('Money Available: $43')
print('Maximum profit stock: NVDA')
print('Lowest price of NVDA stock on 2014-01-02: $15.72')
print('Closing price of NVDA stock on 2019-04-05: $190.94')
print('Number of NVDA stocks that can be bought: 2')
print('Money left: $11.55')
print('Another stock less than $11.55 that can be bought: AMD')
print('Lowest price of AMD stock on 2014-01-02: $3.83')
print('Closing price of AMD stock on 2019-04-05: $28.97')
print('Number of AMD stocks that can be bought: 3')
print('Maximum return on investment of R$100, US (assuming current 1$ = 3.85R$): R$'+str(((nvda_close-nvda_low)*2+(amd_close-amd_low)*3+(43-nvda_low*2-amd_low*3))*3.85))


# 2 shares of nvda and 3 shares of amd




elet3= pd.read_csv('data/ibovespa_stocks/elet3.csv', index_col='Date')
elet3_low = elet3[elet3.index == '2014-01-02']['Low'].values.tolist()[0]
elet3_close = elet3[elet3.index == '2019-04-05']['Close'].values.tolist()[0]



print('\n--- ---')
print('Value of R$100 5 years ago: $43')
print('Money Available: $43')
print('Maximum profit stock: ELET3')
print('Lowest price of ELET3 stock on 2014-01-02: $5.51')
print('Closing price of ELET3 stock on 2019-04-05: $35.66')
print('Number of ELET3 stocks that can be bought: 8')
print('Maximum return on investment of R$100, Brazil (assuming current 1$ = 3.85R$): R$'+str(((elet3_close-elet3_low)*8 + 43-elet3_low*8)*3.85))
print('\n--- ---')


# 18 shares of ELET3


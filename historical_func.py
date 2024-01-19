import yfinance as yf
import investpy as inv
import pandas as pd

def run_extract_historical_data():
    #Find the current stocks available on Brazil
    br = inv.stocks.get_stocks(country='brazil')

    #change the tickers and add an "S.A" in the end to find them prices
    ticker = []
    for a in br['symbol']:
        ticker.append(a+".SA")


    #colect data from yahoo for the range 2020 - 2023
    cont = 0
    for valor in ticker:
        if cont == 0:
            tb = yf.download(valor, start = '2020-01-01', end = '2023-12-31')
            tb['Ticker'] = valor
            cont += 1
        else:
            tb2 = yf.download(valor, start = '2020-01-01', end = '2023-12-31')
            tb2['Ticker'] = valor
            tb = pd.concat([tb,tb2], ignore_index = False)
    historical_data = pd.DataFrame(tb)
    historical_data.to_csv("s3://brazil-stock-data/Dailly_Stocks_price.csv")
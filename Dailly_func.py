import yfinance as yf
import investpy as inv
import pandas as pd
from datetime import datetime, timedelta, date

def run_extract_stocks():
    br = inv.stocks.get_stocks(country='brazil')

    ticker = []
    for a in br['symbol']:
        ticker.append(a+".SA")

    today = date.today()
    yesterday = today - timedelta(days=1)
    print(yesterday)
    print(today)

    cont = 0
    for valor in ticker:
        if cont == 0:
            dailly_stocks = yf.download(valor, start = yesterday, end = today)
            dailly_stocks['Ticker'] = valor
            cont += 1
        else:
            dailly_stocks2 = yf.download(valor, start = yesterday, end = today)
            dailly_stocks2['Ticker'] = valor
            dailly_stocks = pd.concat([dailly_stocks,dailly_stocks2], ignore_index = False)

    daily_stock = pd.DataFrame(dailly_stocks)
    daily_stock.to_csv("s3://brazil-stock-data/Dailly_Stocks_price.csv")
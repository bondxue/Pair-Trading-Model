# -*- coding: utf-8 -*
#!/usr/bin/env python3

import json
import datetime as dt
import urllib.request
import pandas as pd

from sqlalchemy import Column, Integer, Float, String
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import inspect

requestURL = "https://eodhistoricaldata.com/api/eod/"
myEodKey = "5ba84ea974ab42.45160048"

startDate = dt.datetime(2018,5,1)
endDate = dt.date.today()

def get_daily_data(symbol, start=startDate, end=endDate, requestType=requestURL, apiKey=myEodKey):
    symbolURL = str(symbol) + ".US?"
    startURL = "from=" + str(start)
    endURL = "to=" + str(end)
    apiKeyURL = "api_token=" + myEodKey
    completeURL = requestURL + symbolURL + startURL + '&' + endURL + '&' + apiKeyURL + '&period=d&fmt=json'
    print(completeURL)
    with urllib.request.urlopen(completeURL) as req:
        data = json.load(req)
        return data
    
def create_pair_table(name, metadata, engine):
	tables = metadata.tables.keys()
	if name not in tables:
		table = Table(name, metadata, 
					Column('symbol', String(50), primary_key=True, nullable=False),
					Column('date', String(50), primary_key=True, nullable=False),
					Column('open', Float, nullable=False),
					Column('high', Float, nullable=False),
					Column('low', Float, nullable=False),
					Column('close', Float, nullable=False),
                       Column('adjusted_close', Float, nullable=False),
					Column('volume', Integer, nullable=False))
		table.create(engine)

def clear_a_table(table_name, metadata, engine):
    conn = engine.connect()
    table = metadata.tables[table_name]
    delete_st = table.delete()
    conn.execute(delete_st)

def populate_stock_data(tickers, metadata, engine, table_name):
    conn = engine.connect()
    table = metadata.tables[table_name]
    for ticker in tickers:
        stock = get_daily_data(ticker)
        print(stock)
        for stock_data in stock:
            #print(k, v)
            trading_date = stock_data['date']
            trading_open = stock_data['open']
            trading_high = stock_data['high']
            trading_low = stock_data['low']
            trading_close = stock_data['close']
            trading_adjusted_close = stock_data['adjusted_close']
            trading_volume = stock_data['volume']
            insert_st = table.insert().values(symbol=ticker, date=trading_date,
					open = trading_open, high = trading_high, low = trading_low,
					close = trading_close, adjusted_close = trading_adjusted_close, 
                       volume = trading_volume)
            conn.execute(insert_st)

def execute_sql_statement(sql_st, engine):
    result = engine.execute(sql_st)
    return result

def build_pair_trading_model():
    # ............
	# ............
  
    
if __name__ == "__main__":
    
    build_pair_trading_model()
    
    

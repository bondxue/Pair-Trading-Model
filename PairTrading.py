# -*- coding: utf-8 -*
# !/usr/bin/env python3

import json
import datetime as dt
import urllib.request
import pandas as pd
import numpy as np

from sqlalchemy import Column, Integer, Float, String
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import ForeignKey
from sqlalchemy import Table
from sqlalchemy import inspect

requestURL = "https://eodhistoricaldata.com/api/eod/"
myEodKey = "5ba84ea974ab42.45160048"

startDate = dt.datetime(2019, 1, 2)
endDate = dt.datetime(2019, 5, 3)

# function to retrieve daily market data for each stock
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


# function to create Pair1Stocks and Pair2Stocks tables
def create_pairstocks_table(name, metadata, engine):
    tables = metadata.tables.keys()
    if name not in tables:
        if name == 'Pair1stocks':
            foreign_key = 'Pairs.symbol1'
        else:
            foreign_key = 'Pairs.symbol2'
        table = Table(name, metadata,
                      Column('symbol', String(50), ForeignKey(foreign_key), primary_key=True, nullable=False),
                      Column('date', String(50), primary_key=True, nullable=False),
                      Column('open', Float, nullable=False),
                      Column('high', Float, nullable=False),
                      Column('low', Float, nullable=False),
                      Column('close', Float, nullable=False),
                      Column('adjusted_close', Float, nullable=False),
                      Column('volume', Integer, nullable=False))
        table.create(engine)

# function to create Pairs table
def create_pairs_table(name, metadata, engine):
    tables = metadata.tables.keys()
    if name not in tables:
        table = Table(name, metadata,
                      Column('symbol1', String(50), primary_key=True, nullable=False),
                      Column('symbol2', String(50), primary_key=True, nullable=False),
                      Column('volatility', Float, nullable=False),
                      Column('profit_loss', Float, nullable=False))
        table.create(engine)

# function to create PairPrices table
def create_pairprices_table(name, metadata, engine):
    tables = metadata.tables.keys()
    if name not in tables:
        table = Table(name, metadata,
                      Column('symbol1', String(50), ForeignKey("Pair1stocks.symbol1"), primary_key=True, nullable=False),
                      Column('symbol2', String(50), ForeignKey("Pair2stocks.symbol2"), primary_key=True, nullable=False),
                      Column('date', String(50), primary_key=True, nullable=False),                      
                      Column('open1', Float, nullable=False),
                      Column('close1', Float, nullable=False),                     
                      Column('open2', Float, nullable=False),
                      Column('close2', Float, nullable=False))
        table.create(engine)


# function to create Trades table
def create_trades_table(name, metadata, engine):
    tables = metadata.tables.keys()
    if name not in tables:
        table = Table(name, metadata,
                      Column('symbol1', String(50), ForeignKey("Pair1stocks.symbol1"), primary_key=True, nullable=False),
                      Column('symbol2', String(50), ForeignKey("Pair2stocks.symbol2"), primary_key=True, nullable=False),
                      Column('date', String(50), primary_key=True, nullable=False),
                      Column('profit_loss', Float, nullable=False))
        table.create(engine)


# function to clear a table
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
            # print(k, v)
            trading_date = stock_data['date']
            trading_open = stock_data['open']
            trading_high = stock_data['high']
            trading_low = stock_data['low']
            trading_close = stock_data['close']
            trading_adjusted_close = stock_data['adjusted_close']
            trading_volume = stock_data['volume']
            insert_st = table.insert().values(symbol=ticker, date=trading_date,
                                              open=trading_open, high=trading_high, low=trading_low,
                                              close=trading_close, adjusted_close=trading_adjusted_close,
                                              volume=trading_volume)
            conn.execute(insert_st)


def execute_sql_statement(sql_st, engine):
    result = engine.execute(sql_st)
    return result


def build_pair_trading_model():
    pass



if __name__ == "__main__":
    # build_pair_trading_model()
    e = create_engine('sqlite:///:memory:', echo=True)
    m = MetaData()
    create_pairs_table('Pairs', m, e)
    create_pairstocks_table('Pair1Stocks', m, e)
    populate_stock_data(['IBM'], m, e, 'Pair1Stocks')
    print(execute_sql_statement('select * from Pair1Stocks', e).fetchall())

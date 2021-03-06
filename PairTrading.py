# -*- coding: utf-8 -*
# !/usr/bin/env python3

import json
import datetime as dt
import urllib.request
import pandas as pd
import numpy as np

from sqlalchemy import Column, ForeignKey, Integer, Float, String
from sqlalchemy import and_, or_, not_
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import inspect

# location_of_pairs = 'csv/PairTrading.csv'
location_of_pairs = 'C://Users//mengh//Dropbox//FRE7831FinancialAnalyticsBigData//Pair-Trading-Model'

engine = create_engine('sqlite:///PairTrading.db')
conn = engine.connect()
conn.execute("PRAGMA foreign_keys = ON")

metadata = MetaData()
metadata.reflect(bind=engine)

start_date = dt.date(2018, 1, 1)
# end_date = dt.datetime.now()
end_date = dt.date(2019, 1, 31)

back_testing_start_date = "2019-01-02"
back_testing_end_date = "2019-05-07"
k = 1


def get_daily_data(symbol,
                   start=dt.datetime(2018, 12, 1),
                   end=dt.date.today(),
                   requestURL='https://eodhistoricaldata.com/api/eod/',
                   apiKey='5ba84ea974ab42.45160048'):
    symbolURL = str(symbol) + '.US?'
    startURL = 'from=' + str(start)
    endURL = 'to=' + str(end)
    apiKeyURL = 'api_token=' + apiKey
    completeURL = requestURL + symbolURL + startURL + '&' + endURL + '&' + apiKeyURL + '&period=d&fmt=json'
    print(completeURL)
    with urllib.request.urlopen(completeURL) as req:
        data = json.load(req)
        return data


def create_stockpairs_table(table_name, metadata, engine):
    table = Table(table_name, metadata,
                  Column('Ticker1', String(50), primary_key=True, nullable=False),
                  Column('Ticker2', String(50), primary_key=True, nullable=False),
                  Column('Volatility', Float, nullable=False),
                  Column('Profit_Loss', Float, nullable=False),
                  extend_existing=True)
    table.create(engine)


def create_pair_table(table_name, metadata, engine):
    tables = metadata.tables.keys()
    if table_name not in tables:
        if table_name == 'Pair1Stocks':
            foreign_key = 'Pairs.Ticker1'
        else:
            foreign_key = 'Pairs.Ticker2'
        table = Table(table_name, metadata,
                      Column('Symbol', String(50), ForeignKey(foreign_key), primary_key=True, nullable=False),
                      Column('Date', String(50), primary_key=True, nullable=False),
                      Column('Open', Float, nullable=False),
                      Column('High', Float, nullable=False),
                      Column('Low', Float, nullable=False),
                      Column('Close', Float, nullable=False),
                      Column('Adjusted_Close', Float, nullable=False),
                      Column('Volume', Integer, nullable=False))
        table.create(engine)


def create_pairprices_table(table_name, metadata, engine):
    table = Table(table_name, metadata,
                  Column('Symbol1', String(50), ForeignKey("Pair1Stocks.Symbol"), primary_key=True, nullable=False),
                  Column('Symbol2', String(50), ForeignKey("Pair2Stocks.Symbol"), primary_key=True, nullable=False),
                  Column('Date', String(50), primary_key=True, nullable=False),
                  Column('Open1', Float, nullable=False),
                  Column('Close1', Float, nullable=False),
                  Column('Open2', Float, nullable=False),
                  Column('Close2', Float, nullable=False),
                  extend_existing=True)
    table.create(engine)


def create_trades_table(table_name, metadata, engine):
    table = Table(table_name, metadata,
                  Column('Symbol1', String(50), ForeignKey("Pair1Stocks.Symbol"), primary_key=True, nullable=False),
                  Column('Symbol2', String(50), ForeignKey("Pair2Stocks.Symbol"), primary_key=True, nullable=False),
                  Column('Date', String(50), primary_key=True, nullable=False),
                  Column('Open1', Float, nullable=False),
                  Column('Close1', Float, nullable=False),
                  Column('Open2', Float, nullable=False),
                  Column('Close2', Float, nullable=False),
                  Column('Qty1', Integer, nullable=False),
                  Column('Qty2', Integer, nullable=False),
                  Column('Profit_Loss', Float, nullable=False),
                  extend_existing=True)
    table.create(engine)


def clear_a_table(table_name, metadata, engine):
    conn = engine.connect()
    table = metadata.tables[table_name]
    delete_st = table.delete()
    conn.execute(delete_st)


def execute_sql_statement(sql_st, engine):
    result = engine.execute(sql_st)
    return result


# function to pouplate data into pair1stocks and pair2stocks
def populate_stock_data(tickers, engine, table_name, start_date, end_date):
    colume_names = ['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Adjusted_Close', 'Volume']
    price_data = []
    for ticker in tickers:
        stock = get_daily_data(ticker, start_date, end_date)
        for stock_data in stock:
            price_data.append([ticker, stock_data['date'], stock_data['open'], stock_data['high'], stock_data['low'],
                               stock_data['close'], stock_data['adjusted_close'], stock_data['volume']])
    stocks = pd.DataFrame(price_data, columns=colume_names)
    stocks.to_sql(table_name, con=engine, if_exists='append', index=False)


def build_pair_trading_model(metadata, engine, start_date, end_date, back_testing_start_date):
    engine.execute('Drop Table if exists Pairs;')

    # create Pairs table and store pair dataframe into Pairs table
    create_stockpairs_table('Pairs', metadata, engine)
    pairs = pd.read_csv('PairTrading.csv')
    pairs['Volatility'] = 0.0
    pairs['Profit_Loss'] = 0.0
    pairs.to_sql('Pairs', con=engine, if_exists='append', index=False)

    # create pair1stocks and pair2stocks tables
    tables = ['Pair1Stocks', 'Pair2Stocks']
    for table in tables:
        create_pair_table(table, metadata, engine)
    inspector = inspect(engine)
    print(inspector.get_table_names())

    # clear pair1stocks and pair2stocks tables
    for table in tables:
        clear_a_table(table, metadata, engine)

    # populate stock data into pair1stocks and pair2stocks tables
    populate_stock_data(pairs['Ticker1'].unique(), engine, 'Pair1Stocks', start_date, end_date)
    populate_stock_data(pairs['Ticker2'].unique(), engine, 'Pair2Stocks', start_date, end_date)

    # create pairprices table
    engine.execute('Drop Table if exists PairPrices;')
    create_pairprices_table('PairPrices', metadata, engine)

    # select data from 3 tables and saved as dataframe and stored into the pairprice table
    select_st = "SELECT Pairs.Ticker1 as Symbol1, " \
                "Pairs.Ticker2 as Symbol2, \
                 Pair1Stocks.Date as Date, " \
                "Pair1Stocks.Open as Open1, " \
                "Pair1Stocks.Adjusted_close as Close1, \
                 Pair2Stocks.Open as Open2, " \
                "Pair2Stocks.Adjusted_close as Close2 \
            FROM Pairs, Pair1Stocks, Pair2Stocks \
            WHERE (Pairs.Ticker1 = Pair1Stocks.Symbol) and (Pairs.Ticker2 = Pair2Stocks.Symbol) and (Pair1Stocks.Date = Pair2Stocks.Date) \
            ORDER BY Symbol1, Symbol2;"

    result_set = execute_sql_statement(select_st, engine)
    result_df = pd.DataFrame(result_set.fetchall())
    result_df.columns = result_set.keys()
    print(result_df)
    result_df.to_sql('PairPrices', con=engine, if_exists='append', index=False)



    select_st = "SELECT * FROM PairPrices WHERE Date <= " + "\"" + back_testing_start_date + "\";"
    result_set = execute_sql_statement(select_st, engine)
    result_df = pd.DataFrame(result_set.fetchall())
    result_df.columns = result_set.keys()
    result_df['Ratio'] = result_df['Close1'] / result_df['Close2']
    result_df_stdev = result_df.groupby(['Symbol1', 'Symbol2'])['Ratio'].std()
    result_df_stdev.to_sql('tmp', con=engine, if_exists='replace')

    update_st = "UPDATE Pairs SET Volatility = (SELECT t.ratio FROM tmp t WHERE t.symbol1 = Pairs.Ticker1 and t.symbol2 = Pairs.Ticker2) \
                     WHERE EXISTS(SELECT * FROM tmp WHERE tmp.Symbol1 = Pairs.Ticker1 and tmp.Symbol2 = Pairs.Ticker2);"
    execute_sql_statement(update_st, engine)
    execute_sql_statement('DROP Table if exists tmp', engine)


class StockPair:
    def __init__(self, symbol1, symbol2, volatility, k, start_date, end_date):
        self.ticker1 = symbol1
        self.ticker2 = symbol2
        self.volatility = volatility
        self.k = k
        self.start_date = start_date
        self.end_date = end_date
        self.trades = {}  # dict: key: date, value: list of open1, open2, close1, close2, qty1, qty2, profit_loss
        self.total_profit_loss = 0.0

    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__) + "\n"

    def __repr__(self):
        return str(self.__class__) + ": " + str(self.__dict__) + "\n"

    def createTrade(self, date, open1, close1, open2, close2, qty1=0, qty2=0, profit_loss=0.0):
        self.trades[date] = np.array([open1, close1, open2, close2, qty1, qty2, profit_loss])

    def updateTrades(self):
        trades_matrix = np.array(list(self.trades.values()))
        for index in range(1, trades_matrix.shape[0]):  # all dates
            if abs(trades_matrix[index - 1, 1] / trades_matrix[index - 1, 3] - trades_matrix[index, 0] / trades_matrix[
                index, 2]) > self.k * self.volatility:
                trades_matrix[index, 4] = -10000
                trades_matrix[index, 5] = int(10000 * (trades_matrix[index, 0] / trades_matrix[index, 2]))
            else:
                trades_matrix[index, 4] = 10000
                trades_matrix[index, 5] = int(-10000 * (trades_matrix[index, 0] / trades_matrix[index, 2]))
            trades_matrix[index, 6] = trades_matrix[index, 4] * (trades_matrix[index, 1] - trades_matrix[index, 0]) \
                                      + trades_matrix[index, 5] * (trades_matrix[index, 3] - trades_matrix[index, 2])
            trades_matrix[index, 6] = round(trades_matrix[index, 6], 2)

            self.total_profit_loss += trades_matrix[index, 6]

        for key, index in zip(self.trades.keys(), range(0, trades_matrix.shape[0])):
            self.trades[key] = trades_matrix[index]

        return pd.DataFrame(trades_matrix[:, range(4, trades_matrix.shape[1])], columns=['Qty1', 'Qty2', 'Profit_Loss'])


def back_testing(metadata, engine, k, back_testing_start_date, back_testing_end_date):
    engine.execute('Drop Table if exists Trades;')
    stock_pair_map = dict()

    select_st = 'SELECT Ticker1, Ticker2, Volatility From Pairs;'
    result_set = execute_sql_statement(select_st, engine)
    result_df = pd.DataFrame(result_set.fetchall())
    result_df.columns = result_set.keys()

    for index, row in result_df.iterrows():
        aKey = (row['Ticker1'], row['Ticker2'])
        stock_pair_map[aKey] = StockPair(row['Ticker1'], row['Ticker2'], row['Volatility'], k, back_testing_start_date,
                                         back_testing_end_date)

    select_st = "Select * From PairPrices WHERE Date >= " + "\"" + back_testing_start_date + "\"" + \
                " AND Date <= " + "\"" + back_testing_end_date + "\"" + ";"
    result_set = execute_sql_statement(select_st, engine)
    result_df = pd.DataFrame(result_set.fetchall())
    result_df.columns = result_set.keys()

    for index in range(0, result_df.shape[0]):
        aKey = (result_df.at[index, 'Symbol1'], result_df.at[index, 'Symbol2'])
        stock_pair_map[aKey].createTrade(result_df.at[index, 'Date'], result_df.at[index, 'Open1'],
                                         result_df.at[index, 'Close1'],
                                         result_df.at[index, 'Open2'], result_df.at[index, 'Close2'])

    trades_df = pd.DataFrame(columns=['Qty1', 'Qty2', 'Profit_Loss'])
    for key, value in stock_pair_map.items():
        trades_df = trades_df.append(value.updateTrades(), ignore_index=True)
        # print(trades_df)
        np.set_printoptions(precision=2, floatmode='fixed')
        np.set_printoptions(suppress=True)
        # print(key, value)

        table = metadata.tables['Pairs']
        update_st = table.update().values(Profit_Loss=value.total_profit_loss).where(
            and_(table.c.Ticker1 == value.ticker1, table.c.Ticker2 == value.ticker2))
        engine.execute(update_st)

    result_df = result_df.join(trades_df)
    print(result_df)
    engine.execute('Drop Table if exists Trades;')
    create_trades_table('Trades', metadata, engine)
    result_df.to_sql('Trades', con=engine, if_exists='append', index=False)


def EnterPairTrade():
    print("Options:\n 1 - Select k (default k=1)\n 2 - Run Back Test \n 3- Enter a Pair Trade \n 4. Exit. \n")
    loop = 1
    case = '0'
    while loop:
        case = input('')
        if case == '1':
            k = int(input('k = '))
        elif case == '2':
            build_pair_trading_model(metadata, engine, start_date, end_date, back_testing_start_date)
            back_testing(metadata, engine, k, back_testing_start_date, back_testing_end_date)
        elif case == '3':
            print("Enter a pair Trade:")
            Ticker1 = input('Ticker1: ')
            Ticker2 = input('Ticker2: ')
            close1_previous = float(input("Ticker1 Previous Day Close Price: "))
            close2_previous = float(input("Ticker2 Previous Day Close Price: "))
            open1 = float(input("Ticker1 Open Price: "))
            open2 = float(input("Ticker2 Open Price: "))
            close1 = float(input("Ticker1 Close Price: "))
            close2 = float(input("Ticker2 Close Price: "))
            select_st = "Select Volatility from Pairs where Ticker1 = \"" + Ticker1 + "\" and Ticker2 = \"" + Ticker2 + "\";"
            result_set = execute_sql_statement(select_st, engine)
            result_df = pd.DataFrame(result_set.fetchall())
            result_df.columns = result_set.keys()
            sigma = result_df.iloc[0]['Volatility']
            if abs(close1_previous / close2_previous - open1 / open2) > k * sigma:
                vol1 = -10000
                vol2 = int(10000 * (open1 / open2))
            else:
                vol1 = 10000
                vol2 = int(-10000 * (open1 / open2))
            profit_loss = round(vol1 * (open1 - close1) + vol2 * (open2 - close2), 2)
            print(Ticker1, Ticker2)
            print("delta = {d}, k = {k}".format(d=sigma, k=k))
            print("vol1 = {v1}, vol2 = {v2}, P/L = {p}".format(v1=vol1, v2=vol2, p=profit_loss))
        elif case == '4':
            option = 0
            print('exit')


if __name__ == "__main__":
    # app.run()
    # build_pair_trading_model()
    # clear_a_table('Pairs', metadata, engine)
    # create_stockpairs_table('Pairs', metadata, engine)
    # create_pair_table('Pair1Stocks', metadata, engine)
    # populate_stock_data(['IBM'], engine, 'Pair1Stocks',start_date, end_date)
    # print(execute_sql_statement('select * from Pair1Stocks', engine).fetchall())

    # build_pair_trading_model(metadata, engine, start_date, end_date, back_testing_start_date)
    # back_testing(metadata, engine, k, back_testing_start_date, back_testing_end_date)

    EnterPairTrade()


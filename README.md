# Pair-Trading-Model
FRE7831 Financial Analytics &amp; Big Data term project 

---------------------------------
## Condition and Assumption:

1. **Going short** – the first stock of the pair is short and the other is long. 
2. **Going long** - the first stock of the pair is long and the other is short.
3. Always trade *10,000* shares for the first stock (`S1`) and determine the shares for the other (`S2`) accordingly: ```N1P1 + N2P2 = 0```
4. `N1` and `N2` are the numbers of shares of `S1` and `S2`, and `P1` and `P2` are the prices of `S1` and `S2`

## Trading Algorithm:
1. Compute the `standard deviation`, `σp`, of the ratio of the two adjusted closing stock prices in each pair from *1/2/2008* to *12/31/2018*. 
Store this standard deviation in the database.

2. The variable, `k`, has default value 1, but could be changed by user in the pair trading program.

3. Get `Close1d1`, `Close2d1`, `Open1d2`, `Open2d2`, `Close1d2`, and `Close2d2`, where `Close1d1` and `Close2d1` are the closing prices on day *d–1* for stocks 1 and 2, respectively, `Open1d2`, `Open2d2` are the opening prices for day *d*.

4. `Open Trade`:
```Python
    If abs(Close1d1/Close2d1 – Open1d2/Open2d2) >= kσ, 
        short the pair;
    Else go long the pair.
    N1 = 10,000 shares, traded at the price Open1d2, 
    N2 = N1 * (Open1d2/Open2d2), traded at the price Open2d2.
```
5. `Close Trade`:
    The open trades will be closed at the closing prices and P/L for the pair trade will be calculated as:
```Python
        (±N1 * [Open1d2 – Close1d2]) + (±N2 * [Open2d2 – Close2d2])
```

6. Use the stock daily prices from *1/2/2019* to *5/3/2019* for backtesting

7. Your program should support manually entered open and close price for a pair to simulate real-time trading

## Database Implementation:
Create 5 tables, `Pairs`, `Pair1Stocks`, `Pair2Stocks`, `PairPrices`, `Trades`
* `Pairs`: Pair symbols, volatility, profit_loss (from back_testing), 
    + *primary keys*: pair symbols
* `Pair1Stocks` and `Pair2Stocks`: the daily market data retrieved for each stock in the pair from *1/2/2008* to *5/3/2019*
    + *primary keys*: symbol and date
* `PairPrices`: pair symbols, date, open and close price for each stock
    + *primary keys*: pair symbols and date
* `Trades`: pair symbols, date and profit_loss of each day
    + *primary keys*: pair symbols and date

Establish **E-R model** to enforce the relationship of primary keys and foreign keys for each table.

## Python Program:
The database should be a persistent component of your Python program, and all the database queries should be launched from your Python program

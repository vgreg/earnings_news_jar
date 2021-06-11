"""
Code for "How Is Earnings News Transmitted to Stock Prices?" by
Vincent Grégoire and Charles Martineau.

Python 2

The main function processes trades for one event, keeps only
trades starting at the last trade before the event, and computes returns,
time difference between two trades and other trade characteristics.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
import os

from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())

trades_dir = 'M:\\vgregoire\\TRTH_Trades_AroundEarnings\\'
    
def process_task(permno, date, ea_ts, ric, exch):
    
    # Read data
    datestr = date.strftime('%Y-%m-%d')
    fn_trades = (trades_dir + exch + '/' + str(date.year) + '/' + exch +
             '-TradesAroundEvent-' + datestr + '_' + str(permno) + '.csv.gz')
    if not os.path.isfile(fn_trades):
        return None
    df_trades = pd.read_csv(fn_trades)
    
    df_trades['Timestamp'] = pd.to_datetime(df_trades['Date'] + ' ' +
                                df_trades['Time'].apply(lambda x: x[-18:]))
    
    # We care about three type of trades: in AH before the event, in AH after
    # the event, in the morning after the event.
    
    # For events in the after trading period, we want all FormT before
    # next day opening
    # For events in the before trading period, we want all FormT before
    # same-day opening
    if ea_ts.time() > time(12, 0, 0):
        evt_dt = date
    else:
        evt_dt = date - 1*us_bd
        
    open_ts = evt_dt + 1*us_bd + timedelta(hours=9, minutes=30)
    
    sel = ((df_trades.Timestamp > ea_ts) & (df_trades.Timestamp < open_ts) &
           (df_trades.FormT == 1))
    
    df_trades_after = df_trades[sel].copy()
    
    if len(df_trades_after) == 0:
        return None
    
    # Get last trade price before announcement
    if len(df_trades[df_trades.Timestamp < ea_ts] > 0):
        prev_price = df_trades[df_trades.Timestamp < ea_ts].iloc[-1].Price
    else:
        prev_price = np.nan
    prev_logprice = np.log(prev_price)
    
    # Generate trade ID
    df_trades_after['TradeCount'] =  df_trades_after.FormT # Column of ones
    df_trades_after['TradeID'] = df_trades_after.TradeCount.cumsum()
    del df_trades_after['TradeCount']
    
    df_trades_after['Dark'] = 1 * (df_trades_after['Ex/Cntrb.ID'] == 'ADF')
    
    # Listing exchange dummy
    nasdaq_codes = ['NAQ', 'NMQ', 'NSQ']
    nyse_codes = ['ASQ', 'NYQ', 'PSQ']
    
    if exch in nasdaq_codes:
        df_trades_after['Primary'] = 1 * (df_trades_after['Ex/Cntrb.ID'].isin(['NAS', 'THM']))
    elif exch in nyse_codes:
        df_trades_after['Primary'] = 1 * (df_trades_after['Ex/Cntrb.ID'].isin(['PSE', 'NYS', 'ASE']))
    else:
        raise Exception('Unknown exchange')
    
    # Compute trades returns and interval between trade.
    df_trades_after['LogPrice'] = np.log(df_trades_after['Price'])
    df_trades_after['Timestamp_1'] = df_trades_after['Timestamp'].shift()
    df_trades_after['LogPrice_1'] = df_trades_after['LogPrice'].shift()
    
    df_trades_after = df_trades_after.reset_index(drop=True)
    
    df_trades_after.loc[0, 'Timestamp_1'] = ea_ts # Announcement time
    df_trades_after.loc[0, 'LogPrice_1'] = prev_logprice # Pre-announcement log price
    
    df_trades_after['Duration'] = (df_trades_after['Timestamp'] - df_trades_after['Timestamp_1']).dt.total_seconds()
    
    df_trades_after['LogRet'] = df_trades_after['LogPrice'] - df_trades_after['LogPrice_1']
    
    df_trades_after['CumRet'] = df_trades_after['LogRet'].cumsum()
    
    
    df_trades_after['permno'] = permno
    df_trades_after['EA_Timestamp'] = ea_ts
        
        
    outcols = [u'permno', u'EA_Timestamp', u'#RIC', u'Date', u'Timestamp',
               u'Ex/Cntrb.ID', u'Price',
               u'Volume', u'Duration', u'LogRet', u'CumRet', u'LogPrice', 
               u'LogPrice_1',
               u'Sweep', u'OddLot', u'TradeID', u'Dark', u'Primary']
        
    return df_trades_after[outcols].copy()

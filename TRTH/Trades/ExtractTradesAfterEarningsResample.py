"""
Code for "How Is Earnings News Transmitted to Stock Prices?" by
Vincent Grégoire and Charles Martineau.

Python 3

The main function reads the trade file for one event and resamples the last
trade price at one minute intervals.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
import os
import time as tm
import multiprocessing as mp
import logging

import gzip
import shutil

from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay



us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())


trade_dir = 'M:\\vgregoire\\TRTH_Trades_AroundEarnings\\'


def process_task(permno, date, ea_ts, ric, exch):
    datestr = date.strftime('%Y-%m-%d')

    fn_trades = (trade_dir + exch + '/' + str(date.year) + '/' + exch +
                 '-TradesAroundEvent-' + datestr + '_' + str(permno) + '.csv.gz')
    
    if not os.path.isfile(fn_trades):
        #print('Missing file:' + fn_quotes)
        return None
    else:
        try:
            df_trades = pd.read_csv(fn_trades)
        except Exception as e:
            print(e)
            return None

    if len(df_trades) == 0:
        return None

    # Apply filters
    reg_sel = ((df_trades.NextDay == 0) &
               (df_trades.PriorRefPrice == 0) & 
               (df_trades.DerivativelyPriced == 0 ) &
               (df_trades.SoldOutOfSequence == 0))
    cols = ['#RIC', 'Date', 'Time', 'Price', 'Volume',]
    df_trades = df_trades.loc[reg_sel, cols].copy()
    
    
    df_trades = df_trades[df_trades['#RIC'].notnull()]
 
    
    df_trades['Timestamp'] = pd.to_datetime(df_trades['Date'] + ' ' +
                                            df_trades['Time'].str[7:])   

    # Trades that have timestamps outside of extended trading hours may get
    # misclassified, we drop those.
    sel = ((df_trades['Timestamp'].dt.time >= time(4)) &
           (df_trades['Timestamp'].dt.time <= time(20)))

    df_trades = df_trades[sel]

    # We also want at the opening of markets
    open_ts = date + timedelta(hours=9, minutes=30)
    # Shift by one day if necessary
    if ea_ts.time() > time(12):
        open_ts = open_ts + us_bd
    
    # Figure out how far we need to go
    td = open_ts - ea_ts
    after_open = 30
    min_after = int(np.ceil(td.total_seconds()/60)) + after_open
    
    # First, create dataframse to merge on.
    
    ts_ann_1m = pd.DataFrame([[ea_ts + x * timedelta(minutes=1)
                           for x in range(-5, min_after + 1)],
                          range(-5, min_after+ 1)]).T
    ts_ann_1m.columns = ['Timestamp', 'MinutesAfter']
    ts_ann_1m['Timestamp'] = pd.to_datetime(ts_ann_1m['Timestamp'])
    ts_ann_1m['MinutesAfterOpen'] = ts_ann_1m['MinutesAfter'] - ts_ann_1m['MinutesAfter'].max() + after_open

    # Now merge asof to get the valid quotes at each point in time.
    df_trades = df_trades[['Timestamp', 'Price']]
    
    df_trades = df_trades.sort_values('Timestamp')
    
    merge_ann_1m = pd.merge_asof(ts_ann_1m, df_trades,
                                 on='Timestamp',
                                 tolerance=timedelta(minutes=1))

    for x in [merge_ann_1m]:
        x['PERMNO'] = permno
        x['EA_Time'] = date
        x['EA_Timestamp'] = ea_ts
        x['#RIC'] = ric
        x['Exchange'] = exch
    
    id_cols = ['PERMNO', 'EA_Time', 'EA_Timestamp', '#RIC', 'Exchange']
    trade_cols = ['Price']
    
    merge_ann_1m = merge_ann_1m[id_cols + ['MinutesAfter', 'MinutesAfterOpen'] + trade_cols]
    
    return merge_ann_1m


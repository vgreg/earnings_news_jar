"""
Code for "How Is Earnings News Transmitted to Stock Prices?" by
Vincent Grégoire and Charles Martineau.

Python 2

This code extract all trades from one trading day before to one trading
day after each earning announcements.
"""

from os import listdir
import os
import pandas as pd
from datetime import datetime
import gzip
import shutil
import sys

from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())

basedir = 'M:\\vgregoire\\TRTH_Trades_Final\\'
outdir = 'M:\\vgregoire\\TRTH_Trades_AroundEarnings\\'


"""
The summary of the procedure is as follow:
    -we have two timestamps: EA_Time and IBES Timestamp, we need to compute,
        for each event, the trading day before min(EA,IBES) and the trading day
        after max(EA,IBES)
    - The output is one file per event. (permno date)
"""
def process_event(permno, date1, date2, ric, exch):
    permno, date1, date2, ric, exch = task
    
    # One day before to one day after
    date_start = min(date1, date2) - us_bd
    date_end = max(date1, date2) + us_bd
    dr = pd.date_range(date_start, date_end)
    
    dfs = []
    for date in dr:
        y = date.year
        m = date.month
        d = date.day
        datestr = str(y) + '-' + str(m).zfill(2) + '-' + str(d).zfill(2)
        
        fn = (basedir + exch + '\\' + str(y) + '\\' + exch +
              '-TradesParsed-' + datestr + '.csv.gz')
        
        # Keep only trades for the event stock (#RIC)
        for chunk in pd.read_csv(fn, chunksize=1000000):
            dfs.append(chunk[chunk['#RIC'] == ric].copy())
        
    # Prepare output
    df = pd.concat(dfs)
    datestr = date1.strftime('%Y-%m-%d')
    outfn = (outdir + exch + '\\' + str(date1.year) + '\\' + exch +
             '-TradesAroundEvent-' + datestr + '_' + str(permno) + '.csv')
    
    df.to_csv(outfn, index=False)
    with open(outfn, 'r') as f_in, gzip.open(outfn + '.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(outfn)


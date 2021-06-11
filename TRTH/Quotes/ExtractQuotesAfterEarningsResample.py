"""
Code for "How Is Earnings News Transmitted to Stock Prices?" by
Vincent Grégoire and Charles Martineau.

Python 2

The main function reads the quote file for one event and resamples the last
trade price at one second and one minute intervals in event time centered
around the event and around the followong openning cross.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
import os

from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())

quotes_dir = 'M:\\vgregoire\\TRTH_Quotes_AroundEarnings\\'
outdir = 'M:\\vgregoire\\TRTH_Quotes_AroundEarnings_Resample\\'


def process_task(permno, date, ea_ts, ric, exch):
    
    datestr = date.strftime('%Y-%m-%d')
    fn_quotes = (quotes_dir + exch + '/' + str(date.year) + '/' + exch +
             '-QuotesAroundEvent-' + datestr + '_' + str(permno) + '.csv.gz')    
    
    if not os.path.isfile(fn_quotes):
        print('Missing file:' + fn_quotes)
        return None, None, None, None
    df_quotes = pd.read_csv(fn_quotes)
 
    no_bid = ((df_quotes['Bid Size'] == 0.0) |
              (df_quotes['Bid Price'] == 0.0) |
              (df_quotes['NoQuote'] == 1))
    df_quotes.loc[no_bid, 'Bid Price'] = np.nan
    no_ask = ((df_quotes['Ask Size'] == 0.0) |
              (df_quotes['Ask Price'] == 0.0) |
              (df_quotes['NoQuote'] == 1))
    df_quotes.loc[no_ask, 'Ask Price'] = np.nan
    
    df_quotes['Timestamp'] = pd.to_datetime(df_quotes['Date'] + ' ' +
                                            df_quotes['Time'])   
    
    
    # Quote that have timestamps outside of extended trading hours may get
    # misclassified, we drop those.
    sel = ((df_quotes.Time >= '04:00:00.00000') &
           (df_quotes.Time <= '20:00:00.00000'))
    
    df_quotes = df_quotes[sel]
    
    # We want to get quote observations for each seconds in the first
    # 5 minutes following the announcement, and for each minute
    # in the 2 hours follwing the announcements.
    
    # First, create dataframse to merge on.
    ts_ann_1s = pd.DataFrame([[ea_ts + x * timedelta(seconds=1)
                           for x in range(-60 * 5, 60 * 5 + 1)],
                          range(-60 * 5, 60 * 5 + 1)]).T
    ts_ann_1s.columns = ['Timestamp', 'SecondsAfter']
    ts_ann_1s['Timestamp'] = pd.to_datetime(ts_ann_1s['Timestamp'])
    
    
    ts_ann_1m = pd.DataFrame([[ea_ts + x * timedelta(minutes=1)
                           for x in range(-5, 60 * 2 + 1)],
                          range(-5, 60 * 2 + 1)]).T
    ts_ann_1m.columns = ['Timestamp', 'MinutesAfter']
    ts_ann_1m['Timestamp'] = pd.to_datetime(ts_ann_1m['Timestamp'])
    
    
    
    # We also want at the opening of markets
    open_ts = date + timedelta(hours=9, minutes=30)
    # Shift by one day if necessary
    if ea_ts.time() > time(12):
        open_ts = open_ts + us_bd
    
    
    ts_opn_1s = pd.DataFrame([[open_ts + x * timedelta(seconds=1)
                           for x in range(-60 * 5, 60 * 5 + 1)],
                          range(-60 * 5, 60 * 5 + 1)]).T
    ts_opn_1s.columns = ['Timestamp', 'SecondsAfter']
    ts_opn_1s['Timestamp'] = pd.to_datetime(ts_opn_1s['Timestamp'])
    
    
    ts_opn_1m = pd.DataFrame([[open_ts + x * timedelta(minutes=1)
                           for x in range(-5, 60 * 2 + 1)],
                          range(-5, 60 * 2 + 1)]).T
    ts_opn_1m.columns = ['Timestamp', 'MinutesAfter']
    ts_opn_1m['Timestamp'] = pd.to_datetime(ts_opn_1m['Timestamp'])
    
    
    # Now merge asof to get the valid quotes at each point in time.
    df_quotes = df_quotes[['Timestamp', 'Bid Price', 'Bid Size',
                           'Ask Price', 'Ask Size']]
    
    
    df_quotes = df_quotes.sort_values('Timestamp')
    
    
    merge_ann_1s = pd.merge_asof(ts_ann_1s, df_quotes,
                                 on='Timestamp')
    
    merge_ann_1m = pd.merge_asof(ts_ann_1m, df_quotes,
                                 on='Timestamp')
    
    merge_opn_1s = pd.merge_asof(ts_opn_1s, df_quotes,
                                 on='Timestamp')
    
    merge_opn_1m = pd.merge_asof(ts_opn_1m, df_quotes,
                                 on='Timestamp')
    
    for x in [merge_ann_1s, merge_ann_1m, merge_opn_1s, merge_opn_1m]:
        x['PERMNO'] = permno
        x['EA_Time'] = date
        x['EA_Timestamp'] = ea_ts
        x['#RIC'] = ric
        x['Exchange'] = exch
    
    id_cols = ['PERMNO', 'EA_Time', 'EA_Timestamp', '#RIC', 'Exchange']
    quote_cols = ['Bid Price', 'Bid Size', 'Ask Price', 'Ask Size']
    
    merge_ann_1s = merge_ann_1s[id_cols + ['SecondsAfter'] + quote_cols]
    merge_ann_1m = merge_ann_1m[id_cols + ['MinutesAfter'] + quote_cols]
    merge_opn_1s = merge_opn_1s[id_cols + ['SecondsAfter'] + quote_cols]
    merge_opn_1m = merge_opn_1m[id_cols + ['MinutesAfter'] + quote_cols]
    
    return merge_ann_1s, merge_ann_1m, merge_opn_1s, merge_opn_1m

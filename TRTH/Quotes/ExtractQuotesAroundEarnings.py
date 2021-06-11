"""
Code for "How Is Earnings News Transmitted to Stock Prices?" by
Vincent Grégoire and Charles Martineau.

Python 2

This code extract all quotes from one trading day before to one trading
day after each earning announcements.
"""


import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import gzip
import shutil

from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())


basedir = 'M:\\vgregoire\\TRTH_Quotes\\'
outdir = 'M:\\vgregoire\\TRTH_Quotes_AroundEarnings\\'


#%% Utility functions for parsing timestamps and dates quickly
def parse_exch_time(x):
    if type(x) != str:
        return x
    return timedelta(hours=int(x[0:2]),
                     minutes=int(x[3:5]),
                     seconds=int(x[6:8]),
                     milliseconds=int(x[9:13]))


def parse_simple_date(d):
    return datetime.strptime(d, '%d-%b-%Y')


def fast_date_parse(df, col, date_parser=parse_simple_date):
    dt = pd.DataFrame(df[col].unique())
    dt.columns = [col + '_tmp']
    dt[col] = dt[col + '_tmp'].apply(date_parser)
    date_dict = dt.set_index(col + '_tmp').to_dict()
    df[col] = df[col].map(date_dict[col])
    return df


#%% Functions for parsing quote qualifiers
    
def qualif_is_regular_quote(x):
    for c in [a.strip() for a in x.split(';')]:
        if c.endswith('[PRC_QL_CD]'):
            if 'R' == c[:-11].strip():
                return True
        elif c.endswith('[PRC_QL3]'):
            if 'R' == c[:-9].strip():
                return True
    return False

def qualif_is_no_quote(x):
    for c in [a.strip() for a in x.split(';')]:
        if c.endswith('[PRC_QL_CD]'):
            if 'NQ' == c[:-11].strip():
                return True
        elif c.endswith('[PRC_QL3]'):
            if 'NQ' == c[:-9].strip():
                return True
    return False

def qualif_is_opening_quote(x):
    for c in [a.strip() for a in x.split(';')]:
        if c.endswith('[PRC_QL_CD]'):
            if 'OQ' == c[:-11].strip():
                return True
        elif c.endswith('[PRC_QL3]'):
            if 'OQ' == c[:-9].strip():
                return True
    return False

def qualif_is_closing_quote(x):
    for c in [a.strip() for a in x.split(';')]:
        if c.endswith('[PRC_QL_CD]'):
            if 'CQ' == c[:-11].strip():
                return True
        elif c.endswith('[PRC_QL3]'):
            if 'CQ' == c[:-9].strip():
                return True
    return False



# Classifies the quotes in the given dataframe.
def clean_chunk(df):
    df['TS'] = df['Quote Time']
    sel = df['Quote Time'].isnull()
    df.loc[sel, 'TS'] = df.loc[sel, 'Time[G]']
    
    # As opposed to trades, we have only one "date" field, which corresponds
    # to the TRTH timestamp. We need to make sure we align all those timestamps
    # cleanly and get the proper date while using the Quote Time.
    df = fast_date_parse(df, 'TS', date_parser=parse_exch_time)
    df = fast_date_parse(df, 'Time[G]', date_parser=parse_exch_time)
    df = fast_date_parse(df, 'Date[G]')
    
    df['TimeDiff'] = df['Time[G]'] - df['TS']
    
    # In case there is a shift of day at the right instant
    # (Time[G] would be much larger, close to midnight, while TS would be
    #  close to 0.)
    # It is unlikely, but also posisble that the same would occur.
    sel = df['TimeDiff'] > timedelta(hours=23)
    df.loc[sel, 'TimeDiff'] = df.loc[sel, 'TimeDiff'] - timedelta(hours=24)
    sel = df['TimeDiff'] < timedelta(hours=-23)
    df.loc[sel, 'TimeDiff'] = df.loc[sel, 'TimeDiff'] + timedelta(hours=24)
    
    df['QuoteTime'] = (df['Date[G]'] + df['Time[G]'] - df['TimeDiff']  +
                       df['GMT Offset'] * timedelta(hours=1))
    
    df.loc[df['Qualifiers'].isnull(), 'Qualifiers'] = ''
    
    quals = pd.DataFrame(df['Qualifiers'].unique())
    quals.columns = ['quals']
    
    quals_list = [('Regular', qualif_is_regular_quote),
                  ('Opening', qualif_is_opening_quote),
                  ('Closing', qualif_is_closing_quote),
                  ('NoQuote', qualif_is_no_quote)]
    
    for label, fct in quals_list:
        quals[label] = quals['quals'].apply(fct) * 1
    
    quals_dict = quals.set_index('quals').to_dict()
    
    for label, fct in quals_list:
        df[label] = df['Qualifiers'].map(quals_dict[label])
    
    # Columns we want to keep in output
    outcols = ['#RIC', 'Date', 'Time', u'Ex/Cntrb.ID', u'Price', u'Volume',
               u'Market VWAP'] + [x[0] for x in quals_list]
    

    # Columns we want to keep in output
    outcols = ['#RIC', 'Date', 'Time', u'Bid Price', u'Bid Size', u'Ask Price',
               u'Ask Size'] + [x[0] for x in quals_list]
    
    
    if len(df) < 1:
        df['Date'] = df['Date[G]']
        df['Time'] = df['QuoteTime']

        return df[outcols].copy()
  
    
    df['Date'] = pd.to_datetime(df['Date[G]'].dt.date)
    df['Time'] = df['QuoteTime'].dt.time
    
    return df[outcols].copy()

"""
The summary of the procedure is as follow:
    -we have two timestamps: EA_Time and IBES Timestamp, we need to compute,
        for each event, the trading day before min(EA,IBES) and the trading day
        after mac(EA,IBES)
    - The output is one file per event. (permno date)

"""
def process_task(task):
    permno, date1, date2, ric, exch = task
    
    date_start = min(date1, date2) - us_bd
    date_end = max(date1, date2) + 2*us_bd
    dr = pd.date_range(date_start, date_end)
    
    dfs = []
    for date in dr:
        y = date.year
        m = date.month
        d = date.day
        datestr = str(y) + '-' + str(m).zfill(2) + '-' + str(d).zfill(2)
        
        fn = (basedir + exch + '\\' + str(y) + '\\' + exch +
              '-Quotes-' + datestr + '.csv.gz')
        
        
        if os.path.isfile(fn):
            for chunk in pd.read_csv(fn, chunksize=1000000):
                chunk = chunk[chunk['#RIC'] == ric].copy()
                chunk = clean_chunk(chunk)
                dfs.append(chunk)
    if len(dfs) == 0:
        return
    df = pd.concat(dfs)
    datestr = date1.strftime('%Y-%m-%d')
    outfn = (outdir + exch + '\\' + str(date1.year) + '\\' + exch +
             '-QuotesAroundEvent-' + datestr + '_' + str(permno) + '.csv')
    
    df.to_csv(outfn, index=False)
    with open(outfn, 'r') as f_in, gzip.open(outfn + '.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(outfn)

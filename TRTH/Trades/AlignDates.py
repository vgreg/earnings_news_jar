"""
Code for "How Is Earnings News Transmitted to Stock Prices?" by
Vincent Grégoire and Charles Martineau.

Python 2

The main function creates proper daily trade files. The issue is that TRTH
uses UTC days as cutoffs for daily files, so the last after-hours trades
in the US market can appear in the next day file if they are reported late. 
This code realigns daily files according to ET, which allows us to process 
daily files in parallel later on.
"""

import pandas as pd
from datetime import datetime, timedelta
import os

import gzip
import shutil


basedir = 'M:\\vgregoire\\\\TRTH_Trades_Parsed\\'
outdir = 'M:\\vgregoire\\TRTH_Trades_Final\\'


chunk_size = 1000000

# Last date in sample
last_dt = datetime(2015, 12, 31)

# Creates proper daily files.
def process_align_dates(exch, date):
    
    date_str = date.strftime('%Y-%m-%d')
    
    post_date = date + timedelta(days=1)
    
    fn1 = (exch + '/' + str(date.year) + '/' +  exch + '-TradesParsed-' +
           date.strftime('%Y-%m-%d') + '.csv')
    
    fn2 = (exch + '/' + str(post_date.year) + '/' +  exch + '-TradesParsed-' +
           post_date.strftime('%Y-%m-%d') + '.csv')
    
    dfs = []
    
    for chunk in pd.read_csv(basedir + fn1 + '.gz', chunksize=chunk_size):
        chunk = chunk[chunk.Date == date_str].copy()
        dfs.append(chunk)
        del chunk
    
    if date != last_dt:
        for chunk in pd.read_csv(basedir + fn2 + '.gz', chunksize=chunk_size):
            chunk = chunk[chunk.Date == date_str].copy()
            dfs.append(chunk)
            del chunk
    df = pd.concat(dfs)
    df.to_csv(outdir + fn1, index=False)
    
    with open(outdir + fn1, 'rb') as f_in, gzip.open(outdir + fn1 + '.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(outdir + fn1)

"""
Code for "How Is Earnings News Transmitted to Stock Prices?" by
Vincent Grégoire and Charles Martineau.

Python 2

The main function takes the TAS (Time and Sales) file for one exchange on one 
month and extracts only the trades from daily files, creating trade files.
"""

from os import listdir
import os
import pandas as pd
from datetime import datetime
import gzip
import shutil
import hashlib
import sys


outdir = 'M:\\vgregoire\\TRTH_Trades\\'


# Checks the md5 has to make sure the raw file is not corrupted
def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


# This function takes the TAS (Time and Sales) file for one exchange on one 
# month and extracts only the trades from daily files, creating trade files.
def process_task(exch, y, m):
    
    mdir = 'Y:\\' + exch + '\\TAS\\' + str(y) + '\\' + str(m).zfill(2) + '\\'
    
    # List dates in the monthly directory
    ls = listdir(mdir)
    
    ls = [fn for fn in ls if fn[15:23] == 'TAS-Data' and fn.endswith('.gz')]
    ls_df = pd.DataFrame(ls, columns=['Filename'])
    ls_df['Date'] = ls_df.Filename.apply(lambda x: datetime.strptime(x[4:14], '%Y-%m-%d'))
    
    # Get all files related to each specific date.
    dates_fn = {datetime.strptime(x[4:14], '%Y-%m-%d'):[] for x in ls}
    for fn in ls:
        dates_fn[datetime.strptime(fn[4:14], '%Y-%m-%d')].append(fn)
    
    # Process all dates
    for date in dates_fn:
        fn = dates_fn[date]
            
        trade_cols = ['#RIC', 'Date[G]', 'Time[G]', 'GMT Offset', 'Type',
                      'Ex/Cntrb.ID', 'Price', 'Volume', 'Market VWAP',
                      'Qualifiers', 'Seq. No.', 'Exch Time',
                      'Trd/Qte Date']
        
        str_cols = ['Ex/Cntrb.ID', 'Exch Time', 'Trd/Qte Date']
        dtypes = {x: object for x in str_cols}
    
        dfs = []
        
        for f in fn:
            # Validate file
            md5_f = md5(mdir+f)
            
            with open(mdir+f+'.md5sum', 'r') as f_cs:
                md5_check = f_cs.readline()[:32]
                
            if md5_f != md5_check:
                sys.stderr.write('Wrong checksum for ' + f)
                continue
            
            # Read the file by chunk to limit memory usage, filtering on trades.
            for chunk in pd.read_csv(mdir + f, chunksize=10000, usecols=trade_cols,
                                     dtype=dtypes):
                df_trades = chunk[chunk.Type=='Trade'].copy()
                del df_trades['Type']
                dfs.append(df_trades)
            
        df = pd.concat(dfs)
        
        # Output
        out_fn = (outdir + exch + '\\' + str(y) + '\\' + exch + '-Trades-' +
                  date.strftime('%Y-%m-%d') + '.csv')
        
        df.to_csv(out_fn)
        
        with open(out_fn, 'r') as f_in, gzip.open(out_fn + '.gz', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(out_fn)
    

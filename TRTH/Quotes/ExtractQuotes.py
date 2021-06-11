"""
Code for "How Is Earnings News Transmitted to Stock Prices?" by
Vincent Grégoire and Charles Martineau.

Python 2

The main function takes the TAS (Time and Sales) file for one exchange on one 
month and extracts only the quotes from daily files, creating quote files.
The code filters on RIC codes (TRTH identifiers) to keep only symbols included
in the sample and relevant columns.
"""


from os import listdir
import os
import pandas as pd
from datetime import datetime
import gzip
import shutil
import hashlib
import sys


outdir = 'M:\\vgregoire\\TRTH_Quotes\\'

# Checks the md5 has to make sure the raw file is not corrupted
def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def process_task(exch, y, m):
    
    # Load RIC list
    earnings = pd.read_csv('../data/Earnings_Announcements_MergedTRTH_2011_2015.csv',
                           usecols=['#RIC'])
    earnings = earnings['#RIC'].unique()
        
    
    mdir = 'Y:\\' + exch + '\\TAS\\' + str(y) + '\\' + str(m).zfill(2) + '\\'
    
    ls = listdir(mdir)
    
    ls = [fn for fn in ls if fn[15:23] == 'TAS-Data' and fn.endswith('.gz')]
    ls_df = pd.DataFrame(ls, columns=['Filename'])
    ls_df['Date'] = ls_df.Filename.apply(lambda x: datetime.strptime(x[4:14], '%Y-%m-%d'))
    
    
    dates_fn = {datetime.strptime(x[4:14], '%Y-%m-%d'):[] for x in ls}
    for fn in ls:
        dates_fn[datetime.strptime(fn[4:14], '%Y-%m-%d')].append(fn)
    
    for date in dates_fn:
        dt_first = True
        
        out_fn = (outdir + exch + '\\' + str(y) + '\\' + exch + '-Quotes-' +
                  date.strftime('%Y-%m-%d') + '.csv')
        
        fn = dates_fn[date]
            
        quote_cols = ['#RIC', 'Date[G]', 'Time[G]', 'GMT Offset', 'Type',
                      'Buyer ID', 'Bid Price', 'Bid Size',
                      'Seller ID', 'Ask Price', 'Ask Size',
                      'Qualifiers', 'Quote Time']
        
        str_cols = ['Quote Time']
        dtypes = {x: object for x in str_cols}
    
        
        for f in fn:
            # Validate file
            md5_f = md5(mdir+f)
            
            with open(mdir+f+'.md5sum', 'r') as f_cs:
                md5_check = f_cs.readline()[:32]
                
            if md5_f != md5_check:
                sys.stderr.write('Wrong checksum for ' + f)
                continue
            
            for chunk in pd.read_csv(mdir + f, chunksize=1000000, usecols=quote_cols,
                                     dtype=dtypes):
                df_quotes = chunk[chunk.Type=='Quote'].copy()
                del df_quotes['Type']
                sel = df_quotes['#RIC'].isin(earnings)
                if sum(sel) > 0:
                    df_quotes = df_quotes[sel]
                    if dt_first:
                        dt_first = False
                        df_quotes.to_csv(out_fn, header=True,
                                         index=False, mode='w')
                    else:
                        df_quotes.to_csv(out_fn, header=False,
                                         index=False, mode='a')
        
        if os.path.isfile(out_fn):
            with open(out_fn, 'r') as f_in, gzip.open(out_fn + '.gz', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove(out_fn)
    


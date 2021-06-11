"""
Code for "How Is Earnings News Transmitted to Stock Prices?" by
Vincent Grégoire and Charles Martineau.

Python 2

The main function takes the trade file for one exchange on one 
day and extracts the timestamp and trade qualifiers.
"""

import pandas as pd
from datetime import datetime, timedelta
import locale
import os
import gzip
import shutil

locale.setlocale(locale.LC_ALL, 'us')

chunk_size = 1000000


basedir = 'X:\\Data\\AfterHours\\TRTH_Trades\\'
errordir =  'M:\\vgregoire\\TRTH_Trades_Errors\\'
base_outdir = 'M:\\vgregoire\\TRTH_Trades_Parsed\\'



# Early close dates in the sample
early_close = [datetime(2010, 11, 26), datetime(2011, 11, 25),
               datetime(2012, 7, 3), datetime(2012, 11, 23),
               datetime(2012, 12, 24),
               datetime(2013, 7, 3), datetime(2013, 11, 29),
               datetime(2013, 12, 24),
               datetime(2014, 7, 3), datetime(2014, 11, 28),
               datetime(2014, 12, 24),
               datetime(2015, 11, 27),
               datetime(2015, 12, 24)]


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

#%% Functions for parsing trade qualifiers

def qualif_is_form_t(x):
    for c in [a.strip() for a in x.split(';')]:
        if c.endswith('_TEXT]'):
            if 'T' in c[:-10]:
                return True
        elif c.endswith('[LSTSALCOND]'):
            if 'T' in c[:-12]:
                return True
    return False


def qualif_is_oddlot(x):
    for c in [a.strip() for a in x.split(';')]:
        if c == 'ODT[IRGCOND]':
            return True
        elif c == 'ODD[IRGCOND]':
            return True
    return False


def qualif_is_closing(x):
    for c in [a.strip() for a in x.split(';')]:
        if c.endswith('_TEXT]'):
            if '6' in c[:-10]:
                return True
        elif c.endswith('[LSTSALCOND]'):
            if '6' in c[:-12]:
                return True
    return False
           
 
def qualif_is_opening(x):
    for c in [a.strip() for a in x.split(';')]:
        if c.endswith('_TEXT]'):
            if 'O' in c[:-10]:
                return True
        elif c.endswith('[LSTSALCOND]'):
            if 'O' in c[:-12]:
                return True
        elif c == 'O [CTS_QUAL]':
            return True
    return False
           
 
def qualif_is_cross(x):
    for c in [a.strip() for a in x.split(';')]:
        if c.endswith('_TEXT]'):
            if 'X' in c[:-10]:
                return True
        elif c.endswith('[LSTSALCOND]'):
            if 'X' in c[:-12]:
                return True
    return False
    
 
def qualif_is_sweep(x):
    for c in [a.strip() for a in x.split(';')]:
        if c.endswith('_TEXT]'):
            if 'F' in c[:-10]:
                return True
        elif c.endswith('[LSTSALCOND]'):
            if 'F' in c[:-12]:
                return True
    return False
    
 
def qualif_is_next_day(x):
    for c in [a.strip() for a in x.split(';')]:
        if c.endswith('_TEXT]'):
            if 'N' in c[:-10]:
                return True
        elif c.endswith('[LSTSALCOND]'):
            if 'N' in c[:-12]:
                return True
    return False
    
    
 
def qualif_is_bunched_trade(x):
    for c in [a.strip() for a in x.split(';')]:
        if c.endswith('_TEXT]'):
            if 'B' in c[:-10]:
                return True
        elif c.endswith('[LSTSALCOND]'):
            if 'B' in c[:-12]:
                return True
    return False
    
 
def qualif_is_prior_reference_price(x):
    for c in [a.strip() for a in x.split(';')]:
        if c.endswith('_TEXT]'):
            if 'P' in c[:-10]:
                return True
        elif c.endswith('[LSTSALCOND]'):
            if 'P' in c[:-12]:
                return True
    return False

# Extended trading hours (Sold Out of Sequence)
def qualif_is_extended_hours(x):
    for c in [a.strip() for a in x.split(';')]:
        if c.endswith('_TEXT]'):
            if 'U' in c[:-10]:
                return True
        elif c.endswith('[LSTSALCOND]'):
            if 'U' in c[:-12]:
                return True
    return False


def qualif_is_derivatively_priced(x):
    for c in [a.strip() for a in x.split(';')]:
        if c.endswith('_TEXT]'):
            if '4' in c[:-10]:
                return True
        elif c.endswith('[LSTSALCOND]'):
            if '4' in c[:-12]:
                return True
    return False


def qualif_is_average_trade_price(x):
    for c in [a.strip() for a in x.split(';')]:
        if c.endswith('_TEXT]'):
            if 'W' in c[:-10]:
                return True
        elif c.endswith('[LSTSALCOND]'):
            if 'W' in c[:-12]:
                return True
    return False
    
def qualif_is_cash_sale(x):
    for c in [a.strip() for a in x.split(';')]:
        if c.endswith('_TEXT]'):
            if 'C' in c[:-10]:
                return True
        elif c.endswith('[LSTSALCOND]'):
            if 'C' in c[:-12]:
                return True
    return False
    
def qualif_is_sold_out_of_sequence(x):
    for c in [a.strip() for a in x.split(';')]:
        if c.endswith('_TEXT]'):
            if 'Z' in c[:-10]:
                return True
        elif c.endswith('[LSTSALCOND]'):
            if 'Z' in c[:-12]:
                return True
    return False
    

# Classifies the trades in the given dataframe.
def process_classify_chunk(exch, date, df):

    # We want to filter the "useless" trades: those with missing or zero volume.
    df = df[df.Volume.notnull() & df.Volume != 0.0].copy()
    
    # Get the exchange date and time, if not available use the TRTH timestamp.
    df['TS'] = df['Exch Time']

    df = fast_date_parse(df, 'TS', date_parser=parse_exch_time)

    df['DT'] = df['Trd/Qte Date']
    sel = df['DT'].isnull()
    df.loc[sel, 'DT'] = df.loc[sel, 'Date[G]']

    df = fast_date_parse(df, 'DT')

    df['TradeTime'] = df['TS'] + df['DT'] + df['GMT Offset'] * timedelta(hours=1)

    df.loc[df['Qualifiers'].isnull(), 'Qualifiers'] = ''
    
    quals = pd.DataFrame(df['Qualifiers'].unique())
    quals.columns = ['quals']

    # List of qualifiers and corresponding functions
    quals_list = [('FormT', qualif_is_form_t), ('Opening', qualif_is_opening),
                  ('Closing', qualif_is_closing), ('Cross', qualif_is_cross),
                  ('Sweep', qualif_is_sweep), ('NextDay', qualif_is_next_day),
                  ('Bunched', qualif_is_bunched_trade),
                  ('PriorRefPrice', qualif_is_prior_reference_price),
                  ('ExtendedHoursSOoS', qualif_is_extended_hours),
                  ('DerivativelyPriced', qualif_is_derivatively_priced),
                  ('AverageTradePrice', qualif_is_average_trade_price),
                  ('CashSale', qualif_is_cash_sale),
                  ('SoldOutOfSequence', qualif_is_sold_out_of_sequence),
                  ('OddLot', qualif_is_oddlot)]
                 
    # Extract qualifiers
    for label, fct in quals_list:
        quals[label] = quals['quals'].apply(fct) * 1
    
    quals_dict = quals.set_index('quals').to_dict()
    
    # Add qualifiers to dataframe
    for label, fct in quals_list:
        df[label] = df['Qualifiers'].map(quals_dict[label])
        
    # Columns we want to keep in output
    outcols = ['#RIC', 'Date', 'Time', u'Ex/Cntrb.ID', u'Price', u'Volume',
               u'Market VWAP'] + [x[0] for x in quals_list]
        
    if len(df) < 1:
        df['Date'] = df['DT']
        df['Time'] = df['TS']

        return (df[outcols].copy(), df, df)
        
    
    df['Date'] = pd.to_datetime(df.TradeTime.dt.date)
    df['Time'] = df.TradeTime - df.Date

    # Also output "wrong trades" (ie. not FormT, Close, Open or NextDay but out of hours.)
    # Keeping track of early close days.
    
    open_time = date +  timedelta(hours=9, minutes=29, seconds=30)
    close_time = date +  timedelta(hours=16, seconds=30)
    if date in early_close:
        close_time = date +  timedelta(hours=13, seconds=30)
    
    
    sel_late = (df.TradeTime > close_time) & ~df.FormT & ~df.Closing & ~df.NextDay
    sel_early = (df.TradeTime < open_time) & ~df.FormT & ~df.Opening
    
    df_late = df[sel_late]
    df_early = df[sel_early]
    

    return (df[outcols].copy(), df_late, df_early)


# Processes trades for one day on one listing exchange.
def process_classify(exch, date):
    
    
    fn = (basedir + exch + '/' + str(date.year) + '/' + exch + '-Trades-' + 
          date.strftime('%Y-%m-%d') + '.csv.gz')
    
    dfs_late = []
    dfs_early = []

    first = True

    out_fn = (base_outdir + exch + '/' + str(date.year) + '/' + exch +
              '-TradesParsed-' + date.strftime('%Y-%m-%d') + '.csv')

    # Read and process by chunk
    for chunk in pd.read_csv(fn, chunksize=chunk_size):
        df, df_late, df_early = process_classify_chunk(exch, date, chunk)
        del chunk
        dfs_late.append(df_late)
        dfs_early.append(df_early)
        
        if first:
            df.to_csv(out_fn, mode='w', header=True, index=False)
            first = False
        else:
            df.to_csv(out_fn, mode='a', header=False, index=False)
        del df
            
    # Compress output
    with open(out_fn, 'r') as f_in, gzip.open(out_fn + '.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(out_fn)
    
    # Documenting potential errors
    df_late = pd.concat(dfs_late)
    del dfs_late
    df_early = pd.concat(dfs_early)
    del dfs_early
    
    late_fn = errordir + exch + '/' + exch + '-LateTrades-' + date.strftime('%Y-%m-%d') + '.csv'
    early_fn = errordir + exch + '/' + exch + '-EarlyTrades-' + date.strftime('%Y-%m-%d') + '.csv'
    
    if len(df_late) > 0:
        df_late.to_csv(late_fn, index=False)
        with open(late_fn, 'r') as f_in, gzip.open(late_fn + '.gz', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(late_fn)
    if len(df_early) > 0:
        df_early.to_csv(early_fn, index=False)
        with open(early_fn, 'r') as f_in, gzip.open(early_fn + '.gz', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(early_fn)
    
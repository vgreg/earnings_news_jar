"""
Code for "How Is Earnings News Transmitted to Stock Prices?" by
Vincent Grégoire and Charles Martineau.

Python 2

The main function creates stats for each event that are used
for descriptive stats tables.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
import oss

from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())


trades_dir = 'M:\\vgregoire\\TRTH_Trades_AroundEarnings/'
outdir = 'M:\\vgregoire\\TRTH_Trades_Stats\\'


def compute_sample_stats(sub_df, date, sample, vol_bins, val_bins):
    sub_df['vol_bin'] = pd.cut(sub_df['Volume'], vol_bins, right=False)
    sub_df['val_bin'] = pd.cut(sub_df['Value'], val_bins, right=False)
    
    vol_count = sub_df.groupby(['#RIC', 'vol_bin'])[['Volume']].count()
    vol_count = vol_count.unstack(level=1)
    vol_count = vol_count.fillna(0)
    # Compute in %
    
    vol_sum = vol_count.sum(axis=1)
    for c in vol_count.columns:
        vol_count[c] = vol_count[c] / vol_sum
    
    val_count = sub_df.groupby(['#RIC', 'val_bin'])[['Value']].count()
    val_count = val_count.unstack(level=1)
    val_count = val_count.fillna(0)
    # Compute in %
    val_sum = val_count.sum(axis=1)
    for c in val_count.columns:
        val_count[c] = val_count[c] / val_sum
        
    vol_count.columns = [c[0] + ' ' + str(c[1]) for c in vol_count.columns]
    val_count.columns = [c[0] + ' ' + str(c[1]) for c in val_count.columns]
    merged = pd.merge(vol_count, val_count,
                      left_index=True, right_index=True)

    
    merged['Date'] = date
    merged['Sample'] = sample
    merged['NumberOfTrades'] = len(sub_df)
    return merged.reset_index().set_index(['#RIC', 'Date', 'Sample'])
   

def process_event(permno, date, ea_ts, ric, exch):
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
    
    df_pre = df_trades[(df_trades.Timestamp.dt.date == evt_dt) &
                       ((df_trades.FormT == 0))].copy()
    df_post = df_trades[(df_trades.Timestamp.dt.date == (evt_dt + 1*us_bd)) &
                       ((df_trades.FormT == 0))].copy()
    
    # Count by size
    vol_bins = [0, 100, 500, 1000, np.inf]
    # Count by $ value
    val_bins = [0, 1000, 5000, 50000, np.inf]
    
    if len(df_pre) > 0:
        df_pre = df_pre[['#RIC', 'Ex/Cntrb.ID', 'Price', 'Volume', 'FormT']]
        df_pre['Value'] = df_pre['Price'] * df_pre['Volume']
        
        
        pre_stats = compute_sample_stats(df_pre, date, 'pre_all', vol_bins, val_bins)
    else:
        pre_stats = None
            
    if len(df_post) > 0:
        df_post = df_post[['#RIC', 'Ex/Cntrb.ID', 'Price', 'Volume', 'FormT']]
        df_post['Value'] = df_post['Price'] * df_post['Volume']
        
        
        post_stats = compute_sample_stats(df_post, date, 'post_all', vol_bins, val_bins)
    else:
        post_stats = None
        
        if pre_stats is None:
            return None
    
    stats = pd.concat([pre_stats, post_stats])
    
    stats['#RIC'] = ric

    return stats

import sys, os, re, zipfile, holidays
import numpy as np
import datetime as dt
import pandas as pd
import datetime as dt

RENAME_DATA = {'Date Time':'datetime',
               'Segment ID':'seg_id',
               'UTC Date Time':'datetime_utc',
               'Speed(km/hour)':'speed_kph',
               'Hist Av Speed(km/hour)':'hist_speed_avg_kph',
               'Ref Speed(km/hour)':'ref_speed_kph',
               'Speed(miles/hour)':'speed_mph',
               'Hist Av Speed(miles/hour)':'hist_speed_avg_mph',
               'Ref Speed(miles/hour)':'ref_speed_mph',
               'Travel Time(Minutes)':'minutes',
               'CValue':'c_value',
               'Pct Score30':'score30',
               'Pct Score20':'score20',
               'Pct Score10':'score10',
               'Road Closure':'road_closure',
               'Corridor/Region Name':'location'}

RENAME_META = {'Segment ID':'seg_id', 
               'Segment Length(Kilometers)':'length_km',
               'Segment Length(Miles)':'length_mi'}

#class InrixStore():
#    def __init__(path):

def add_datetime_fields(df, typical_weekday_months=[4,5,9,10]):
    ca_holidays = holidays.CountryHoliday('US', prov=None, state='CA')
    
    df['year'] = df['datetime'].map(lambda x: x.year)
    df['month'] = df['datetime'].map(lambda x: x.month)
    df['hour'] = df['datetime'].map(lambda x: x.hour)
    
    #df['tp'] = 'EV'
    #df.loc[df['hour'].between(3,5),'tp'] = 'EA'
    #df.loc[df['hour'].between(6,8),'tp'] = 'AM'
    #df.loc[df['hour'].between(9,15),'tp'] = 'MD'
    #df.loc[df['hour'].between(16,18),'tp'] = 'PM'

    df['day_of_week'] = df['datetime'].map(lambda x: x.weekday())
    df['is_holiday'] = df['datetime'].map(lambda x: x in ca_holidays) * 1
    df['is_weekday'] = df['day_of_week'].map(lambda x: x < 5) * 1
    df['is_midweek'] = df['day_of_week'].map(lambda x: x in [1, 2, 3]) * 1
    df['is_typical_weekday'] = (df['is_midweek'].eq(1) & df['is_holiday'].eq(0) & df['datetime'].map(lambda x: x.month).isin(typical_weekday_months)) * 1
    
    df['season_year'] = df['year'] 
    df.loc[df['datetime'].map(lambda x: x.month).gt(12) & df['datetime'].map(lambda x: x.day).gt(21),'season_year'] = df['year'] + 1
    #df.loc[df['datetime'].gt(dt.datetime(year,12,21,0,0,0,0,tzinfo)),'season_year'] = df['year'] + 1
    
    df['season'] = 1
    df.loc[df['datetime'].map(lambda x: x.month).ge(3) & df['datetime'].map(lambda x: x.day).ge(20) &
           df['datetime'].map(lambda x: x.month).le(6) & df['datetime'].map(lambda x: x.day).lt(20),
           'season'] = 2
    df.loc[df['datetime'].map(lambda x: x.month).ge(6) & df['datetime'].map(lambda x: x.day).ge(20) &
           df['datetime'].map(lambda x: x.month).le(9) & df['datetime'].map(lambda x: x.day).lt(22),
           'season'] = 3
    df.loc[df['datetime'].map(lambda x: x.month).ge(9) & df['datetime'].map(lambda x: x.day).ge(22) &
           df['datetime'].map(lambda x: x.month).le(12) & df['datetime'].map(lambda x: x.day).lt(21),
           'season'] = 4           
           
    return df
    
def is_date_range_overlap(start1, end1, start2, end2):
    if end1 < start2 or start1 > end2:
        return False
    return True
    
def get_files_by_date_range(path, start_date, end_date, pattern):
    start_time = dt.datetime(*list(int(x) for x in start_date.split('-')))
    # add 1 day to the end_date to get end_time, because the time is set to midnight and would otherwise exclude the last day
    end_time = dt.datetime(*list(int(x) for x in end_date.split('-'))) + dt.timedelta(days=1)

    c = re.compile(pattern)

    #all = os.listdir(path)
    filtered = []
    for root, folders, files in os.walk(path):
        for f in files+folders:
            # if there is a zipped and extracted version, use the extracted version
            if f[-4:] == '.zip' and f[:-4] in folders:
                continue

            m = c.match(f)
            if m != None:
                d = m.groupdict()
                file_start_time = dt.datetime(*list(int(x) for x in d['start_date'].split('-')))
                file_end_time = dt.datetime(*list(int(x) for x in d['end_date'].split('-'))) - dt.timedelta(seconds=1)
                if is_date_range_overlap(start_time, end_time, file_start_time, file_end_time):
                    filtered.append(os.path.join(root,f))
    return filtered
    
def read_inrix_file(path, file, timezone='US/Pacific', filter_typical_weekday=True, save_extracted=False):
    root, fname = os.path.split(path)
    name, ext = os.path.splitext(fname)
    
    
    if ext.lower() == '.zip':
        z = zipfile.ZipFile(path)

        if save_extracted:
            z.extractall(root)
            path = os.path.join(root, name, file)
        else: 
            path = z.extract(name + '/' + file, 'temp')
    else:
        path = os.path.join(path, file)
        
    if file == 'data.csv':
        df = pd.read_csv(path, parse_dates=['Date Time', 'UTC Date Time'])
        df.rename(columns=RENAME_DATA, inplace=True)
        if 'datetime_utc' in df.columns:
            df['datetime'] = df['datetime_utc'].dt.tz_convert(timezone)
        else:
            print('WARNING: UTC DATETIME MISSING')
        df = add_datetime_fields(df)
        
        if filter_typical_weekday:
            df = df.loc[df['is_typical_weekday'].eq(1)]
        
    elif file == 'metadata.csv':
        df = pd.read_csv(path)
        df.rename(columns=RENAME_META, inplace=True)
    return df
    
def apply_resolution(df, resolution, time_bin_var='time_bin'):
    '''
    df: a dataframe of inrix speeds
    resolution: integer between 1 and 60, or "champ"
    time_bin_var: the variable name where the time bin will be stored
    '''
    df = df.copy()
    if len(df) == 0:
        df[time_bin_var] = None
        return df
    if isinstance(resolution, int):
        df[time_bin_var] = df['datetime'].map(lambda x: '{:02d}:{:02d}'.format(x.hour, int(x.minute/resolution)*resolution))
    elif isinstance(resolution, str):
        if not resolution.lower() == 'champ':
            raise Exception('invalid resolution: {}'.format(resolution))
            
        df[time_bin_var] = 'EV'
        time = df['datetime'].map(lambda x: x.time())
        df.loc[time.between(dt.time(3), dt.time(6)), time_bin_var] = 'EA'
        df.loc[time.between(dt.time(6), dt.time(9)), time_bin_var] = 'AM'
        df.loc[time.between(dt.time(9,30), dt.time(15,30)), time_bin_var] = 'MD'
        df.loc[time.between(dt.time(15,30), dt.time(18,30)), time_bin_var] = 'PM'
    return df
        
def merge_segments(df, xref, left_on, right_on, xref_id, c_value=50, filter_c_value=False):
    df = pd.merge(df, xref, left_on=left_on, right_on=right_on)
    groupby = [xref_id,'datetime','year','season_year','season','month','is_holiday','is_midweek','is_typical_weekday','day_of_week','hour']
    agg_args = {'length_km':'sum','length_mi':'sum','minutes':'sum','c_value':'mean'}
    count = df.groupby(groupby, as_index=False).size().rename(columns={'size':'count'})
    count_good = (df.loc[df['c_value'].ge(c_value)]
                    .groupby(groupby, as_index=False)
                    .size()
                    .rename(columns={'size':'count_good'})
                 )
    c, cg = len(count), len(count_good)
    if c == 0:
        p = 0 
    else: 
        p = 100.0*cg/c
    print('total records: {}, records over c_value {}: {} ({:0.1f}%)'.format(c, c_value, cg, p))
    
    if filter_c_value:
        print('dropping records with c_value < {}'.format(c_value))
        df = df.loc[df['c_value'].ge(c_value)]
    merged = (df.groupby(groupby,as_index=False)
                .agg(agg_args)
             )
    merged['speed_kph'] = merged['length_km'] * 60.0 / merged['minutes']
    merged['speed_mph'] = merged['length_mi'] * 60.0 / merged['minutes']
    merged = pd.merge(merged, count)
    merged = pd.merge(merged, count_good)
    
    return merged
    
def aggregate(df, groupby, buffer_time_quantile=95):
    '''
    These aggregations are predefined, it's just the groupby that varies
    '''
    if buffer_time_quantile < 1.0:
        buffer_time_quantile = int(buffer_time_quantile) * 100
        
    quantiles = [0.05, 0.10, 0.15, 0.85, 0.90, 0.95]
    agg_args = {'minutes':['mean','std'],
        'speed_mph':['mean','std'],
        'count':['sum'],
        'count_good':['sum']}
    a_func = ['mean','std']
    q_func = ['q{:d}'.format(int(x*100)) for x in quantiles]
    a_cols = ['minutes_{}'.format(x) for x in a_func] + ['mph_{}'.format(x) for x in a_func] + ['count','count_good']
    ord_cols = ['minutes_{}'.format(x) for x in a_func + q_func] + ['mph_{}'.format(x) for x in a_func + q_func] + ['bti', 'count','count_good']
    
    
    df_agg = df.groupby(groupby).agg(agg_args)
    df_agg.columns = a_cols
    for q in quantiles:
        df_agg['minutes_q{:d}'.format(int(q*100))] = df.groupby(groupby)['minutes'].quantile(q)
        df_agg['mph_q{:d}'.format(int(q*100))] = df.groupby(groupby)['speed_mph'].quantile(q)
    df_agg['bti'] = (df_agg['minutes_q{:d}'.format(buffer_time_quantile)] - df_agg['minutes_mean']) / df_agg['minutes_mean']
    df_agg = df_agg[ord_cols]
    
    return df_agg
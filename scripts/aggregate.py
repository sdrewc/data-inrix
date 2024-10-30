import sys, os, re, pytz, zipfile, holidays
import numpy as np
import datetime as dt
import pandas as pd
import tomllib

try:
    from inrix.stats import add_datetime_fields, is_date_range_overlap, get_files_by_date_range, read_inrix_file, merge_segments, apply_resolution, aggregate
except:
    sys.path.insert(0, '<localpath>')
    from agg import (add_datetime_fields, 
                     is_date_range_overlap, 
                     get_files_by_date_range, 
                     read_inrix_file, 
                     merge_segments, 
                     apply_resolution, 
                     aggregate)

if __name__ == '__main__':
    # load the config file
    args = sys.argv[1:]
    configfile = args[0]
    with open(configfile, "rb") as f:
        config = tomllib.load(f)
    
    # inrix input parameters
    input = config['input']
    # inrix input parameters
    indir = input['directory']
    prefix_in = input['prefix']
    reso_in = '{:d}_min'.format(input['resolution'])
    dates = config['dates']
    if isinstance(dates, list):
        dates = {'all': dates}

    save_extracted = input['save_extracted']

    # inrix filename regular expression pattern
    start = '(?P<start_date>\d{4}-\d{2}-\d{2})'
    end = '(?P<end_date>\d{4}-\d{2}-\d{2})'
    ext = '(?P<ext>(.zip){0,1})'
    pattern = '{}_{}_to_{}_{}_part_(\d+){}'.format(prefix_in, start, end, reso_in, ext)
    c = re.compile(pattern)

    # xref
    xref = pd.read_csv(input['xref']) #.rename(columns={'xd_id':'seg_id', 'cmp_segid':'cmp_id'})
    xref_join = input['xref_join']
    xref_id = input['xref_id']
    #SEG_MAP_API = r'https://api.sfcta.org/commapi/sf_xd_2101_agg_view'
    #segments = pd.read_json(SEG_MAP_API)
    
    # output
    output = config['output']
    outdir = output['directory']
    #reso_out = output['resolution']
    #time_bin_var = output['time_bin_var']
    prefix_out = output['prefix']
    timezone = output['timezone']
    filter_typical_weekday = output['filter_typical_weekday']
    c_value = output['c_value']
    filter_c_value = output['filter_c_value']
    date_range_var = output['date_range_var']
    buffer_time_quantile = output['buffer_time_quantile']

    resolutions = config['resolutions']
    # aggregations
    aggregations = config['aggregations']
    
    dfs = []

    tzinfo = pytz.timezone('US/Pacific')
    for name, (start_date, end_date) in dates.items():
        print("{} Date range {} - {}".format(dt.datetime.now(), start_date, end_date))
        start_time = dt.datetime(*list(int(x) for x in start_date.split('-')), tzinfo=tzinfo)
        end_time = dt.datetime(*list(int(x) for x in end_date.split('-')), tzinfo=tzinfo) + dt.timedelta(days=1)
        
        for filepath in get_files_by_date_range(indir, start_date, end_date, pattern):
            print('{} Reading inrix data file {}'.format(dt.datetime.now(), filepath))
            df = read_inrix_file(filepath, 'data.csv', 
                                 filter_typical_weekday=filter_typical_weekday,
                                 timezone=timezone, 
                                 save_extracted=save_extracted)

            print('{} Reading inrix meta file {}'.format(dt.datetime.now(), filepath))
            meta = read_inrix_file(filepath, 
                                   'metadata.csv', 
                                   timezone=timezone,
                                   save_extracted=save_extracted)
            print('{} Merging meta and data files'.format(dt.datetime.now()))
            # this logic should go into a "merge_meta" function
            merge_cols = ['seg_id']
            calc_cols = []
            for m in ['length_km', 'length_mi']:
                if m in meta.columns:
                    merge_cols = merge_cols + [m]
                    
            df = pd.merge(df, meta[merge_cols])
            
            if 'speed_mph' not in df.columns:
                df['speed_mph'] = df['speed_kph'] * 0.621371
            if 'speed_kph' not in df.columns:
                df['speed_kph'] = df['speed_mph'] / 0.621371
            if 'length_mi' not in df.columns:
                df['length_mi'] = df['length_km'] * 0.621371
            if 'length_km' not in df.columns:
                df['length_km'] = df['length_mi'] / 0.621371
            df[date_range_var] = name

            print('{} Merging xref and data files'.format(dt.datetime.now()))
            df = merge_segments(df, xref, 
                                left_on='seg_id', 
                                right_on=xref_join, 
                                xref_id=xref_id, 
                                c_value=c_value, 
                                filter_c_value=filter_c_value)
            
            df = df.loc[df['datetime'].between(start_time, end_time)]
            
            for i, d in resolutions.items():
                name = d['name']
                reso = d['resolution']
                df = apply_resolution(df, 
                                      resolution=reso, 
                                      time_bin_var=name)
            
            dfs.append(df)

    df2 = pd.concat(dfs)
    
    print("{} Preparing aggregations".format(dt.datetime.now()))
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    for name, args in aggregations.items():
        print("{} Preparing aggregations: {}".format(dt.datetime.now(), name))
        groupby = args['groupby']
        fname = '{}{}.csv'.format(prefix_out, name)
        agg = aggregate(df2, groupby, buffer_time_quantile)
        agg.to_csv(os.path.join(outdir, fname))
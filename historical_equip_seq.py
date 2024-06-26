import os
from pathlib import Path
import sys
import time
import pandas as pd
import random
import numpy as np
from stemgen.validation import filter_name
import polars as pl
from datetime import datetime, timedelta

import ctypes
MessageBox = ctypes.windll.user32.MessageBoxW
class colors:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    RESET = '\033[0m' 

time_format = '%Y-%m-%d %H:%M'
#function 1
def date_time_convert(data, column):
    temp_data = data.select(pl.col(column),
        pl.col(column).str.strptime(pl.Datetime, time_format).alias('new_'+str(column)))
    return temp_data['new_'+str(column)][0]



def generate_time_stamps(start, end, dataframe):
    run_time = ( end - start ).total_seconds() / 3600 # run time in hours
    num_of_vessel = len(dataframe)
    average_intervals = (run_time / num_of_vessel) 
    time_stamps_in_hrs = np.cumsum(np.random.poisson(average_intervals, num_of_vessel))
    final_time_stamp = []
    for hour in time_stamps_in_hrs:
        final_time_stamp.append(start + timedelta(hours=int(hour)))
    return final_time_stamp




def generate_data_tonnes(df_choose_from, parameter_row, track):
    current_tonne = 0 
    generatedVehicles_target_tonne  = pl.DataFrame()
    target_tonne = parameter_row['target_tonnes'][0]
    quarter = target_tonne / 4
    half = target_tonne / 2
    three_quarters = quarter*3
    notification_qua = 0 
    notification_half = 0 
    notification_three_qua = 0 

    while target_tonne > current_tonne:
        array_to_choose_from = np.arange(0, len(df_choose_from), 1)
        while len(array_to_choose_from) != 0:
            if target_tonne  < current_tonne:
                return generatedVehicles_target_tonne
            choice = random.choice(array_to_choose_from)
            current_tonne = current_tonne + df_choose_from['tonnes'][int(choice)]
            generatedVehicles_target_tonne = generatedVehicles_target_tonne.vstack(df_choose_from[int(choice)])
            array_to_choose_from = array_to_choose_from[array_to_choose_from!=choice]

            if track == 6:
                    if (current_tonne >= quarter) and (current_tonne < quarter + df_choose_from['tonnes'][int(choice)]) and (notification_qua == 0):
                        print('\n1/4 of this vehicle done')
                        notification_qua = notification_qua + 1
                    if (current_tonne >= half) and (current_tonne < half + df_choose_from['tonnes'][int(choice)]) and (notification_half == 0):
                        print('2/4 of this vehicle done')
                        notification_half = notification_half + 1
                    if (current_tonne >= three_quarters) and (current_tonne < three_quarters + df_choose_from['tonnes'][int(choice)]) and (notification_three_qua == 0):
                        print('3/4 of this vehicle done')
                        notification_three_qua = notification_three_qua + 1
    return generatedVehicles_target_tonne


def generate_data_count(df_choose_from, parameter_row, track_gen):
    current_count = 0
    generatedVehicles_target_count  = pl.DataFrame()
    target_count = parameter_row['target_count'][0]
    quarter = target_count / 4
    half = target_count / 2
    three_quarters = quarter*3
    notification_qua = 0 
    notification_half = 0 
    notification_three_qua = 0 

    if target_count == 0:
        return

    while target_count  >= current_count:
        array_to_choose_from = np.arange(0, len(df_choose_from), 1)
        while len(array_to_choose_from) != 0:
            choice = random.choice(array_to_choose_from)
            current_count = current_count + 1
            generatedVehicles_target_count = generatedVehicles_target_count.vstack(df_choose_from[int(choice)])
            if current_count >= target_count:
                return generatedVehicles_target_count
            array_to_choose_from = array_to_choose_from[array_to_choose_from!=choice]       
            if track_gen == 6:
                if (current_count >= quarter) and (current_count < quarter + 1) and (notification_qua == 0):
                    print('\n1/4 of this vehicle done')
                    notification_qua = notification_qua + 1
                if (current_count >= half) and (current_count < half + 1) and (notification_half == 0):
                    print('2/4 of this vehicle done')
                    notification_half = notification_half + 1
                if (current_count >= three_quarters) and (current_count < three_quarters + 1) and (notification_three_qua == 0):
                    print('3/4 of this vehicle done')
                    notification_three_qua = notification_three_qua + 1
    return generatedVehicles_target_count

def warm_up_and_cool_down(generatedVehicles_to_feed, hist_param, stem_param):
    stem_param_start = datetime.strptime(stem_param['start_date'][0], time_format)
    stem_param_end = datetime.strptime(stem_param['end_date'][0], time_format)
    warm_up_cut_off = stem_param_start + timedelta(days = stem_param['warmup_days'][0])
    cool_down_cut_off = stem_param_end - timedelta(days = stem_param['cooldown_days'][0])
    for row in range(len(hist_param)):
        if datetime.strptime(hist_param['start_date'][row] , time_format) == stem_param_start:
            paramId = hist_param['paramId'][row]
            data_to_find_offset = generatedVehicles_to_feed.filter(pl.col('paramId') == paramId)
            warm_up_offset_dup = data_to_find_offset.filter(pl.col("arrivalDateTime") <= warm_up_cut_off)
            if len(warm_up_offset_dup) != 0:
                warm_up_offset_dup_to_add = warm_up_offset_dup.with_columns(warm_up_offset_dup['arrivalDateTime'] - timedelta(days=stem_param['warmup_days'][0]))
                generatedVehicles_to_feed = generatedVehicles_to_feed.vstack(warm_up_offset_dup_to_add)
        
        if datetime.strptime(hist_param['end_date'][row] , time_format) == stem_param_end:
            paramId = hist_param['paramId'][row]
            data_to_find_offset = generatedVehicles_to_feed.filter(pl.col('paramId') == paramId)
            cool_down_offset_dup = data_to_find_offset.filter(pl.col("arrivalDateTime") >= cool_down_cut_off)
            if len(cool_down_offset_dup) != 0:
                cool_down_offset_dup_to_add = cool_down_offset_dup.with_columns(cool_down_offset_dup['arrivalDateTime'] + timedelta(days=stem_param['cooldown_days'][0]))
                generatedVehicles_to_feed = generatedVehicles_to_feed.vstack(cool_down_offset_dup_to_add)
    generatedVehicles_to_feed = generatedVehicles_to_feed.sort(by = 'arrivalDateTime')
    return generatedVehicles_to_feed


def chop_off_outsiders(generatedVehicles_to_feed, stem_param):
    stem_param_start = datetime.strptime(stem_param['start_date'][0], time_format)
    stem_param_end = datetime.strptime(stem_param['end_date'][0], time_format)    
    ultimate_start_date = stem_param_start - timedelta(days = stem_param['warmup_days'][0])
    cool_down_cut_off = stem_param_end + timedelta(days = stem_param['cooldown_days'][0])

    generatedVehicles_to_feed = generatedVehicles_to_feed.filter(pl.col("arrivalDateTime") >= ultimate_start_date)
    generatedVehicles_to_feed = generatedVehicles_to_feed.filter(pl.col("arrivalDateTime") <= cool_down_cut_off)
    return generatedVehicles_to_feed


def restricted_poisson(start, end, dataframe):
    time_stamps_in_hours = []
    final_time_stamp = []
    run_time = ( end - start ).total_seconds() / 3600 # run time in hours
    num_of_vessel = len(dataframe)
    average_intervals = (run_time / num_of_vessel) 
    min_lambda = average_intervals*0.5
    max_lambda = average_intervals*1.5
    while (time_stamps_in_hours == []) or (time_stamps_in_hours[-1] > run_time):
        time_stamps_in_hours = []
        # first time_stamp with 0.5 labmda
        first_time_stamp = np.random.poisson(average_intervals*0.5, 1)
        time_stamps_in_hours.append(first_time_stamp)
        while len(time_stamps_in_hours) < len(dataframe):
                generated_time_stamp = np.random.poisson(average_intervals, 1)
                if (generated_time_stamp < max_lambda) and (generated_time_stamp > min_lambda):
                    # accept
                    time_accepted = generated_time_stamp + time_stamps_in_hours[-1]
                    time_stamps_in_hours.append(time_accepted)

    for hour in time_stamps_in_hours:
        final_time_stamp.append(start + timedelta(hours=int(hour)))
    return final_time_stamp
            
    
    


def hist_populate_and_time(user_response):
    # path to this file
    this_file_path = Path(__file__).parent.resolve()
    print(this_file_path)
    os.path.realpath(__file__)
    input_folder = os.path.join(this_file_path,'../../data/inputs' )
    historical_param = pl.read_csv(os.path.join(input_folder,'historical_parameters.csv' ))
    source = pl.read_csv(os.path.join(input_folder,'historical.csv' ))
    stem_param = pl.read_csv(os.path.join(input_folder,'stem_parameters.csv' ))

    # adding "filter_" in order to join two dataframes
    filtered_columns = filter_name(historical_param, 'filter_')
    for col in filtered_columns:
        source_data_col = col[7:]
        filter_added_col_in_source = str('filter_')+source_data_col
        source = source.rename({source_data_col:filter_added_col_in_source})
    
    # golden key for the filter
    historical_param = historical_param.with_row_count(name="paramId", offset=1)


    # create generated vehicles array
    generatedVehicles = pl.DataFrame()

    ## find combination of it from hist partamter table 
    for row in range(len(historical_param)):
        source_data_to_join = source
        historical_param_row = historical_param[row] # this variable has 'start_date','end_date','target_count','target_tonnes'
        filtered_row_witout_target_n_count = historical_param_row.drop(columns=['start_date','end_date','target_count','target_tonnes'])
        # filtering the columns depending on whether it's * or not
        non_wildcard_cols = []
        wild_card_cols = []
        for col in filtered_columns:
            if filtered_row_witout_target_n_count[col][0] != '*':
                non_wildcard_cols.append(col)
            else:
                wild_card_cols.append(col)
        
        if len(wild_card_cols) > 0:
            # if there are wildcard columns, then we are not going to care about those columns. So we drop those columns from the source data to join/merge
            source_data_to_join = source_data_to_join.select(pl.all().exclude(wild_card_cols))

        # merging the source data with non wild card columns to source data which does not have any wildcard columns. 
        filtered_extracted_source = filtered_row_witout_target_n_count.join(source_data_to_join, on = non_wildcard_cols, how='inner')
        # we need to remove the rows that do not have the tonnage 
        match historical_param_row['target_count'][0]:
            case None: # meaning that this case has target tonnage
                # we need to remove the rows that do not have the tonnage 
                filtered_extracted_source = filtered_extracted_source.drop_nulls(subset=['tonnes'])
                if len(filtered_extracted_source) == 0:
                    print('[Error]From row '+ str(row)+ ' in your historical data with target tonnage being present, none of rows in your source data that match with this vehicle characteristics and tonnage. At least, there must be one row with the tonnage to generate the data.')
                    quit()
                generatedVehicles_target_tonne = generate_data_tonnes(filtered_extracted_source, historical_param_row, user_response)
                # time stamp
                start_date = date_time_convert(historical_param_row, 'start_date')
                end_date = date_time_convert(historical_param_row, 'end_date')
                final_time_stamp = restricted_poisson(start_date, end_date, generatedVehicles_target_tonne)
                generatedVehicles_target_tonne = generatedVehicles_target_tonne.with_columns(pl.Series(name="arrivalDateTime", values=final_time_stamp)) 
                generatedVehicles = generatedVehicles.vstack(generatedVehicles_target_tonne)
            
            case other: # meaning that this case has target counts 
                generatedVehicles_target_count = generate_data_count(filtered_extracted_source, historical_param_row, user_response)
                # timestamp
                start_date = date_time_convert(historical_param_row, 'start_date')
                end_date = date_time_convert(historical_param_row, 'end_date')
                final_time_stamp = restricted_poisson(start_date, end_date, generatedVehicles_target_count)
                generatedVehicles_target_count = generatedVehicles_target_count.with_columns(pl.Series(name="arrivalDateTime", values=final_time_stamp)) 
                generatedVehicles = generatedVehicles.vstack(generatedVehicles_target_count)
        if user_response == 6:
            print('Row '+str(row + 1)+' in historical parameter is done.')
    generatedVehicles = generatedVehicles.sort(by = 'arrivalDateTime')
    # Warm-up and cool-down
    generatedVehicles = warm_up_and_cool_down(generatedVehicles, historical_param, stem_param)
    # cut off the data that is the outside of the stretched warm up and cool down duration
    generatedVehicles = chop_off_outsiders(generatedVehicles, stem_param)
    
    generatedVehicles = generatedVehicles.with_columns(pl.lit(None).alias('vehicle_type'))
    generatedVehicles = generatedVehicles.with_columns(pl.lit(None).alias('payload'))
    columns_names = generatedVehicles.columns
    stem_cols = ['paramId', 'arrivalDateTime', 'vehicle_type', 'payload']
    historical_col_names = [x for x in columns_names if x not in stem_cols]
    cols_order = np.append(stem_cols, historical_col_names)
    generatedVehicles = generatedVehicles[cols_order]
    return generatedVehicles
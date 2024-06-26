# import libraries
import os
from pathlib import Path
import ctypes
MessageBox = ctypes.windll.user32.MessageBoxW


import pandas as pd
import numpy as np

# path to this file
this_file_path = Path(__file__).parent.resolve()

# path to input folder
input_path = os.path.join(this_file_path, '../../data/inputs')

# path to input folder
output_path = os.path.join(this_file_path, '../../data/outputs')

# accessing to stem parameter
param = pd.read_csv(os.path.join(input_path,'stem_parameters.csv'))
pop_param = pd.read_csv(os.path.join(input_path,'population_parameters.csv'))    
warm_up = pd.Timestamp(str(param['start_date'].iloc[0]) )  - pd.to_timedelta(param['warmup_days'], unit='days')
cool_down = pd.Timestamp(str(param['end_date'].iloc[0]) )  + pd.to_timedelta(param['cooldown_days'], unit='days')

# filling empty cells with 0 to work with the 
param = param.fillna(0)

def calculate_tonnes_without_cool_warm(final_data, stem_param):
    cut_off_warm_up = final_data[final_data['arrivalDateTime'] > stem_param['start_date'].iloc[0]]
    cut_off_warm_up_and_cool_down = cut_off_warm_up[cut_off_warm_up['arrivalDateTime'] < stem_param['end_date'].iloc[0]]
    sum_of_tonne = sum(cut_off_warm_up_and_cool_down['payload_distribution'])
    return sum_of_tonne


def sequencing_population_mode(Equipment_set):
    # total run time in minutes (we would like our output timestamps to be yy-MM-dd HH:mm) 
    run_time = (cool_down.iloc[0] - warm_up.iloc[0]).total_seconds() / 60 # it's in minutes

    # iterating each vessel type in the population partameter and assign the dates by using Poisson
    for dist in pop_param['stem_vehicle_type']:
        # total vessel count for the particular vessel
        vessel_count = len(Equipment_set[Equipment_set['stem_vehicle_type']== str(dist)])
        # average interval time in days for this vessel type
        average_interval = (run_time / vessel_count)
        # create the interval times for each vessel type
        time_stamps_in_min = np.cumsum(np.random.poisson(average_interval, vessel_count))
        # start_day in the stem parameter + the interval times to generate the actual time stamps
        time_stamp_from_start_date =  (pd.Timestamp(warm_up.iloc[0]))+ pd.to_timedelta(time_stamps_in_min, unit='minute')
        # assign those timestamps to the vessel type 
        Equipment_set.loc[Equipment_set['stem_vehicle_type']==str(dist), 'arrivalDateTime'] = time_stamp_from_start_date
        
    # sort the table by the time
    Equipment_set_with_timestamps = Equipment_set.sort_values(by = 'arrivalDateTime')

    essential_col_in_order = ['paramId', 'arrivalDateTime', 'stem_vehicle_type', 'payload_distribution']
    all_cols = Equipment_set_with_timestamps.columns
    meta_col_in_panda_index = all_cols.drop(essential_col_in_order)
    meta_col = []
    for val in meta_col_in_panda_index.values:
        meta_col.append(val)
    combined_cols = essential_col_in_order + meta_col
    Equipment_set_with_timestamps = Equipment_set_with_timestamps[combined_cols]
    # assign those variables warm_up is when the warm up ends, and cool_down is when cool down starts
    print('Fixed_start_date is '+ str(warm_up.iloc[0])+ ' with '+str(param['warmup_days'].iloc[0])+ ' warm_up days')
    print('Fixed_end_date is '+ str(cool_down.iloc[0])+ ' with '+str(param['cooldown_days'].iloc[0])+ ' cool down days' )
    return Equipment_set_with_timestamps


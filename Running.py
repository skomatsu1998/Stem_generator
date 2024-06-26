# import libraries
import os
from pathlib import Path
import sys
import polars as pl
import pandas as pd
import pyarrow 
import ctypes
MessageBox = ctypes.windll.user32.MessageBoxW
import time

# validation function 
from validation import stem_param_check_2
from Equipment_set_population import pop_equipment
from Sequencing_population_mode import sequencing_population_mode
from Sequencing_population_mode import calculate_tonnes_without_cool_warm
from historical_equip_seq import hist_populate_and_time
from KPI_generator import KPI_gen_historical, KPI_gen_population
# path to this file
this_file_path = Path(__file__).parent.resolve()
# path to input folder
input_path = os.path.join(this_file_path, '../../data/inputs')
param = pd.read_csv(os.path.join(input_path,'stem_parameters.csv'))
class colors:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    RESET = '\033[0m'

# Messaging functions for error messages
stdout_fileno = sys.stdout
stderr_fileno = sys.stderr

# path to the input folder
if os.path.isdir(os.path.join(this_file_path,'../../data/inputs')) == False:
    stderr_fileno.write ("We cannot find the input folder. Hint: is the input folder name spelt correctly? - 'inputs' ")
    quit()
else: input_path = os.path.join(this_file_path, '../../data/inputs')

# path to the output folder
if os.path.isdir(os.path.join(this_file_path,'../../data/outputs')) == False:
    stderr_fileno.write ("We cannot find the output folder. Hint: is the input folder name spelt correctly? - 'outputs' ")
    quit()
else: output_path = os.path.join(this_file_path, '../../data/outputs')

 

# this validates stem_para, historical para and population para csv
test = stem_param_check_2()

if test == None:
    user_response = MessageBox(None, 'Click Yes to generate while tracking the progress, \nClick No to generate without tracking the process, or \nClick Cancel to stop and make change in your input', 'Ready to generate', 3)
    if user_response == 2:
        quit()
start_time = time.time()
if test == None:
    stem_param = pl.read_csv(os.path.join(input_path, 'stem_parameters.csv'))
    match stem_param['mode'][0].lower():
        case 'population': 
            tonnage_in_period = 0 
            equipment_set = pop_equipment(user_response)
            while tonnage_in_period < param['target_payload'].iloc[0]:
                final_data = sequencing_population_mode(equipment_set)
                tonnage_in_period = calculate_tonnes_without_cool_warm(final_data, param)
                if tonnage_in_period < param['target_payload'].iloc[0]:
                    print('We are trying to run the sequencing again because the it did not hit the target tonnage in the period excluding the warm-up and cool-down')
            final_data.to_csv(os.path.join(output_path, 'Population_final_data.csv'))
            KPI_gen_population(final_data)
            print(colors.BLUE + "[Confirmation]"+ colors.RESET +"Final output is in your output folder")
        case 'historical': 
            final_data = hist_populate_and_time(user_response)
            final_data_in_pd = pl.DataFrame(final_data).to_pandas(use_pyarrow_extension_array=True)
            final_data_in_pd.to_csv(os.path.join(output_path, 'Historical_final_data.csv'))
            KPI_gen_historical(final_data_in_pd)
            print(colors.BLUE + "[Confirmation]"+ colors.RESET +"Final output is in your output folder")

else:
    print('\nThere are still errors you need to fix before we proceed to generate data')
    exit 

end_time = time.time()
elapsed_time = end_time - start_time
time_in_min = elapsed_time / 60
print(f"Elapsed time: {elapsed_time} seconds or {time_in_min} minutes")
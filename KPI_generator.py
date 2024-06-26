import os
from pathlib import Path
import pandas as pd
import numpy as np

this_file_path = Path(__file__).parent.resolve()
output_path = os.path.join(this_file_path, '../../data/outputs')

class colors:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    RESET = '\033[0m' 

def KPI_gen_historical(generated_data):

    paramId_arr =[] 
    earliest_arrival_arr =[]
    latest_arrival_arr = []
    count_arr = []
    total_payload_arr = []
    min_payload_arr = []
    avg_payload_arr = []
    max_payload_arr = []


    uniqueID = generated_data['paramId'].unique()
    for id in uniqueID:
        data_to_analyse = generated_data[generated_data['paramId']== id]
        earliest_datetime = data_to_analyse['arrivalDateTime'].min()
        latest_datetime = data_to_analyse['arrivalDateTime'].max()
        count = len(data_to_analyse)
        tonnage_array = np.array(data_to_analyse['tonnes'])
        non_null_numbers = tonnage_array[~np.isnan(tonnage_array)]
        total_payload = sum(non_null_numbers)
        min_pay_load = min(non_null_numbers)
        max_pay_load = max(non_null_numbers)
        average_pay_load = total_payload / len(non_null_numbers)

        # appending the information
        paramId_arr.append(id)
        earliest_arrival_arr.append(earliest_datetime)
        latest_arrival_arr.append(latest_datetime)
        count_arr.append(count)
        total_payload_arr.append(total_payload)
        min_payload_arr.append(min_pay_load)
        max_payload_arr.append(max_pay_load)
        avg_payload_arr.append(average_pay_load)
        
    data = {
            'paramId':paramId_arr,
            'earliest_arrival':earliest_arrival_arr,
            'latest_arrival':latest_arrival_arr,
            'count':count_arr,
            'total_payload':total_payload_arr,
            'min_payload':min_payload_arr,
            'avg_payload':max_payload_arr,
            'max_payload':avg_payload_arr
        }


        
    KPI_historical_df = pd.DataFrame(data)
    KPI_historical_df = KPI_historical_df.sort_values(by='paramId', ascending=True)
    KPI_historical_df.to_csv(os.path.join(output_path, 'Historical_mode_KPI.csv'))
    print(colors.BLUE + "[Confirmation]"+ colors.RESET + "Your historical mode KPI is stored in your output folder")

def KPI_gen_population(generated_data):
    paramId_arr =[] 
    earliest_arrival_arr =[]
    latest_arrival_arr = []
    count_arr = []
    total_payload_arr = []
    min_payload_arr = []
    avg_payload_arr = []
    max_payload_arr = []
    unique_vehicle = generated_data['stem_vehicle_type'].unique()
    
    for vechicle in unique_vehicle:
        data_to_analyse = generated_data[generated_data['stem_vehicle_type']== str(vechicle)]
        earliest_datetime = data_to_analyse['arrivalDateTime'].min()
        latest_datetime = data_to_analyse['arrivalDateTime'].max()
        count = len(data_to_analyse)
        tonnage_array = np.array(data_to_analyse['payload_distribution'])
        non_null_numbers = tonnage_array.astype(float)[~np.isnan(tonnage_array.astype(float))]
        # non_null_numbers = tonnage_array[~np.isnan(tonnage_array)]

        total_payload = sum(non_null_numbers)
        min_pay_load = min(non_null_numbers)
        max_pay_load = max(non_null_numbers)
        average_pay_load = total_payload / len(non_null_numbers)

    # appending the information
        paramId_arr.append(vechicle)
        earliest_arrival_arr.append(earliest_datetime)
        latest_arrival_arr.append(latest_datetime)
        count_arr.append(count)
        total_payload_arr.append(total_payload)
        min_payload_arr.append(min_pay_load)
        max_payload_arr.append(max_pay_load)
        avg_payload_arr.append(average_pay_load)
    data = {
            'paramId':paramId_arr,
            'earliest_arrival':earliest_arrival_arr,
            'latest_arrival':latest_arrival_arr,
            'count':count_arr,
            'total_payload':total_payload_arr,
            'min_payload':min_payload_arr,
            'avg_payload':max_payload_arr,
            'max_payload':avg_payload_arr
        }
    
    KPI_population_df = pd.DataFrame(data)
    KPI_population_df = KPI_population_df.sort_values(by='paramId', ascending=True)
    KPI_population_df.to_csv(os.path.join(output_path, 'Population_mode_KPI.csv'))
    print(colors.BLUE + "[Confirmation]"+ colors.RESET + "Your population mode KPI is stored in your output folder")
    
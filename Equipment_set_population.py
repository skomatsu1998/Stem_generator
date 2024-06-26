# from importlib.metadata import distribution
import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np
import os
from pathlib import Path
    
# path to this file
this_file_path = Path(__file__).parent.resolve()
input_path = os.path.join(this_file_path, '../../data/inputs')
stem_paramters = pd.read_csv(os.path.join(input_path, 'stem_parameters.csv'))
# filling empty cells with 0, otherwise the day/week/month/year calculation will not work
stem_paramters = stem_paramters.fillna(0)
distribution_table = pd.read_csv(os.path.join(input_path, 'distribution.csv')) # keep it here for now
population_parameters = pd.read_csv(os.path.join(input_path, 'population_parameters.csv'))



# Dictionary to store models
models_dict = {}
# Iterate over unique names and create a model for each one
for name in distribution_table['distribution'].unique():
    # Filter data for the current name
    data_for_name = distribution_table[distribution_table['distribution'] == name]
    
    # Extract X and y
    X = data_for_name['percent'].values.reshape(-1, 1)
    y = data_for_name['value'].values.reshape(-1, 1)
    
    # Create and fit a linear regression model
    model = LinearRegression()
    model.fit(X, y)
    
    # Store the model in the dictionary with the name as the key
    models_dict[name] = model

def tonnage_generator(dist_name):
    regression_model_name = dist_name + '_tonnes'
    model = models_dict[regression_model_name]
    x_to_feed = np.random.uniform(low=0, high = 1)
    y = model.predict(np.array(x_to_feed).reshape(-1, 1)).item()
    return y

def pop_equipment(user_response):
    population_parameters['paramId'] = population_parameters.index


    
    cumulative_table = pd.DataFrame(columns=['paramId', 'stem_vehicle_type', 'payload_distribution'])

    # overall time from start_date to end_date in seconds
    time_in_second = (pd.to_datetime(stem_paramters['end_date'].iloc[0]) - pd.to_datetime(stem_paramters['start_date']).iloc[0]).total_seconds()
    time_in_days = time_in_second / 86400
     
    # calculating overall tonnage based on the input time unit 
    time_unit = stem_paramters['payload_time_scale'].iloc[0]
    match time_unit.lower():
        case 'day':
            tonne_per_day =  stem_paramters['target_payload'].iloc[0]
        case 'week':
            tonne_per_day =  stem_paramters['target_payload'].iloc[0] / 7
        case 'month':
            tonne_per_day =  stem_paramters['target_payload'].iloc[0] / 30.437     
        case 'year':
            tonne_per_day =  stem_paramters['target_payload'].iloc[0] / 365.2425   
    
    # ultimate tonnage = tonnage per day x (total days + warm up days + cool down days )
    ultimate_target_tonne =  tonne_per_day * (time_in_days + stem_paramters['warmup_days'].iloc[0] + stem_paramters['cooldown_days'].iloc[0])
    
    quarter_tonne = ultimate_target_tonne / 4
    half_tonne = ultimate_target_tonne / 2
    three_quarters = quarter_tonne*3
    max_tonne = distribution_table['value'].max()
    notifications_quarter = 0
    notifications_half = 0
    notifications_three_qua = 0

    # creating the vehicleTargets table to track on the population and vessel percentage
    vehicleTargets = pd.DataFrame(columns =['paramId','VehicleType', 'currentCount', 'currentPercent', 'targetPercent', 'payload_distribution'])
    vehicleTargets['paramId'] = population_parameters.index
    vehicleTargets['VehicleType'] = population_parameters['stem_vehicle_type']
    vehicleTargets['targetPercent'] = population_parameters['ratio']
    vehicleTargets['payload_distribution'] = population_parameters['payload_distribution']
    vehicleTargets['currentCount'] = 0
    vehicleTargets['currentPercent'] = 0
    vehicleTargets['difference'] = 0

    current_tonnage = 0 
    while current_tonnage < ultimate_target_tonne:
        # Calculate the difference
        vehicleTargets['difference'] = vehicleTargets['targetPercent'] - vehicleTargets['currentPercent']
        # sort the table by the difference and first row [0] is the vehivle to populate  
        vehicleTargets = vehicleTargets.sort_values(by = ['difference'], ascending=[False])
        vehicle_type = vehicleTargets['VehicleType'].iloc[0]
        tonnage = tonnage_generator(str(vehicle_type))
        current_tonnage = current_tonnage + tonnage
        if user_response == 6:
            if (current_tonnage >= quarter_tonne) and (current_tonnage <= quarter_tonne + max_tonne) and (notifications_quarter == 0):
                print('\n1/4 of population is done')
                notifications_quarter = notifications_quarter + 1
            if (current_tonnage >= half_tonne) and (current_tonnage <= half_tonne + max_tonne) and (notifications_half == 0):
                print('2/4 of population for is done')
                notifications_half = notifications_half + 1
            if (current_tonnage >= three_quarters) and (current_tonnage <= three_quarters + max_tonne) and (notifications_three_qua == 0):
                print('3/4 of population for is done')
                notifications_three_qua = notifications_three_qua + 1

        vahicle_to_update = population_parameters[population_parameters['stem_vehicle_type']==vehicle_type].copy()
        vahicle_to_update.loc[vahicle_to_update.index, 'payload_distribution'] = tonnage



        # adding current count + 1
        vehicleTargets.iloc[0, 2] = vehicleTargets.iloc[0, 2]  + 1
        # update percentage 
        vehicleTargets['currentPercent'] = (vehicleTargets['currentCount'] / sum(vehicleTargets['currentCount'])) * 100
        
        # append this row to the distriburion table 
        # cumulative_table = pd.concat([cumulative_table, pd.DataFrame([vahicle_to_update])], ignore_index=True)

        cumulative_table = pd.concat([cumulative_table, vahicle_to_update], ignore_index=True)
        #cumulative_table.loc[len(cumulative_table)] =(vahicle_to_update['paramId'], vahicle_to_update['VehicleType'], vahicle_to_update['payload_distribution'])  

    cumulative_table = cumulative_table.drop(columns = ['ratio'])                                      

    # return the distribution to hand over to sequencing
    return cumulative_table
        



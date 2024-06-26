import numpy as np
import datetime
import os
from pathlib import Path
from datetime import datetime
import polars as pl


# locate this file 
this_file_path = Path(__file__).parent.resolve()
# input folder and output folder 
input_path = os.path.join(this_file_path, '../../data/inputs')
output_path = os.path.join(this_file_path, '../../data/outputs')

datetime_format = "%Y-%m-%d %H:%M"
class colors:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    RESET = '\033[0m'
    
# function 1 
def schema_check(schema_info, data_to_validate, null_cols=[]):
    check = []

    for col, dtype in schema_info.items():
        if (data_to_validate[col].name in null_cols) and (data_to_validate[col].dtype == pl.String):
            continue

        if data_to_validate[col].dtype != dtype:
            print(f"Column '{col}' has incorrect data type.")
            check.append(col)

    for col in data_to_validate.columns:
        if col == 'start_date' or col =='end_date':
            try:
                data_to_validate[col].map_elements(lambda x: datetime.strptime(x, datetime_format))
            except ValueError:
                print(f"Column '{col}' has a bad date format.")
                check.append(col)
        
    return check


# function 2
def only_unique_vals_in_col(data, column):
    if (len(data) != len(data[str(column)].unique())) == True:
        print(colors.RED + "\n[Error]"+ colors.RESET + 'This data type: '+ str(column)+ ' has item(s) repeating its appearance. Every item is supposed to appear once')
        
        return 1
    else:
        return 0

# function 3
def sum_to_hundred(data, param):
    # function to see if the number adds up too 100 exactly
    if data.select(pl.sum(param))[0, 0] != 100:
        total = data.select(pl.sum(param))[0, 0] 
        print(colors.RED + "\n[Error]"+ colors.RESET + 'Please have this parameter adds up to 100, currently it is ' +str(total))
        
        return 1
    else:
        return 0

# function 4
def positive_val(data, param):
    if data[param][0] < 0: 
        print(param + ' must be above 0. It cannot be negative')
        return 1
    else:
        return 0

# function 5 
def filter_name(data, column):
    filtered_name = []

    # for columnName in data.columns:
    #     if (columnName in )
    for i in range(len(data.columns)):
        if (column in data.columns[i]) == True:

            filtered_name.append(data.columns[i])
    return filtered_name


# function 6
def start_end_check(data, start_day, end_day):
    data = data.with_columns(
        pl.col(start_day).str.to_datetime("%Y-%m-%d %H:%M"),
        pl.col(end_day).str.to_datetime("%Y-%m-%d %H:%M"))
    data = data.with_columns(enddate_minus_startdate = (pl.lit(data['end_date'] - data['start_date']).dt.days()))
    data = data.with_columns(day_below_0 = (pl.lit(data['enddate_minus_startdate'] < 0 )))
    date_not_correct = np.count_nonzero(data['day_below_0'])
    if date_not_correct > 0:
        print(str(date_not_correct)+' errors in your start and end date')
    return date_not_correct

# function 7 column exists or not
def column_exists_or_not(data, array_of_columns):

    # columns_match = all(col in data.columns for col in array_of_columns)
    missing_col = [x for x in array_of_columns if x not in data.columns]
    return missing_col

# function 8
def hist_param_filter_col_exists_in_source(source, filter_cols, start_with_word):
    error = 0

    if len(filter_cols) == 0:
        print("No filters present")
        error = error + 1

    for filter in filter_cols :
        if filter.startswith(str(start_with_word)):
            filtered_col_in_source = filter[len(start_with_word):]
            match filtered_col_in_source in source.columns:
                case True:
                    pass
                case False:
                    print(colors.RED + "\n[Error]"+ colors.RESET + ' could not be found in the source data. They must be exactly the same column names in the source data')
                    error = error + 1



    return error


#function 9
def hist_param_not_repeat(data, filter):
    error = 0
    filtered = filter_name(data, str(filter))
    for row in range(len(data)):
        joint_df = data[row].join(data, on=filtered, how = 'left')
        if len(joint_df) != 1:
            print('\nRow '+str(row + 1)+ ' in the historical parameter')
            error = error + 1
    if error > 0 :
        print('are duplicates')
    return error 

# function 10
def unit_scale(data, column):
    error = 0
    allowed_types = ["day", "week", "month", "year"]

    if (str(data[column][0]).lower() not in allowed_types):
        print(colors.RED + "\n[Error]"+ colors.RESET + 'payload_time_scale column must be one of Day, Week, Month or Year')
        error = 1
    return error 

# function 11 (if there are two target_ columns, then we only want to have one of them present and the other one to be absent.)
def target_columns_row_check(data):
    error = 0
    for i in range(len(data)):
        entered_count = data['target_count'][i]
        entered_tonnes = data['target_tonnes'][i]
        null_list = [entered_count, entered_tonnes]
        if null_list.count(None) != 1:
            error = error + 1
            print(colors.RED + "\n[Error]"+ colors.RESET + 'Row ' +str(i + 1)+' in the historical parameter data may have both target columns data or none of them. One of target_ column should be present ')
    return error

# funcrion 12
def hist_param_filter_exists_in_source(hist_param, source):
    error = 0 
    filter_column = filter_name(hist_param, 'filter_')
    for col in filter_column:
        source_data_col = col[7:]
        filter_added_col_in_source = str('filter_')+source_data_col
        source = source.rename({source_data_col:filter_added_col_in_source})
    hist_param = hist_param.drop(columns=['start_date','end_date','target_count','target_tonnes'])
    for row in range(len(hist_param)):
        source_to_check = source
        wild_card_cols = []
        non_wild_card = []
        hist_param_row =   hist_param[row]
        for col in filter_column:
            if hist_param_row[col][0] == '*':
                wild_card_cols.append(col)
            else: non_wild_card.append(col)
        
        # if there are wildcard columns, then we are not going to care about those columns. So we drop those columns from the source data to join/merge
        if len(wild_card_cols) > 0:
            source_to_check = source_to_check.select(pl.all().exclude(wild_card_cols))

        
        filtered_extracted_source = hist_param_row.join(source_to_check, on = non_wild_card, how='inner')
        if len(filtered_extracted_source) == 0:
            print(colors.RED + "\n[Error]"+ colors.RESET +str(row)+' in your historical parameter does not match in terms of the filtered_ clumns with any rows in your source data')
            error = error + 1
    return error

# function 13
def target_tonnage_row_has_source_with_tonne(hist_param, source):
    error = 0 
    filter_column = filter_name(hist_param, 'filter_')
    for col in filter_column:
        if col in source.columns:
            continue
        else:
            source_data_col = col[7:]
            filter_added_col_in_source = str('filter_')+source_data_col
            source = source.rename({source_data_col:filter_added_col_in_source})
    for row in range(len(hist_param)):
        hist_param_to_check = hist_param[row]
        if hist_param_to_check['target_tonnes'][0] != None: # if the target_tonnes is present in this row
            hist_param_to_check = hist_param_to_check.drop(columns=['start_date','end_date','target_count','target_tonnes'])
            # we would like to make sure that the rows in the source data with the filtered columns are matching and tonnage is present
            filtered_extracted_source = hist_param_to_check.join(source, on = filter_column, how='inner')
            filtered_extracted_source = filtered_extracted_source.drop_nulls(subset=['tonnes'])
            if len(filtered_extracted_source) == 0:
                print(colors.RED + "\n[Error]"+ colors.RESET +str(row)+' in your historical parameter has target tonnage, but in your source data with matched filtered columns does not have any tonnage record.')
                error = error + 1
            else: pass
    return error

# function 14
def dist_table_req(dist_table, pop_param):
    error = []
    for vehicle_type in pop_param['stem_vehicle_type']:
        distribution_name = vehicle_type + '_tonnes'
        extracted = dist_table.filter(pl.col('distribution') == distribution_name)
        if len(extracted) == 0:
            print(colors.RED + "\n[Error]"+ colors.RESET +str(distribution_name)+'_tonnes could not be found in the distribution table')
        elif (extracted['percent'].max() > 1.0) or (extracted['percent'].min() < 0):
            print(colors.RED + "\n[Error]"+ colors.RESET +str(distribution_name)+' percent must be bounded between 0 and 1.')
            error.append(1)
    return error






# schema for stem parameters, population parameter, historical parameter
population_schema = {
    "stem_vehicle_type": pl.String,
    "ratio": pl.Float64,
    "payload_distribution": pl.String
}

distribution_schema ={
    'distribution': pl.String,
    'value':  pl.Float64,
    'percent':  pl.Float64
}
stem_param_schema = {
    "mode": pl.String,
    "start_date": pl.String, #'%Y-%m-%d %H:%M',
    "end_date": pl.String, #'%Y-%m-%d %H:%M',
    "target_payload": pl.Float64,
    "payload_time_scale": pl.String,
    "warmup_days": pl.Int64,
    "cooldown_days": pl.Int64
}

historical_param_schema = {
    "start_date": pl.String, #'%Y-%m-%d %H:%M',
    "end_date": pl.String, #'%Y-%m-%d %H:%M',
    "target_tonnes" : pl.Int64,
    'target_count': pl.Int64
}

def population_mode():


    # population mode and we do the stem parameter check here 
    print('\nIt is in population mode.')
    # stem parameter datatype check
    # load the data
    stem_param = pl.read_csv(os.path.join(input_path, 'stem_parameters.csv')) 
    # Schema check function – it checks if the data types are correct according to the data schema. 
    # null_cols is the schema columns that are or missing and that are string. If you would like to ignore some columns to check from the data, you put the column's name here
    

    # check if the time_scale is one of the oprions
    time_scale_pass = unit_scale(stem_param, 'payload_time_scale')
    # printing out how many errors that were found in the csv
    print(f'{time_scale_pass} Error(s) in payload_time_scale column in stem parameter csv')
        
    # load the population parameter
    pop_param = pl.read_csv(os.path.join(input_path, 'population_parameters.csv'))
    distribution_table = pl.read_csv(os.path.join(input_path, 'distribution.csv'))
        
    # load the population csv and distribution tables
    null_cols = []
    pop_param_missing_col = schema_check(population_schema, pop_param,  null_cols)
    dist_input_missing_col = schema_check(distribution_schema, distribution_table, null_cols)

    # distribution table check
    dist_table_error = dist_table_req(distribution_table, pop_param)
    

    # see if they have the required columns
    if len(pop_param_missing_col) > 0:
        print(colors.RED + "\n[Error]"+ colors.RESET + f'population parameter missing columns or in the wrong data format [{" | ".join(pop_param_missing_col)}]')

    if len(dist_input_missing_col) > 0:
        print(colors.RED + "\n[Error]"+ colors.RESET + f'distribution table missing columns or in the wrong data format [{" | ".join(dist_input_missing_col)}]')


    if ((len(pop_param_missing_col) + len(dist_input_missing_col) + len(dist_table_error)) > 0):
        quit()
            
    # pop_param parameter datatype check
    error_count =   (                   # checking if vehicle_type column has unique values only
                                        only_unique_vals_in_col(pop_param, 'stem_vehicle_type') + 
                                        # Ration needs to add up to 100
                                        sum_to_hundred(pop_param, 'ratio')+
                                        # end date is greater than start date
                                        start_end_check(stem_param,  'start_date', 'end_date') +
                                        positive_val(stem_param, 'target_payload') + 
                                        positive_val(stem_param, 'warmup_days') +
                                        positive_val(stem_param, 'cooldown_days') 
                                        )
   
    # printing out how many errors that were found in the csv
    print(f'{error_count} Error(s) in population parameter csv')
    return error_count  


def historical_mode():
    print('\nIt is in historical mode')
    # load the phistorical parameter and source data
    hist_param = pl.read_csv(os.path.join(input_path, 'historical_parameters.csv'))            
    source = pl.read_csv(os.path.join(input_path, 'historical.csv'))

    null_cols = ['target_tonnes', 'target_count']
    hist_param_missing_col = schema_check(historical_param_schema, hist_param, null_cols)

    # one of count or tonne is present 
    one_of_target_columns_present =  target_columns_row_check(hist_param)
    if (len(hist_param_missing_col) + one_of_target_columns_present)> 0:
        quit()
        
            
    # checking if filter_ colums are in string
    filter_column = filter_name(hist_param, 'filter_')
    for col in filter_column:
        match hist_param[col].dtype:
            # string is good
            case pl.String:
                hist_param_csv_error = 0
                # if anything else, we flag
            case other:
                print(colors.RED + "\n[Error]"+ colors.RESET +str(col)+' is not in string. Filter columns must be in string')
                hist_param_csv_error = 1
    
    # check if the end date is ahead of the start date
    for row in range(len(hist_param)):
        hist_param_csv_error = hist_param_csv_error + start_end_check(hist_param[row], 'start_date', 'end_date')

            

    # Making sure none of the same filtered condiiton is repeating in the historical parameter data
    hist_param_csv_error = hist_param_csv_error + hist_param_not_repeat(hist_param, 'filter_')

    # print the number of errors  in the hsitorical parameter csv
    print(str(hist_param_csv_error), 'errors in historical paramater data')

    # source data check
    # if filtered columns exist in source data check
    source_csv_error = hist_param_filter_col_exists_in_source(source, filter_column, 'filter_') 
    source_csv_error = source_csv_error + hist_param_filter_exists_in_source(hist_param, source)
    source_csv_error = source_csv_error + target_tonnage_row_has_source_with_tonne(hist_param, source)
    source_csv_error = source_csv_error + hist_param_filter_col_exists_in_source(source, ['target_tonnes'], 'target_') 

    if (source_csv_error + hist_param_csv_error) > 0:
        print(colors.RED + "\n[Error]"+ colors.RESET + 'More than one of' +str(filter_column)+ ' could not be found from your source data or something wrong with the historical parameter data')
        quit()
    

    # printing out the number of errors in the source data
    print(str(source_csv_error), 'errors in historical source data')
    return source_csv_error

def stem_param_check_2():
    # load the data
    stem_param = pl.read_csv(os.path.join(input_path, 'stem_parameters.csv'))
    
    # the parameter table has one row
    if stem_param.shape[0] != 1:
        print(colors.RED + "\n[Error]"+ colors.RESET + 'stem parameter table is supposed to have 1 row in the table')
        quit()
    
    # check if the required column exists or not
    stem_param_missing_col = column_exists_or_not(stem_param, list(stem_param_schema.keys())[0:7])
    if len(stem_param_missing_col) > 0:
        print(colors.RED + "\n[Error]"+ colors.RESET +str(stem_param_missing_col)+' are missing from stem parameter csv.')
        quit()
    
    null_cols = []
    stem_parameter_data_val = schema_check(stem_param_schema, stem_param, null_cols) 
    if len(stem_parameter_data_val) > 0:
        print(colors.RED + "\n[Error]"+ colors.RESET + f'Some columns from stem parameter are missing or the data format is wrong [{" | ".join(stem_parameter_data_val)}]')
        
        quit()
    # population or historical?
    mode = stem_param[list(stem_param_schema.keys())[0]][0] 
    match mode.lower():
        case 'population':
            population_mode()


        case 'historical':
            historical_mode()

        case other:
            print(colors.RED + "\n[Error]"+ colors.RESET + ' mode must be either population or historical')
            quit()
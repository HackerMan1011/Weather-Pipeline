import os
import pandas as pd
import logging
import math
import sys


script_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(script_dir,'output')
input_csv='raw_forecast_data.csv'
output_csv='transformed_forecast_data.csv'

logger=logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter=logging.Formatter('%(asctime)s : [%(levelname)s] : %(name)s : %(message)s')
#Writing logs
file_handler=logging.FileHandler(f'{output_dir}/validate.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
stream_handler=logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.propagate=False

def main():
    logger.info('Validation started')

    try:
        data_input=pd.read_csv(os.path.join(output_dir,input_csv))
    except FileNotFoundError:
        logger.error(f'Input file {os.path.join(output_dir,input_csv)} not found')
        raise

    try:
        data_output=pd.read_csv(os.path.join(output_dir,output_csv))
    except FileNotFoundError:
        logger.error(f'Output file {os.path.join(output_dir,output_csv)} not found')
        raise


    errors=[]

    # Check if input records is equal to output records
    logger.info('Validating Input and Output Records')
    if len(data_input) != len(data_output):
        errors.append(f'Input record count ({len(data_input)}) does not match with transformed data count ({len(data_output)})')


    #Check if output data matches the total count
    logger.info('Validating Output records with total count')
    if len(data_output) != (24*14):
        errors.append(f'Transformed record count ({len(data_output)}) does not match with expected data count ({24*14})')

    # Check if any of the row contains null
    logger.info('Checking for NULL values')
    null_counts=data_output.isnull().sum()
    coll_at_null=null_counts[null_counts > 0]
    if not coll_at_null.empty:
        errors.append(f'Nulls found : {coll_at_null.to_dict()}')

    #Check if any column has out of nominal range values
    logger.info('Checking if the data has valid values')
    RANGES = {
        'temperature_2m' : (-50,50),
        'apparent_temperature' : (-50,50),
        'precipitation_probability' : (0,100),
        'rain': (0, math.inf),
        'showers': (0, math.inf)
    }
    for col, (low,high) in RANGES.items():
        bad_data = data_output[~data_output[col].between(low,high)]
        if not bad_data.empty:
            errors.append(f'{col} out of range {(low,high)}: {len(bad_data)} rows. First 5 error-ridden records : \n{bad_data.head(5).to_string()}')


    #Check if duplicate rows are present
    logger.info('Checking for duplicate records')
    data_dup=data_output.duplicated(subset=['time','fetched_at']).sum()
    if data_dup > 0:
        errors.append(f'{data_dup} duplicate records found.')

    #Check if the fetched_at is unique
    logger.info('Checking run history')
    fetch_count=data_output['fetched_at'].nunique()
    if fetch_count != 1:
        errors.append(f'Data fetch timestamp is not uniform : {fetch_count} run IDs found')

    if len(errors) == 0:
        logger.info ('All Validations passed. Data is ready for loading.')
    else:
        for item in errors:
            logger.error(item)
        raise ValueError(errors)

if __name__=='__main__':
    main()
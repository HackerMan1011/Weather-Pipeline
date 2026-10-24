import os, sys
import pandas as pd
import logging


script_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(script_dir,"output")
input_csv="raw_forecast_data.csv"
output_csv="transformed_forecast_data.csv"

logger=logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter=logging.Formatter('%(asctime)s : [%(levelname)s] : %(name)s : %(message)s')
#Writing logs
file_handler=logging.FileHandler(f'{output_dir}/transform.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
stream_handler=logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.propagate=False

def main():
    logger.info("Starting Transformation")
    try:
        data_raw=pd.read_csv(os.path.join(output_dir,input_csv))
    except FileNotFoundError:
        logger.exception(f"Input file does not exists at {output_dir}")
        raise
    else:
        logger.info(f"File read for transformation : Input records - {len(data_raw)}")


    try:
        data_raw["time"]=pd.to_datetime(data_raw["time"]).dt.tz_localize("Asia/Kolkata").dt.tz_convert("UTC")
    except KeyError:
        logger.error("Column 'time' does not exist.")
        raise
    else:
        logger.info("Time column data type converted into UTC timezone")

    try:
        data_raw["fetched_at"]=pd.to_datetime(data_raw["fetched_at"],utc=True)
    except KeyError:
        logger.error("Column 'fetched_at' does not exist.")
        raise
    else:
        logger.info("Fetched_at column data type converted into UTC timezone")

    try:
        data_raw.to_csv(path_or_buf=os.path.join(output_dir,output_csv),index=False)
    except (OSError,ValueError):
        logger.error("Transformed data cannot be saved")
        raise
    else:
        logger.info(f'Transformed data. Total Records - {len(data_raw)} ,  File name : {os.path.join(output_dir,output_csv)}')

if __name__=='__main__':
    main()
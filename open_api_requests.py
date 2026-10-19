import requests
import logging
import json
import os
import pandas as pd
import datetime as dt
from time import sleep

api_url = 'https://api.open-meteo.com/v1/forecast?latitude=12.968&longitude=80.1502&hourly=temperature_2m,apparent_temperature,precipitation_probability,rain,showers&timezone=auto&forecast_days=14'
script_dir = os.path.dirname(os.path.abspath(__file__))
output_dir=os.path.join(script_dir,"output")
input_json="data.json"
output_file="raw_forecast_data.csv"
os.makedirs(output_dir,exist_ok=True)

logger=logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter=logging.Formatter('%(asctime)s : [%(levelname)s] : %(name)s : %(message)s')
#Writing logs
file_handler=logging.FileHandler(f'{output_dir}/api_test.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)



#TODO 1: Get data from API and store it as a JSON
MAX_ATTEMPTS = 5
wait = 5
logger.debug("Fetching data from Open-Meteo API")
for attempt in range(1, MAX_ATTEMPTS + 1):
    try:
        response = requests.get(api_url, timeout=15)
        response.raise_for_status()
        run_time = dt.datetime.now(dt.timezone.utc)
        logger.info(f"API call made at : {run_time}")
        logger.debug(f"HTTP {response.status_code}")
        break
    except (requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError) as e:
        if isinstance(e, requests.exceptions.HTTPError):
            retry_flag = e.response.status_code >= 500
        else:
            retry_flag = True

        if not retry_flag or attempt == MAX_ATTEMPTS:
            logger.exception(f"API call failed, giving up (attempt {attempt}): {e}")
            raise
        logger.warning(f"Attempt {attempt} failed: {e}. Retrying in {wait}s")
        sleep(wait)
        wait *= 2


data = response.json()
try:
    with open(f"{output_dir}/{input_json}","w") as f:
        json.dump(data,f,indent=2)
    logger.info(f"API data stored as JSON. File located at : {output_dir}, File Name - {input_json}")
except OSError as e:
    logger.error(f"Unable to write the file : {e}")
    raise


# TODO 2 : Clean data and keep only hourly data into a CSV file

try:
    with open(f"{output_dir}/{input_json}","r") as f:
        data=json.load(f)
except json.JSONDecodeError as e:
    logger.exception(f"Invalid JSON: {e}")
    raise
except FileNotFoundError:
    logger.exception(f"File does not exists at {output_dir}")
    raise

logger.debug('Cleaning JSON into useful Raw Data')

if "hourly" not in data:
    logger.error("Invalid Column name")
    raise KeyError("'hourly' not found in API response")
   

output_path = f"{output_dir}/{output_file}"
try:
    hourly_fc=pd.DataFrame(data["hourly"])
    hourly_fc["fetched_at"]=run_time
    logger.info("API call time updated in CSV file for version history")
    hourly_fc.to_csv(path_or_buf=output_path, index=False)
    logger.info(f'Cleaned data. Total Records - {len(hourly_fc.index)} ,  File located at: {output_dir}, File Name - {output_file}')
except (OSError,ValueError):
    logger.exception("Failed to write records into CSV file")
    raise



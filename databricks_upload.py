import os, requests
import logging, sys
from time import sleep
import datetime as dt

script_dir=os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(script_dir,'output')
output_file='transformed_forecast_data.csv'
os.makedirs(output_dir,exist_ok=True)

#Log setup
logger=logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter=logging.Formatter('%(asctime)s : [%(levelname)s] : %(name)s : %(message)s')
file_handler=logging.FileHandler(f'{output_dir}/databricks_upload.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
stream_handler=logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.propagate=False



def main():
    host=os.environ["DATABRICKS_URL"].rstrip("/")
    token=os.environ["DATABRICKS_TOKEN"]
    MAX_ATTEMPTS = 5
    wait = 5
    try:
        with open(os.path.join(output_dir,output_file),'rb') as f:
            passed_data=f.read()
            logger.info("Readying CSV file for loading into Databricks")
            for attempt in range(1,MAX_ATTEMPTS+1):
                try:
                    response =requests.put(url=host+f'/api/2.0/fs/files/Volumes/workspace/default/raw_files/{output_file}', 
                                    headers={"Authorization" : "Bearer "+token},
                                    params={"overwrite": "true"},
                                    data=passed_data,
                                    timeout=30)
                    response.raise_for_status()
                    logger.debug(f"HTTP {response.status_code}")
                    logger.info(f"File uploaded to DataBricks successfully at {dt.datetime.now(dt.timezone.utc)} UTC")
                    break
                except (requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout,
                        requests.exceptions.HTTPError ) as e:
                    if isinstance(e, requests.exceptions.HTTPError):
                        retry_flag = (e.response.status_code==429)  or  (e.response.status_code>= 500)
                    else:
                        retry_flag = True
                    
                    if not retry_flag or attempt == MAX_ATTEMPTS:
                        logger.exception(f"API call failed, giving up (attempt {attempt}): {e}")
                        raise
                    logger.warning(f"Attempt {attempt} failed: {e}. Retrying in {wait}s")
                    sleep(wait)
                    wait *= 2
    except FileNotFoundError:
        logger.exception(f"CSV File does not exist at {output_dir}")
        raise


if __name__=="__main__":
    main()
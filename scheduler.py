import logging
import schedule
import os, time
import subprocess, sys

script_dir=os.path.dirname(os.path.abspath(__file__))
output_dir=os.path.join(script_dir,'output')
os.makedirs(output_dir,exist_ok=True)

#Logging setup
logger=logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter=logging.Formatter('%(asctime)s : [%(levelname)s] : %(name)s : %(message)s')
file_handler=logging.FileHandler(f"{output_dir}/scheduler.log")
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

def run_pipeline():
    PIPELINE = {"Extract":"open_api_requests.py", 
                "Transformation":"transform.py",
                "Validation":"validate.py",
                "Data Load":"data_load.py"}
    
    for step,script in PIPELINE.items():
        logger.info(f"Starting {step}")
        rc=subprocess.run([sys.executable,os.path.join(script_dir,script)])
        if rc.returncode==0:
            logger.info(f"{step} completed successfully.")
        else:
            logger.error(f"{step} failed with exit code {rc.returncode}")
            return
    logger.info("ETL Pipeline completed end-to-end successfully")

if __name__=='__main__':
    interval_set=int(os.environ.get("SCHEDULE_INTERVAL_MINUTES",5))
    schedule.every(interval_set).minutes.do(run_pipeline)
    logger.info(f"Scheduler started. Running every {interval_set} minutes")
    run_pipeline()
    while True:
        schedule.run_pending()
        time.sleep(1)

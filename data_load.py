from sqlalchemy import create_engine, text
import pandas as pd
import os, sys
import logging
import sqlalchemy.exc

script_dir = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(script_dir,'output')

#Log setup
logger=logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter=logging.Formatter('%(asctime)s : [%(levelname)s] : %(name)s : %(message)s')
file_handler=logging.FileHandler(f'{output_dir}/data_load.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
stream_handler=logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.propagate=False

def main():
    logger.info("Data load started.")
    # import psycopg2
    user = os.environ["POSTGRES_USER"] # get is not used as it prevents null or None return
    password= os.environ["POSTGRES_PASSWORD"]
    database = os.environ["POSTGRES_DB"]
    hostname= os.environ.get("POSTGRES_HOST","localhost")
    port_number=os.environ.get("POSTGRES_PORT",5433)
    my_engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{hostname}:{port_number}/{database}")

    try:
        df=pd.read_csv(f"{output_dir}/transformed_forecast_data.csv")
    except FileNotFoundError:
        logger.error(f"File does not exists at {output_dir}")
        raise
    else:
        logger.info(f"Reading CSV file for load. Record count : {len(df)}")
        rows_to_be_inserted=df.to_dict('records')


    try:
        with my_engine.begin() as conn:
            conn.execute(text("""create table if not exists weather_hourly_raw (
                                time timestamptz not null,
                                temperature_2m float not null,
                                apparent_temperature float not null,
                                precipitation_probability int not null,
                                rain float not null,
                                showers float not null,
                                fetched_at timestamptz not null,
                                primary key (time,fetched_at)
                        );""")) 
            logger.info('Database connection established.')
            result=conn.execute(text(""" INSERT INTO weather_hourly_raw (time,temperature_2m,apparent_temperature,precipitation_probability, rain, showers, fetched_at)
                                values (:time,:temperature_2m,:apparent_temperature,:precipitation_probability,:rain,:showers,:fetched_at) ON CONFLICT (time,fetched_at) DO NOTHING;"""),rows_to_be_inserted)
            logger.info(f'Skipping {len(df)-result.rowcount} records. Only {result.rowcount} new records inserted.')
    except sqlalchemy.exc.SQLAlchemyError as e:
        logger.exception(f"DB step failed. Error : {e}")
        raise

if __name__=='__main__':
    main()
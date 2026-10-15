from sqlalchemy import create_engine
import pandas as pd
import os
# import psycopg2
user = os.environ["POSTGRES_USER"] # get is not used as it prevents null or None return
password= os.environ["POSTGRES_PASSWORD"]
database = os.environ["POSTGRES_DB"]
my_engine = create_engine(f"postgresql+psycopg2://{user}:{password}@postgres:5432/{database}")

cur_dir=os.path.join(os.path.dirname(os.path.abspath(__file__)),"output")
df=pd.read_csv(f"{cur_dir}/raw_forecast_data.csv")
df.to_sql(name='weather_hourly',con=my_engine,if_exists='replace',index=False)

result = pd.read_sql("SELECT COUNT(*) FROM weather_hourly", con=my_engine)
print(result)
result = pd.read_sql("SELECT * FROM weather_hourly LIMIT 5", con=my_engine)
print(result)

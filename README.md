# Weather Pipeline

A small batch pipeline that pulls a 14-day hourly weather forecast from the [Open-Meteo API](https://open-meteo.com/) and loads it into a Postgres database for analysis. Everything runs in Docker with a single command.

## How it works

```
Open-Meteo API  ->  data.json  ->  raw_forecast_data.csv  ->  Postgres (weather_hourly)
   [weather]                                                      [loader]      [postgres]
```

Docker Compose runs three services:

| Service | What it does |
|---|---|
| `weather` | Runs `open_api_requests.py`: calls the API, saves the response as JSON, then keeps only the hourly data and writes it to a CSV. |
| `postgres` | A Postgres 16 container that stores the data. A health check tells Compose when it is ready. |
| `loader` | Runs `data_load.py`: reads the CSV and loads it into the `weather_hourly` table with pandas and SQLAlchemy, then prints a small sample of the loaded rows as a check. |

`loader` starts only after `weather` has finished successfully and `postgres` is healthy.

The output (`data.json`, `raw_forecast_data.csv` and `api_test.log`) is written to the local `output/` folder through a Docker volume.

## Data

Hourly forecasts for one location (set in the API URL in `open_api_requests.py`), with these columns: `time`, `temperature_2m`, `apparent_temperature`, `precipitation_probability`, `rain`, `showers`.

## Run it

You need Docker and Docker Compose.

1. Create your settings file from the template and fill in the values:
   ```
   cp .env.example .env
   ```
2. Build and start everything:
   ```
   docker compose up --build
   ```
   `--build` is needed the first time (and whenever the scripts or `requirements.txt` change), because the scripts are baked into the image.
3. When `loader` prints the sample rows and exits with code 0, the load is done.

To look at the data yourself, connect any Postgres client (pgAdmin, psql) to `localhost`, port `5433`, using the database name, user and password from your `.env`.

To stop everything: `docker compose down`. Add `-v` to also delete the database volume.

## Project layout

```
open_api_requests.py   fetch the API data, write JSON and CSV
data_load.py           load the CSV into Postgres
Dockerfile             image used by weather and loader
docker-compose.yml     the three services
requirements.txt       pinned Python dependencies
.env.example           template for the database settings
```

## Limitations and next steps

- The load replaces the whole `weather_hourly` table on every run, so each run starts clean but history is not kept. An upsert is the planned fix.
- The `time` column is stored as text, not a timestamp. It should be converted in a transform step before loading.
- The location and forecast length are fixed in the script.
- There is no scheduler yet; the pipeline runs when you start it.

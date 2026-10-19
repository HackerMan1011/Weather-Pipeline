# Weather Pipeline

Weather forecasts change from one run to the next. This pipeline pulls a 14-day hourly forecast for one location from the [Open-Meteo API](https://open-meteo.com/) every 6 hours and keeps every run in Postgres, so you can see how the forecast for the same hour changed over time. It is a batch ETL pipeline: four separate Python stages run in order under a scheduler, and everything starts with one command, `docker compose up`.

## Architecture

![Architecture](docs/architecture.png)

## Pipeline stages

1. **Extract** (`open_api_requests.py`): calls the API with `requests`, saves the JSON response and writes the hourly data to a CSV. It adds a `fetched_at` column, stamped in UTC right after the API call returns, which identifies the run.
2. **Transform** (`transform.py`): converts `time` from text to a timestamp, from IST (Asia/Kolkata) to UTC, and `fetched_at` to a UTC timestamp.
3. **Validate** (`validate.py`): checks the transformed data: row counts (input equals output, 336 rows = 24 hours x 14 days), no nulls, values in plausible ranges, no duplicate (`time`, `fetched_at`) pairs, and a single `fetched_at` per run. Any failure exits with a non-zero code.
4. **Load** (`data_load.py`): creates the `weather_hourly_raw` table if needed and inserts the rows, skipping any that already exist (`ON CONFLICT DO NOTHING`).

Around the stages:

- **Scheduler** (`scheduler.py`): runs the four stages in order using the `schedule` library, once at startup and then on an interval set by `SCHEDULE_INTERVAL_MINUTES` (360 = 6 hours). Each stage's exit code decides whether the next one runs.
- **Postgres**: a Postgres 16 container with a health check; the data lives in a named Docker volume.

## Design decisions

- **Append-only history.** Every run adds 336 rows. The primary key is (`time`, `fetched_at`), so the same forecast hour can appear once per run.
- **Idempotent load.** Loading the same run twice inserts nothing (conflicts are skipped); a new run adds new rows.
- **Atomic load.** The table creation and insert run in one transaction, so a failed load leaves all rows or none.
- **Failure handling.** A failed stage is logged with its name and exit code, later stages do not run, and nothing is loaded. The scheduler stays alive and tries again at the next interval.
- **Logging.** Each stage writes its own log file to `output/`.

## Tech stack

Python 3.11, `requests` 2.34.2, `pandas` 3.0.5, SQLAlchemy 2.1.3 with `psycopg2-binary` 2.9.13, `schedule` 1.2.2, Postgres 16, Docker Compose.

## Data

Hourly forecasts for one location (set in the API URL in `open_api_requests.py`), stored in the `weather_hourly_raw` table with these columns: `time`, `temperature_2m`, `apparent_temperature`, `precipitation_probability`, `rain`, `showers` and `fetched_at`.

## Run it

You need Docker and Docker Compose.

1. Create your settings file from the template and fill in the values:
   ```
   cp .env.example .env
   ```
   It holds the database credentials, `SCHEDULE_INTERVAL_MINUTES`, and `POSTGRES_HOST` / `POSTGRES_PORT` for running the scripts outside Docker (inside Compose, the host and port are set for you).
2. Build and start everything:
   ```
   docker compose up --build -d
   ```
   The first pipeline run starts immediately. Follow it in `output/scheduler.log`.
3. Look at the data: connect any Postgres client (pgAdmin, psql) to `localhost`, port `5433`, with the database name, user and password from your `.env`, and run:
   ```sql
   SELECT fetched_at, COUNT(*) FROM weather_hourly_raw GROUP BY 1 ORDER BY 1 DESC;
   ```
   Each run shows up as one `fetched_at` value with 336 rows.

To stop everything: `docker compose down`. Add `-v` to also delete the database volume (and with it, the whole run history).

## Project layout

```
open_api_requests.py   extract: fetch the API data, write JSON and CSV
transform.py           transform: fix timestamps and time zones
validate.py            validate: data quality checks
data_load.py           load: append the rows into Postgres
scheduler.py           run the four stages in order, on a schedule
Dockerfile             image used by the pipeline service
docker-compose.yml     the postgres and pipeline services
requirements.txt       pinned Python dependencies
.env.example           template for the settings
docs/architecture.png  architecture diagram
```

## Limitations

- No resume from the failed stage: the next run starts again from Extract.
- The table grows without limit (336 rows per run, about 1,344 a day at 6-hour intervals) and there is no retention policy.
- The location and forecast length are fixed in the script.

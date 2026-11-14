import pendulum, os
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import timedelta

def run_extract():
    import open_api_requests
    open_api_requests.main()

def run_transform():
    import transform
    transform.main()

def run_validate():
    import validate
    validate.main()

def run_load():
    import data_load
    data_load.main()

def run_databricks_load():
    import databricks_upload
    databricks_upload.main()

with DAG(
    dag_id='weather_pipeline_dag',
    schedule='0 */6 * * *',
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    start_date=pendulum.datetime(2026,10,24,tz='UTC'),
    tags=["project1"]
) as dag:
    extraction = PythonOperator(task_id='extract', python_callable=run_extract,retries=1, retry_delay=timedelta(minutes=5))
    transformation = PythonOperator(task_id='transform', python_callable=run_transform, retries=0)
    validation = PythonOperator(task_id='validate', python_callable=run_validate, retries=0)
    loading = PythonOperator(task_id='load', python_callable=run_load,retries=2, retry_delay=timedelta(minutes=2))
    databricks_uploading = PythonOperator(task_id='databricks_upload', python_callable=run_databricks_load,retries=1, retry_delay=timedelta(minutes=1))
    run_notebook = DatabricksSubmitRunOperator(
            task_id="run_notebook",
            databricks_conn_id="databricks_default",
            tasks=[ { "task_key": "weather_load",
                      "notebook_task": { "notebook_path": f"/Workspace/Users/{os.environ['DATABRICKS_USERNAME']}/weather_load" } } ],
            retries=1,
            retry_delay=timedelta(minutes=5)
        )
    extraction >> transformation >> validation >> loading
    validation >> databricks_uploading >> run_notebook
from airflow import DAG
from airflow.decorators import dag
from airflow.utils.dates import timedelta
import pendulum

from dags.etl_weather.tasks import (
    extract_weather_data,
    treatment_wheater_data,
    df_to_db
)


args = {
    "owner": "lucca.ribeiro",
    "start_date": pendulum.today("UTC").add(days=-7, minutes=-60),
    "max_active_runs": 1,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "dagrun_timeout": timedelta(minutes=300),
}


@dag(
    dag_id="etl_weather",
    default_args=args,
    schedule="30 3 * * *",
    tags=["ETL"],
)
def taskflow():
    api_weather_data = extract_weather_data()
    treatment_data = treatment_wheater_data()
    pandas_to_database = df_to_db(treatment_data)

    (api_weather_data >> treatment_data >> pandas_to_database)


dag: DAG = taskflow()

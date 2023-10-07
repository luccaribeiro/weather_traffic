from airflow.decorators import task
from airflow.models import Variable
import requests
from datetime import datetime
from airflow.exceptions import AirflowFailException
import pandas as pd
from sqlalchemy import create_engine
from dags.commons.utils import save_raw_data, read_data, normalize_dict_columns


today = datetime.today().strftime("%Y-%m-%d")


@task
def extract_weather_data():
    API_KEY = Variable.get("openweathermap_api_key", None)
    cities = ["sao paulo", "rio de janeiro"]
    weather_data = []

    for city in cities:
        url = f"https://api.openweathermap.org/data/2.5/forecast?q={city}&appid={API_KEY}&units=metric"
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            data["list"] = [{"city": data["city"], **item} for item in data["list"]]
            weather_data.extend(data["list"])
        else:
            print(f"Não foi possível obter dados para {city}")

    if len(weather_data) > 0:
        save_raw_data(weather_data, today, "weather")
    else:
        raise AirflowFailException("No Data")


@task
def treatment_wheater_data():
    data = read_data(f"raw_weather_data-{today}.json")

    if data is None:
        return None

    df = pd.DataFrame(data)

    # Lista das colunas de dicionário que desejamos normalizar
    dict_columns = ["main", "weather", "wind", "city", "weather_0"]

    # Aplicar normalização para todas as colunas da lista
    df = normalize_dict_columns(df, dict_columns, prefix=True)

    # Mapeamento de nomes de colunas
    name_columns = {
        "dt": "timestamp",
        "clouds": "cloud_coverage",
        "visibility": "visibility",
        "pop": "probability_of_precipitation",
        "rain": "rain_info",
        "sys": "time_of_day",
        "dt_txt": "date",
        "main_temp": "temperature",
        "main_feels_like": "feels_like_temperature",
        "main_temp_min": "min_temperature",
        "main_temp_max": "max_temperature",
        "main_pressure": "pressure",
        "main_sea_level": "sea_level_pressure",
        "main_grnd_level": "ground_level_pressure",
        "main_humidity": "humidity",
        "main_temp_kf": "temperature_kf",
        "weather_0_id": "weather_id",
        "weather_0_main": "weather_main",
        "weather_0_description": "weather_description",
        "weather_0_icon": "weather_icon",
        "wind_speed": "wind_speed",
        "wind_deg": "wind_direction_deg",
        "wind_gust": "wind_gust_speed",
        "city_id": "city_id",
        "city_name": "city_name",
        "city_country": "city_country",
        "city_population": "city_population",
        "city_timezone": "city_timezone",
        "city_sunrise": "city_sunrise",
        "city_sunset": "city_sunset",
        "city_coord.lat": "city_latitude",
        "city_coord.lon": "city_longitude",
    }

    # Renomeie as colunas com base no mapeamento
    df = df.rename(columns=name_columns)

    # Remover linhas duplicadas, já trata valores NAN
    df.drop_duplicates(subset=["timestamp", "city_id"], keep="first", inplace=True)

    # Padronizacao de formatos
    df["rain_info"] = df["rain_info"].apply(
        lambda x: x.get("3h") if isinstance(x, dict) and "3h" in x else 0
    )
    df["cloud_coverage"] = df["cloud_coverage"].apply(
        lambda x: x.get("all") if isinstance(x, dict) and "all" in x else 0
    )
    df["time_of_day"] = df["time_of_day"].apply(
        lambda x: x.get("pod") if isinstance(x, dict) and "pod" in x else ""
    )
    df["time_of_day"] = df["time_of_day"].replace({"n": "night", "d": "day"})

    return df


@task
def df_to_db(df):
    engine = create_engine("sqlite:///zebrinha_azul.db", echo=True)
    df.to_sql("weather", engine, if_exists="replace", index=True)

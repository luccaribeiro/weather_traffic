from airflow.decorators import task
from airflow.models import Variable
import requests
from datetime import datetime
import json
from airflow.exceptions import AirflowFailException
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

today = datetime.today().strftime("%Y-%m-%d")


def save_raw_data(raw_data):
    raw_data_json = json.dumps(raw_data)
    file_name = f"raw_weather_data-{today}.json"

    with open(file_name, "w") as arquivo:
        arquivo.write(raw_data_json)

    # Coloquei para salvar localmente pois não queria que dependesse de uma conta AWS
    # Porém normalmente esse arquivo estaria em um bucket no s3


def read_weather_data(file_path):
    # Aqui seria o codigo de leitura no S3 ao inves de ler localmente
    try:
        with open(file_path, "r") as raw_data:
            data = json.load(raw_data)
        return data
    except FileNotFoundError:
        print(f"O arquivo '{file_path}' não foi encontrado.")
        return None
    except json.JSONDecodeError:
        print(f"Erro ao decodificar o arquivo JSON em '{file_path}'. Verifique a formatação.")
        return None
    
def normalize_dict_columns(df, dict_columns, prefix=False):
    for col_name in dict_columns:
        # Extrai o dicionário
        dict_data = df.pop(col_name)

        # Normaliza o dicionário
        df_join = pd.json_normalize(dict_data)

        # Adiciona prefixo se necessário
        if prefix:
            df_join = df_join.add_prefix(f"{col_name}_")

        # Junta o DataFrame original com o DataFrame normalizado
        df = pd.concat([df, df_join], axis=1)

    return df


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
        save_raw_data(weather_data)
    else:
        raise AirflowFailException("No Data")


@task
def treatment_wheater_data():
    data = read_weather_data(f"raw_weather_data-{today}.json")

    if data is None:
        return None

    df = pd.DataFrame(data)

    # Lista das colunas de dicionário que desejamos normalizar
    dict_columns = ["main", "weather", "wind", "city", "weather_0"]

    # Aplicar normalização para todas as colunas da lista
    df = normalize_dict_columns(df, dict_columns, prefix=True)

    # Mapeamento de nomes de colunas
    name_columns = {
        'dt': 'timestamp',
        'clouds': 'cloud_coverage',
        'visibility': 'visibility',
        'pop': 'probability_of_precipitation',
        'rain': 'rain_info',
        'sys': 'time_of_day',
        'dt_txt': 'date',
        'main_temp': 'temperature',
        'main_feels_like': 'feels_like_temperature',
        'main_temp_min': 'min_temperature',
        'main_temp_max': 'max_temperature',
        'main_pressure': 'pressure',
        'main_sea_level': 'sea_level_pressure',
        'main_grnd_level': 'ground_level_pressure',
        'main_humidity': 'humidity',
        'main_temp_kf': 'temperature_kf',
        'weather_0_id':"weather_id",
        'weather_0_main':"weather_main",
        'weather_0_description':"weather_description",
        'weather_0_icon':"weather_icon",
        'wind_speed': 'wind_speed',
        'wind_deg': 'wind_direction_deg',
        'wind_gust': 'wind_gust_speed',
        'city_id': 'city_id',
        'city_name': 'city_name',
        'city_country': 'city_country',
        'city_population': 'city_population',
        'city_timezone': 'city_timezone',
        'city_sunrise': 'city_sunrise',
        'city_sunset': 'city_sunset',
        'city_coord.lat': 'city_latitude',
        'city_coord.lon': 'city_longitude'
    }

    # Renomeie as colunas com base no mapeamento
    df = df.rename(columns=name_columns)


    # Remover linhas duplicadas
    df.drop_duplicates(subset=["timestamp", "city_id"], keep="first", inplace=True)

    # Padronizacao de formatos
    df["rain_info"] = df["rain_info"].apply(
        lambda x: x.get("3h") if isinstance(x, dict) and "3h" in x else 0
    )  # Já trata valores NAN
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
    engine = create_engine('sqlite:///zebrinha_azul.db', echo=True)
    df.to_sql('weather', engine, if_exists='replace', index=True)
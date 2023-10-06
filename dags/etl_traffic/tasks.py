from airflow.decorators import task
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
import requests
from dags.commons.utils import (
    save_raw_data,
    read_weather_data,
    normalize_dict_columns
    )
from datetime import datetime
from sqlalchemy import create_engine, types
import pandas as pd


today = datetime.today().strftime("%Y-%m-%d")


@task
def extract_traffic_data():
    API_KEY = Variable.get("google_directions_api_key", None)
    origin_destiny = {
       "Sao Paulo": "Rio de janeiro",
       "Rio de janeiro": "Sao Paulo"
    }
    extra_params = "" # para cada parametro extra que voce for definir tem que colocar o & antes do parametro
    traffic_data = []
    for origin, destiny in origin_destiny.items():
        url = f"https://maps.googleapis.com/maps/api/directions/json?origin={origin.replace(' ', '+')}&destination={destiny.replace(' ', '+')}&avoid=tolls|highways|ferries{extra_params}&key={API_KEY}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            data["routes"][0]['origin'] = origin
            data["routes"][0]['destiny'] = destiny
            traffic_data.extend(data["routes"])

        else:
            print(f"Não foi possível obter dados para o trajeto {origin} - {destiny}")

    if len(traffic_data) > 0:
        save_raw_data(traffic_data, today, 'traffic')
    else:
        raise AirflowFailException("No Data")
    

@task
def treatment_traffic_data():
    data = read_weather_data(f"raw_traffic_data-{today}.json")
    if data is None:
        return None

    df = pd.DataFrame(data)

    # Lista das colunas de dicionário que desejamos normalizar
    dict_columns = ['bounds', 'legs', 'legs_0']

    # Aplicar normalização para todas as colunas da lista
    df = normalize_dict_columns(df, dict_columns, prefix=True)

    #  Retirando coisas desnecessarias 
    df = df.dropna()
    df = df.drop('copyrights', axis=1)

    # Criando colunas
    df['date'] = today

    name_columns = {
    'overview_polyline': 'polyline_overview',
    'summary': 'route_summary',
    'warnings': 'warnings',
    'waypoint_order': 'waypoint_order',
    'bounds_northeast.lat': 'northeast_bounds_latitude',
    'bounds_northeast.lng': 'northeast_bounds_longitude',
    'bounds_southwest.lat': 'southwest_bounds_latitude',
    'bounds_southwest.lng': 'southwest_bounds_longitude',
    'legs_0_end_address': 'end_address',
    'legs_0_start_address': 'start_address',
    'legs_0_steps': 'route_steps',
    'legs_0_traffic_speed_entry': 'traffic_speed_entry',
    'legs_0_via_waypoint': 'via_waypoints',
    'legs_0_distance.text': 'distance_text',
    'legs_0_distance.value': 'distance_value',
    'legs_0_duration.text': 'duration_text',
    'legs_0_duration.value': 'duration_value',
    'legs_0_end_location.lat': 'end_location_latitude',
    'legs_0_end_location.lng': 'end_location_longitude',
    'legs_0_start_location.lat': 'start_location_latitude',
    'legs_0_start_location.lng': 'start_location_longitude',
    'origin': 'origin',
    'destiny': 'destiny',
    'date': 'date'
    }

    # Renomeie as colunas com base no mapeamento
    df = df.rename(columns=name_columns)
    
    return df

@task
def df_to_db(df):
    # Acessa o banco zebrinha_azul
    engine = create_engine('sqlite:///zebrinha_azul.db', echo=True)

    # Salvar o DataFrame no banco de dados com os tipos de dados JSON definidos
    df.to_sql('traffic', engine, if_exists='replace', index=True, dtype={"polyline_overview": types.JSON})

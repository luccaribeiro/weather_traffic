from airflow import DAG
from airflow.decorators import task
from sqlalchemy.orm import declarative_base
from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    DateTime,
    create_engine,
    JSON,
    ForeignKey,
)
from airflow import DAG
from airflow.decorators import dag
from airflow.utils.dates import timedelta
import pendulum


@task
def create_database():
    engine = create_engine("sqlite:///zebrinha_azul.db", echo=True)
    Base = declarative_base()

    class Weather(Base):
        __tablename__ = "weather"
        id = Column(Integer, primary_key=True, autoincrement=True)
        timestamp = Column(DateTime)
        cloud_coverage = Column(Integer)
        visibility = Column(Integer)
        probability_of_precipitation = Column(Float)
        rain_info = Column(Float)
        time_of_day = Column(String)
        date = Column(String)
        temperature = Column(Float)
        feels_like_temperature = Column(Float)
        min_temperature = Column(Float)
        max_temperature = Column(Float)
        pressure = Column(Integer)
        sea_level_pressure = Column(Integer)
        ground_level_pressure = Column(Integer)
        humidity = Column(Integer)
        temperature_kf = Column(Float)
        wind_speed = Column(Float)
        wind_direction_deg = Column(Integer)
        wind_gust_speed = Column(Float)
        city_id = Column(Integer)
        city_name = Column(String)
        city_country = Column(String)
        city_population = Column(Integer)
        city_timezone = Column(Integer)
        city_sunrise = Column(Integer)
        city_sunset = Column(Integer)
        city_latitude = Column(Float)
        city_longitude = Column(Float)
        weather_id = Column(Integer)
        weather_main = Column(String)
        weather_description = Column(String)
        weather_icon = Column(String)

    class Traffic(Base):
        __tablename__ = "traffic"
        id = Column(Integer, primary_key=True, autoincrement=True)
        polyline_overview = Column(JSON)
        route_summary = Column(String)
        warnings = Column(JSON)
        waypoint_order = Column(JSON)
        origin = Column(String, ForeignKey("weather.city_name"))
        destiny = Column(String, ForeignKey("weather.city_name"))
        northeast_bounds_latitude = Column(Float)
        northeast_bounds_longitude = Column(Float)
        southwest_bounds_latitude = Column(Float)
        southwest_bounds_longitude = Column(Float)
        end_address = Column(String)
        start_address = Column(String)
        route_steps = Column(JSON)
        traffic_speed_entry = Column(JSON)
        via_waypoints = Column(JSON)
        distance_text = Column(String)
        distance_value = Column(Integer)
        duration_text = Column(String)
        duration_value = Column(Integer)
        end_location_latitude = Column(Float)
        end_location_longitude = Column(Float)
        start_location_latitude = Column(Float)
        start_location_longitude = Column(Float)
        date = Column(DateTime)

    Base.metadata.create_all(engine)


args = {
    "owner": "lucca.ribeiro",
    "start_date": pendulum.today("UTC").add(days=-7, minutes=-60),
    "max_active_runs": 1,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "dagrun_timeout": timedelta(minutes=300),
}


@dag(
    dag_id="database.inital_config",
    default_args=args,
    schedule=None,
    tags=["database"],
)
def taskflow():
    initial_database = create_database()

    (initial_database)


dag: DAG = taskflow()

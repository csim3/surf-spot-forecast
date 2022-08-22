"""ETL for 17-day surf forecast data from Surfline's API"""

import os

import datetime
import json
import pandas as pd
import pathlib
import psycopg2
import psycopg2.sql
import pygsheets
import pytz
import re
import requests
import sqlalchemy
import sqlite3
import timezonefinder
import yaml

from spot_mapping import get_spot_mapping_list, get_spot_mapping_df

class Location():
    """Surf break spot that has 17-day forecasts provided by Surfline.

    Forecast type (waves, weather, wind, tides) of the specific spot is not 
    specified in the Location class but is instead specified in the below 
    classes that extend the Location class.
    
    Attributes:
        spot_id (string): Surfline's alphanumeric ID of a surf break location.
        spot_name (string): Common name of a surf break location. 
            One-to-one mapping between spot_id and spot_name provided by 
            https://github.com/swrobel/meta-surf-forecast/blob/main/db/seeds.rb
    """

    def __init__(self, s_id, s_name):
        """Inits Location with spot_id and spot_name."""
        self.spot_id = s_id
        self.spot_name = s_name
    
    def get_forecast_json(self, f_type):
        """Fetches JSON object of the request result from Surfline's API.

        JSON object is a spot's forecast data of a specific forecast type 
        (wave, weather, wind, or tides). Time period is 17 days and three hour 
        intervals.

        Args:
            f_type (string): Forecast type of the API request.
                Possible values are "wave", "weather", "wind", or "tides".

        Returns:
            JSON: Request result from Surfline's API.
        """
        base_url = "https://services.surfline.com/kbyg/spots/forecasts/{t}".format(
            t = f_type)
        p = {}
        p['spotId'] = self.spot_id
        p['days'] = 17
        p['intervalHours'] = 3
        p['sds'] = True
        r = requests.get(base_url, params = p)
        return r.json()

class WaveOutlook(Location):
    """Surf break spot's wave forecast.

    Attributes
        forecast_type (string): Set to "wave" to indicate wave forecast.
    """

    forecast_type = "wave"
    
    def get_forecast_json(self):
        """See base class."""
        return Location.get_forecast_json(self, self.forecast_type)
    
    def get_wave_dataframe(self, json_data):
        """Fetches DataFrame object of the spot's wave forecast.

        Args:
            json_data (JSON): Request result from Surfline's API for wave forecast.

        Returns:
            DataFrame: 17-day wave forecast for every third hour of a specific spot.
        """
        #static location-specific data
        latitude = json_data['associated']['location']['lat']
        longitude = json_data['associated']['location']['lon']
        time_zone = timezonefinder.TimezoneFinder().timezone_at(
            lng=longitude, lat=latitude)
        
        #initialize lists for forecasts from each third hour
        timestamps = []
        local_times = []
        max_heights = []
        min_heights = []
        human_relations = []
        swells = []
        
        #forecast data of specified hour
        for hourly_wave in json_data['data']['wave']:
            timestamps.append(int(hourly_wave['timestamp']))
            max_heights.append(hourly_wave['surf']['raw']['max'])
            min_heights.append(hourly_wave['surf']['raw']['min'])
            human_relations.append(hourly_wave['surf']['humanRelation'])
            #6 swells for each hour
            swell_dict = {}
            swell_dict['heights'] = []
            for swell_num in hourly_wave['swells']:
                if swell_num['height']>0:
                    swell_dict['heights'].append(swell_num['height'])
                else:
                    swell_dict['heights'].append(None)
            swells.append(swell_dict)
        
        #convert timestamp int to utc and local time
        for timestamp in timestamps:
            local_times.append(get_formatted_local_time(
                timestamp, pytz.timezone(time_zone)))
        
        wave_dict = {
            "spot_id": self.spot_id,
            "spot_name": self.spot_name,
            "spot_timezone": time_zone,
            "spot_with_timestamp": [self.spot_id+"-"+str(t) for t in timestamps],
            "spot_local_time": local_times,
            "wave_max_height": max_heights,
            "wave_min_height": min_heights,
            "human_relation": human_relations,
            "swell_height_1": [swell['heights'][0] for swell in swells],
            "swell_height_2": [swell['heights'][1] for swell in swells],
            "swell_height_3": [swell['heights'][2] for swell in swells],
            "swell_height_4": [swell['heights'][3] for swell in swells],
            "swell_height_5": [swell['heights'][4] for swell in swells],
            "swell_height_6": [swell['heights'][5] for swell in swells],
        }
        return pd.DataFrame(data=wave_dict)
    
class WindOutlook(Location):
    """Surf break spot's wind forecast.

    Attributes
        forecast_type (string): Set to "wind" to indicate wind forecast.
    """

    forecast_type = "wind"
    
    def get_forecast_json(self):
        """See base class."""
        return Location.get_forecast_json(self, self.forecast_type)
    
    def get_wind_dataframe(self, json_data):
        """Fetches DataFrame object of the spot's wind forecast.

        Args:
            json_data (JSON): Request result from Surfline's API for wind forecast.

        Returns:
            DataFrame: 17-day wind forecast for every third hour of a specific spot.
        """
        #initialize lists for forecasts from each third hour
        timestamps = []
        speeds = []
        direction_types = []
        
        #forecast data of specified hour
        for hourly_wind in json_data['data'][self.forecast_type]:
            timestamps.append(int(hourly_wind['timestamp']))
            speeds.append(hourly_wind['speed'])
            direction_types.append(hourly_wind['directionType'])
        
        wind_dict = {
            "spot_with_timestamp": [self.spot_id+"-"+str(t) for t in timestamps],
            "wind_speed": speeds,
            "wind_direction_type": direction_types,
        }
        return pd.DataFrame(data=wind_dict)

class TidesOutlook(Location):
    """Surf break spot's tides forecast.

    Attributes
        forecast_type (string): Set to "tides" to indicate tides forecast.
    """

    forecast_type = "tides"
    
    def get_forecast_json(self):
        """See base class.

        Note:
            Tides forecast is hourly along with timestamps of high and low tides.
        """
        base_url = "https://services.surfline.com/kbyg/spots/forecasts/{t}".format(
            t = self.forecast_type)
        p = {}
        p['spotId'] = self.spot_id
        p['days'] = 17
        p['intervalHours'] = 1
        p['sds'] = True
        r = requests.get(base_url, params = p)
        return r.json()
    
    def get_tides_dataframe(self, json_data):
        """Fetches DataFrame object of the spot's tides forecast.

        Args:
            json_data (JSON): Request result from Surfline's API for tides forecast.

        Returns:
            DataFrame: Hourly tides forecast, along with timestamps of high and low tides.
        """
        #static location-specific data
        latitude = json_data['associated']['tideLocation']['lat']
        longitude = json_data['associated']['tideLocation']['lon']
        location = json_data['associated']['tideLocation']['name']
        time_zone = timezonefinder.TimezoneFinder().timezone_at(
            lng=longitude, lat=latitude)
        
        #initialize lists for hourly forecasts
        timestamps = []
        local_times = []
        local_hours = []
        heights = []
        types = []
        
        #hourly forecast data
        for hourly_tide in json_data['data']['tides']:
            timestamps.append(int(hourly_tide['timestamp']))
            heights.append(hourly_tide['height'])
            types.append(hourly_tide['type'])
        
        #convert timestamp int to utc and local time
        for timestamp in timestamps:
            local_times.append(get_formatted_local_time(
                timestamp, pytz.timezone(time_zone)))
            local_hours.append(datetime.datetime.fromtimestamp(
                timestamp,tz=datetime.timezone.utc).astimezone(pytz.timezone(time_zone)).hour)
        
        tides_dict = {
            "spot_id": self.spot_id,
            "spot_name": self.spot_name,
            "spot_with_timestamp": [self.spot_id+"-"+str(t) for t in timestamps],
            "tide_local_time": local_times,
            "tide_local_hour": local_hours,
            "tide_height": heights,
            "tide_type": types
        }
        return pd.DataFrame(data=tides_dict)
    
class WeatherOutlook(Location):
    """Surf break spot's weather forecast.

    Attributes
        forecast_type (string): Set to "weather" to indicate weather forecast.
    """
    
    forecast_type = "weather"
    
    def get_forecast_json(self):
        """See base class."""
        return Location.get_forecast_json(self, self.forecast_type)
    
    def get_weather_dataframe(self, json_data):
        """Fetches DataFrame object of the spot's weather forecast.

        Args:
            json_data (JSON): Request result from Surfline's API for weather forecast.

        Returns:
            DataFrame: 17-day weather forecast for every third hour of a specific spot.
        """
        #dictionaries for each characterisitc of daily weather data
        dawns = {}
        sunrises = {}
        sunsets = {}
        dusks = {}
        
        for daily in json_data['data']['sunlightTimes']:
            dawns[str(daily['midnight'])] = daily['dawn']
            sunrises[str(daily['midnight'])] = daily['sunrise']
            sunsets[str(daily['midnight'])] = daily['sunset']
            dusks[str(daily['midnight'])] = daily['dusk']
        
        #initialize lists for forecasts from each third hour
        timestamps = []
        temperatures = []
        dawns_list = []
        sunrises_list = []
        sunsets_list = []
        dusks_list = []
            
        #forecast data of specified hour
        for hourly_weather in json_data['data'][self.forecast_type]:
            timestamps.append(int(hourly_weather['timestamp']))
            temperatures.append(hourly_weather['temperature'])
            
            #look up daily weather characteristics for forecast of specified hour
            time_zone = datetime.timezone(datetime.timedelta(
                hours=hourly_weather['utcOffset']))
            local_time = datetime.datetime.fromtimestamp(
                int(hourly_weather['timestamp']),tz=datetime.timezone.utc).astimezone(time_zone)
            local_start_of_day_timestamp = str(
                int(local_time.replace(hour=0,minute=0,second=0,microsecond=0).timestamp()))
            
            dawns_list.append(get_formatted_local_time(
                dawns[local_start_of_day_timestamp], time_zone))
            sunrises_list.append(get_formatted_local_time(
                sunrises[local_start_of_day_timestamp], time_zone))
            sunsets_list.append(get_formatted_local_time(
                sunsets[local_start_of_day_timestamp], time_zone))
            dusks_list.append(get_formatted_local_time(
                dusks[local_start_of_day_timestamp], time_zone))
        
        weather_dict = {
            "spot_with_timestamp": [self.spot_id+"-"+str(t) for t in timestamps],
            "temperature": temperatures,
            "first_light": dawns_list,
            "sunrise": sunrises_list,
            "sunset": sunsets_list,
            "last_light": dusks_list
        }
        return pd.DataFrame(data=weather_dict)

def delete_rows(table_name, s_id):
    """Delete forecast data from PostgreSQL database table for specific spot.

    Args:
        table_name (string): PostgreSQL database table name.
        s_id (string): Surfline's alphanumeric ID of a surf break location.
    """
    with open('config.yaml') as config_file:
        dict = yaml.safe_load(config_file)
    host = dict['DATABASE']['HOST']
    db_name = dict['DATABASE']['DBNAME']
    user = dict['DATABASE']['USER']
    conn_string = "host=" + host + " dbname=" + db_name + " user=" + user
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    sql_delete = psycopg2.sql.SQL("DELETE FROM {table} WHERE spot_id=%s").format(
        table=psycopg2.sql.Identifier(table_name))
    try:
        cursor.execute(sql_delete, (s_id,))
        print("Data for {s} deleted from {t}".format(s=s_id, t=table_name))
    except:
        print("Data for {s} did not delete from {t}".format(s=s_id, t=table_name))
    conn.commit()
    conn.close()
    
def truncate_table(table_name):
    """Truncate forecast data from PostgreSQL database table.

    Args:
        table_name (string): PostgreSQL database table name.
    """
    with open('config.yaml') as config_file:
        dict = yaml.safe_load(config_file)
    host = dict['DATABASE']['HOST']
    db_name = dict['DATABASE']['DBNAME']
    user = dict['DATABASE']['USER']
    conn_string = "host=" + host + " dbname=" + db_name + " user=" + user
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    sql_truncate = psycopg2.sql.SQL("TRUNCATE TABLE {table}").format(
        table=psycopg2.sql.Identifier(table_name))
    try:
        cursor.execute(sql_truncate)
        print("Data truncated from {t}".format(t=table_name))
    except:
        print("Data did not truncate from {t}".format(t=table_name))
    conn.commit()
    conn.close()
    
def insert_rows(table_name, df):
    """Insert forecast DataFrame for specific spot into PostgreSQL database table.

    Args:
        table_name (string): PostgreSQL database table name.
        df (DataFrame): 17-day forecast data of specific spot.
    """
    with open('config.yaml') as config_file:
        dict = yaml.safe_load(config_file)
    host = dict['DATABASE']['HOST']
    db_name = dict['DATABASE']['DBNAME']
    user = dict['DATABASE']['USER']
    conn_string = 'postgresql://' + user + '@' + host + '/' + db_name
    db = sqlalchemy.create_engine(conn_string)
    conn = db.connect()
    try:
        df.to_sql(table_name, con=conn, index=False, if_exists='append')
        print("Data uploaded to {t}".format(t=table_name))
    except:
        print("Data did not upload to {t}".format(t=table_name))
    conn.autocomit = True
    conn.close()
    
def get_formatted_local_time(unix_timestamp, t_zone):
    """Fetches string-formatted time of Unix timestamp in the specified timezone.

    Args:
        unix_timestamp (int): Unix timestamp
        t_zone (tzinfo): Instance of a tzinfo subclass that specifies a spot's timezone

    Returns:
        string: Equivalent time in spot's timezone of the inputted Unix timestamp
    """
    return datetime.datetime.fromtimestamp(
        unix_timestamp,tz=datetime.timezone.utc).astimezone(
            t_zone).strftime('%Y-%m-%d %H:%M:%S')

def truncate_google_sheet(sheet_name):
    """Deletes all rows (except header row) of a Google Sheet.

    Args:
        sheet_name (string): Google Sheet name.
    """
    with open('config.yaml') as config_file:
        dict = yaml.safe_load(config_file)
    file_path = dict['GOOGLE_DRIVE']['CREDENTIALS_FILE_PATH']
    client = pygsheets.authorize(service_account_file=file_path)
    sht = client.open('Surfline_Forecasts')
    wks = sht.worksheet('title',sheet_name)
    try:
        wks.clear('A2')
        print("Data truncated from {s}".format(s=sheet_name))
    except:
        print("Data did not truncate from {s}".format(s=sheet_name))
    
def insert_google_sheet(sheet_name, table_name):
    """Inserts PostgreSQL db table into Google Sheet

    Inserts all rows (except header row) from PostgreSQL db table
    into Google Sheet, starting from cell A2

    Args:
        sheet_name (string): Google Sheet name.
        table_name (string): PostgreSQL database table name.
    """
    with open('config.yaml') as config_file:
        dict = yaml.safe_load(config_file)
    file_path = dict['GOOGLE_DRIVE']['CREDENTIALS_FILE_PATH']
    host = dict['DATABASE']['HOST']
    db_name = dict['DATABASE']['DBNAME']
    user = dict['DATABASE']['USER']
    
    client = pygsheets.authorize(service_account_file=file_path)
    sht = client.open('Surfline_Forecasts')
    wks = sht.worksheet('title',sheet_name)

    conn_string = 'postgresql://' + user + '@' + host + '/' + db_name
    db = sqlalchemy.create_engine(conn_string)
    cur = db.raw_connection().cursor()
    try:
        query = psycopg2.sql.SQL("SELECT * FROM {table};").format(
            table=psycopg2.sql.Identifier(table_name))
        query_string = query.as_string(cur)
        data = pd.read_sql_query(query_string,db)
        wks.set_dataframe(data, (2,1), copy_head=False, extend=True, nan='')
        print("Data uploaded to {s}".format(s=sheet_name))
    except:
        print("Data did not uload to {s}".format(s=sheet_name))
    cur.close()

def run_postgresql_etl():
    """Runs Surfline API -> PostgreSQL db table ETL process.

    ETL process for wave, weather, wind, and tides forecast is run for each 
    surf break spot that has a mapped spot_id.
    """
    #Change working dir to home dir to find config.yaml file in Airflow runs
    os.chdir(str(pathlib.Path.home())) 
    #Update postgresql table for wave/weather/wind and tides
    truncate_table("wave_weather_wind_tides")
    for spot in get_spot_mapping_list():
        #Create dataframes for each wave, weather, wind, and tides forecast
        wave_spot = WaveOutlook(spot['spot_id'], spot['spot_name'])
        wave_json = wave_spot.get_forecast_json()
        wave_df = wave_spot.get_wave_dataframe(wave_json)
        
        weather_spot = WeatherOutlook(spot['spot_id'], spot['spot_name'])
        weather_json = weather_spot.get_forecast_json()
        weather_df = weather_spot.get_weather_dataframe(weather_json)
        
        wind_spot = WindOutlook(spot['spot_id'], spot['spot_name'])
        wind_json = wind_spot.get_forecast_json()
        wind_df = wind_spot.get_wind_dataframe(wind_json)
        
        tides_spot = TidesOutlook(spot['spot_id'], spot['spot_name'])
        tides_json = tides_spot.get_forecast_json()
        tides_df = tides_spot.get_tides_dataframe(tides_json)
        #filter tides df for records that are multiples of 3rd hour or high/low tides
        filtered_tides_df = tides_df[(tides_df['tide_local_hour'] % 3 == 0) | (
            tides_df['tide_type'] != 'NORMAL')]
        
        #Join dataframes to get singular dataframe for database input
        www_df = wave_df.merge(weather_df, on='spot_with_timestamp', how='left').merge(
            wind_df, on='spot_with_timestamp', how='left')
        wwwt_df = pd.merge(filtered_tides_df, www_df, 
            on=['spot_with_timestamp','spot_id','spot_name'], how='left')
        wwwt_df_mapping = pd.merge(wwwt_df, get_spot_mapping_df(), 
            on=['spot_id','spot_name'], how='left')
        final_df = wwwt_df_mapping[[
            'tide_local_time',
            'tide_height',
            'tide_type',
            'spot_id',
            'spot_name',
            'spot_timezone',
            'spot_local_time',
            'wave_max_height',
            'wave_min_height',
            'human_relation',
            'swell_height_1',
            'swell_height_2',
            'swell_height_3',
            'swell_height_4',
            'swell_height_5',
            'swell_height_6',
            'temperature',
            'first_light',
            'sunrise',
            'sunset',
            'last_light',
            'wind_speed',
            'wind_direction_type',
            'subregion',
            'region'
        ]]
        insert_rows("wave_weather_wind_tides", final_df)

def run_gsheets_etl():
    """Updates Google Sheet based on data from PostgreSQL db table.

    Truncates data from Google Sheet and then inserts updated forecast data. 
    """
    #Change working dir to home dir to find config.yaml file in Airflow runs
    os.chdir(str(pathlib.Path.home())) 
    truncate_google_sheet("GSheet_Wave_Weather_Wind_Tides")
    insert_google_sheet("GSheet_Wave_Weather_Wind_Tides","wave_weather_wind_tides")

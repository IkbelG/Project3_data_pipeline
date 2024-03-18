import functions_framework
import pandas as pd
import sqlalchemy
import requests
from pytz import timezone
from datetime import datetime,timedelta
import my_passwords as ps

@functions_framework.http
def insert_API_data(request):
  connection_string = connection()

  cities_from_sql_df = pd.read_sql("cities", con=connection_string) # Get the list of cities from the "cities" Table
  weather_infos_df=get_weather_info(cities_from_sql_df) # Get the weather Dataframe
  send_data_to_sql(weather_infos_df,'weather_infos',connection_string) # Send data about weather to our database

  airports_from_sql_df = pd.read_sql("airports", con=connection_string) # Get the list of airports from the "airports" Table
  flight_infos_df=get_arrival_flights_info(airports_from_sql_df["airport_icao"]) # Get the flights Dataframe for all "airports_ICAO"
  send_data_to_sql(flight_infos_df,"flights",connection_string) # Send data about flights to our database
  
  return 'Data successfully added'

######################## connection() function ########################
def connection():
  connection_name = "sharp-airway-417014:europe-west1:wbs-mysql-db"
  db_user = "root"
  db_password = ps.my_password
  schema_name = "gans"
  driver_name = 'mysql+pymysql'
  query_string = {"unix_socket": f"/cloudsql/{connection_name}"}
  db = sqlalchemy.create_engine(
      sqlalchemy.engine.url.URL(
          drivername = driver_name,
          username = db_user,
          password = db_password,
          database = schema_name,
          query = query_string,
      )
  )
  return db


######################## send_data_to_sql() function ########################

def send_data_to_sql(name_df,name_table,connection_string):
  name_df.to_sql(name_table,
                  if_exists='append',
                  con=connection_string,
                  index=False)

######################## get_weather_info() function ########################

def get_weather_info(cities_df):
  weather_dic = {"city_id": [],
    "weather_time": [],
    "data_collected_time": [],
    "temperature": [],
    "weather_outlook": [],
    "weather_description": [],
    "wind_speed": [],
    "chance_rain": [],
    "rain": [],
    "snow": []
    }
  
  berlin_timezone = timezone('Europe/Berlin')
  for city_name in cities_df["city"]:

    url = "https://api.openweathermap.org/data/2.5/forecast"
    querystring = {"q": city_name, "units":"metric"}
    header = {"X-Api-Key": ps.API_key_weather}

    weather = requests.request("GET", url, headers=header, params=querystring)
    weather_json = weather.json()
    current_time=datetime.now(berlin_timezone).strftime("%Y-%m-%d %H:%M:%S")

    city_id = cities_df.loc[cities_df["city"] == city_name, "city_id"].values[0]
    for w in weather_json["list"]:
      weather_dic["city_id"].append(city_id)
      weather_dic["weather_time"].append(w["dt_txt"])
      weather_dic["data_collected_time"].append(current_time)
      weather_dic["temperature"].append(w["main"]["temp"])
      weather_dic["weather_outlook"].append(w["weather"][0]["main"])
      weather_dic["weather_description"].append(w["weather"][0]["description"])
      weather_dic["wind_speed"].append(w["wind"]["speed"])
      weather_dic["chance_rain"].append(w["pop"])
      try:
        weather_dic["rain"].append(w["rain"]["3h"])
      except:
        weather_dic["rain"].append(0)
      try:
        weather_dic["snow"].append(w["snow"]["3h"])
      except:
        weather_dic["snow"].append(0)  

  weather_df = pd.DataFrame(weather_dic)
  weather_df['weather_time'] = pd.to_datetime(weather_df['weather_time'])
  weather_df['data_collected_time'] = pd.to_datetime(weather_df['data_collected_time'])
  weather_df['snow'] = weather_df['snow'].astype(float)

  return weather_df

######################## get_arrival_flights_info() function ########################

def get_arrival_flights_info(list_icao):
    
    arrivals_df=[]
    all_arrivals_df=[]
    berlin_timezone = timezone('Europe/Berlin')
    today=datetime.now(berlin_timezone).date()
    tomorrow= (today + timedelta(1))
    times = [["00:00","11:59"],["12:00","23:59"]]

    for icao in list_icao:
      
      for time in times:
        url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{icao}/{tomorrow}T{time[0]}/{tomorrow}T{time[1]}"

        querystring = {"withLeg":"true",
          "direction":"Arrival",
          "withCancelled":"false",
          "withCodeshared":"true",
          "withCargo":"false",
          "withPrivate":"false"}
        
        headers = {
          "X-RapidAPI-Key": ps.API_key_flights,
          "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
        }
        response = requests.get(url, headers=headers, params=querystring)
        arrivals_df=pd.json_normalize(response.json()["arrivals"])
        arrivals_df["arrival_airport_icao"]=icao
        arrivals_df["data_collected_time"]=datetime.now(berlin_timezone).strftime("%Y-%m-%d %H:%M:%S")
      
        all_arrivals_df.append(arrivals_df)
        all_flights_arrivals=pd.concat(all_arrivals_df, ignore_index=True)

        all_flights_arrivals = all_flights_arrivals.loc[:,["number","departure.airport.icao", "arrival.scheduledTime.local","arrival.terminal","arrival_airport_icao","data_collected_time"]]
        all_flights_arrivals = all_flights_arrivals.rename(columns={"number": "flight_number", "departure.airport.icao": "departure_airport_icao", "arrival.scheduledTime.local": "scheduled_arrival_time","arrival.terminal":"arrival_terminal"})
        all_flights_arrivals["scheduled_arrival_time"] = all_flights_arrivals["scheduled_arrival_time"].str[:-6]
        all_flights_arrivals["scheduled_arrival_time"] = pd.to_datetime(all_flights_arrivals["scheduled_arrival_time"])
        all_flights_arrivals["data_collected_time"] = pd.to_datetime(all_flights_arrivals["data_collected_time"])
        
    return all_flights_arrivals
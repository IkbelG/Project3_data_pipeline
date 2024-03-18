import pandas as pd
import requests
from pytz import timezone
from datetime import datetime,timedelta
from bs4 import BeautifulSoup as bs

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from my_passwords import API_key_weather,API_key_flights
################################################################################################

#############################      connect_to_sql() function    ###############################

################################################################################################

def connect_to_sql(password):
    schema = "gans"
    host = "127.0.0.1"
    user = "root"
    password = password
    port = 3306

    connection_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
    return connection_string

################################################################################################

##################      send_data_to_sql_without_truncate() function     #######################

#################################################################################################
# Send data of the dataframe "name_df" to the MySQL table "name_table"
def send_data_to_sql_without_truncate(name_df,name_table,connection_string):
  name_df.to_sql(name_table,
                  if_exists='append',
                  con=connection_string,
                  index=False)
  
################################################################################################

########################    send_data_to_sql_with_truncate() function     #######################

#################################################################################################
# Send data of the dataframe "name_df" to the MySQL table "name_table" with truncate
def send_data_to_sql_with_truncate(name_df, name_table, connection_string):
    engine = create_engine(connection_string)
    try:
        # Using a transaction block to ensure that the operation is atomic*
        with engine.begin() as connection:
            # Using the text construct to ensure the SQL command is executed as raw SQL (otherwise won't work)
            connection.execute(text(f"TRUNCATE TABLE {name_table}"))
            name_df.to_sql(name_table, 
                              if_exists='append', 
                              con=connection, 
                              index=False)
    except SQLAlchemyError as e:
        print(f"Error occurred: {e}")  
################################################################################################

#############################    get_cities_info() function     ##############################

#################################################################################################
# This function takes the list of cities and returns the Dataframe that contains all cities data
def get_cities_info(cities):

  # Initialising the city dictionary
  city_dic = {"city": [],
    "country_code": [],
    "latitude": [],
    "longitude": [],
    "population": [],
    "year_data_retrieved": []
    }

  for city_name in cities:
    url = "https://en.wikipedia.org/wiki/"+city_name # URL of wikipedia page of each city
    response = requests.get(url)
    soup_city = bs(response.content, 'html.parser') # We are using beautiful soup for web scraping

    # Adding all the information about the city to the city dictionary
    # City name
    city_dic["city"].append(city_name)
    # Country
    country=soup_city.select('td.infobox-data')[0].text
    # Country code
    match country:
      case "Germany":code="DE"
      case "France":code="FR"
    city_dic["country_code"].append(code)
    # Coordinates
    city_dic["latitude"].append(soup_city.find(class_="latitude").get_text())
    city_dic["longitude"].append(soup_city.find(class_="longitude").get_text())
    # Population
    city_dic["population"].append(soup_city.find('table', class_='vcard').find(string="Population").find_next("td").text)
    # Year when the population is calculated
    city_dic["year_data_retrieved"].append(soup_city.find('table', class_='vcard').find(string="Population").find_next("div").text[2:6])

    # Create the city Dataframe 
    cities_df = pd.DataFrame(city_dic)
    # Data cleaning for the population 
    cities_df.population = cities_df.population.apply(lambda x : x.replace(',',''))
    # Change the type of the population and the year columns
    cities_df['population'] = cities_df['population'].astype(int)
    cities_df['year_data_retrieved'] = cities_df['year_data_retrieved'].astype(int)
  return cities_df

################################################################################################

#############################      get_weather_info() function     ##############################

#################################################################################################
# This function takes the cities dataframe and returns the Dataframe that contains all weather data
def get_weather_info(cities_df):
  
  # Initialising the weather dictionary
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
  
  # Current time when the data were collected
  berlin_timezone = timezone('Europe/Berlin') # We used timezone since we will deploy our function in the cloud
  current_time=datetime.now(berlin_timezone).strftime("%Y-%m-%d %H:%M:%S")

  for city_name in cities_df["city"]:  # For each city in the cities Dataframe

    # Define the sections that will together form the url
    url = "https://api.openweathermap.org/data/2.5/forecast" # URL of the weather API
    querystring = {"q": city_name, "units":"metric"} # We need the city name and metric to have temperature in Celsius
    header = {"X-Api-Key": API_key_weather} # The API key to access to the API

    # Reference the sections in the request
    weather = requests.request("GET", url, headers=header, params=querystring) # Send the request
    weather_json = weather.json() # Get the response in a JSON format
    
    # We take the city_id from the cities dataframe
    city_id = cities_df.loc[cities_df["city"] == city_name, "city_id"].values[0]

    # Adding all the information about weather to the weather dictionary
    for w in weather_json["list"]:
      weather_dic["city_id"].append(city_id)
      weather_dic["weather_time"].append(w["dt_txt"])
      weather_dic["data_collected_time"].append(current_time)
      weather_dic["temperature"].append(w["main"]["temp"])
      weather_dic["weather_outlook"].append(w["weather"][0]["main"])
      weather_dic["weather_description"].append(w["weather"][0]["description"])
      weather_dic["wind_speed"].append(w["wind"]["speed"])
      weather_dic["chance_rain"].append(w["pop"])
      # We used exceptions when collecting data about rain and snow because this data is missing when the value is 0
      try:
        weather_dic["rain"].append(w["rain"]["3h"])
      except:
        weather_dic["rain"].append(0)
      try:
        weather_dic["snow"].append(w["snow"]["3h"])
      except:
        weather_dic["snow"].append(0)  

  # Create the weather Dataframe
  weather_df = pd.DataFrame(weather_dic)
  # Change the type of the weather_time, the data_collected_time and the snow columns
  weather_df['weather_time'] = pd.to_datetime(weather_df['weather_time'])
  weather_df['data_collected_time'] = pd.to_datetime(weather_df['data_collected_time'])
  weather_df['snow'] = weather_df['snow'].astype(float)

  return weather_df


################################################################################################

#############################      get_airpots_info() function     ##############################

#################################################################################################
# This function takes the cities dataframe and returns the Dataframe that contains all airports data
def get_airpots_info(cities_df):
  
  # Initialising the airport dictionary
  airport_dic = {"city_id": [],
    "airport_icao": [],
    "airport_name": []
    }
  
  for city_name in cities_df["city"]: # For each city in the cities Dataframe
    
    # Define the sections that will together form the url 
    # These sections were generated by the API
    url = "https://aerodatabox.p.rapidapi.com/airports/search/term" # URL of the airport API
    querystring = {"q":city_name,"limit":"10","withFlightInfoOnly":"True"} # We need the city name and only airports with flights
    headers = {
      "X-RapidAPI-Key": API_key_flights, # Change the generated API key to our API Key
      "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
    }

    airport = requests.get(url, headers=headers, params=querystring) # Send the request
    airport_json=airport.json() # Get the response in a JSON format
    
    # We take the city_id and the country code from the cities dataframe
    city_id = cities_df.loc[cities_df["city"] == city_name, "city_id"].values[0]
    country_code = cities_df.loc[cities_df["city"] == city_name, "country_code"].values[0]
    for w in airport_json["items"]: # For each airport in the JSON response
      if(w["countryCode"]==country_code): # Since we are searching for airport by the airport name and not by coordinates,  
                                          # we need to check if the country code is the same as the one of the city
                                          # The same city name can exist in more than one country
        # Adding all the information about the airport to the airport dictionary
        airport_dic["city_id"].append(city_id)
        airport_dic["airport_icao"].append(w["icao"])
        airport_dic["airport_name"].append(w["name"])
    
    # Create the weather Dataframe
    airport_df = pd.DataFrame(airport_dic)

  return airport_df

#################################################################################################

#########################       get_arrival_flights_info() function     #########################

#################################################################################################
# This function takes the list of airports ICAO and returns the Dataframe that contains all flights data
def get_arrival_flights_info(list_icao):
    
    # Initializing the Dataframes
    arrivals_df=[]
    all_arrivals_df=[]

    # We need to have data about flights for the next day
    berlin_timezone = timezone('Europe/Berlin')
    today=datetime.now(berlin_timezone).date()
    tomorrow= (today + timedelta(1))
    # The AeroDataBox API only returns flight data for 12 hours. 
    # To get data for a whole day, we used two requests in a for loop. 
    times = [["00:00","11:59"],["12:00","23:59"]]

    for icao in list_icao: # For each airport ICAO in the list
      
      for time in times: # For each 12 hours in the day
        # The sections that form the URL were generated by the flights API
        url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{icao}/{tomorrow}T{time[0]}/{tomorrow}T{time[1]}" # URL fo the flights API

        querystring = {"withLeg":"true",
          "direction":"Arrival", # We only need Arrival flights
          "withCancelled":"false",
          "withCodeshared":"true",
          "withCargo":"false",
          "withPrivate":"false"}
        
        headers = {
          "X-RapidAPI-Key": API_key_flights,
          "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
        }
        response = requests.get(url, headers=headers, params=querystring) # Send the request
        # In this function, we used json_normalize rather than using dictionaries as in the previous functions
        # JSON normalize JSON data into a Dataframe
        arrivals_df=pd.json_normalize(response.json()["arrivals"]) # Add information about arrivals to the arrivals_df
        # Add the arrival airport ICAO and the time when data were collected to our dataframe since this information is not included in the JSON response
        arrivals_df["arrival_airport_icao"]=icao
        arrivals_df["data_collected_time"]=datetime.now(berlin_timezone).strftime("%Y-%m-%d %H:%M:%S")
        
      
        # Concat all the generated Dataframe to our flights Dataframe
        all_arrivals_df.append(arrivals_df)
        all_flights_arrivals=pd.concat(all_arrivals_df, ignore_index=True)

        # Extract only the needed columns
        all_flights_arrivals = all_flights_arrivals.loc[:,["number","departure.airport.icao", "arrival.scheduledTime.local","arrival.terminal","arrival_airport_icao","data_collected_time"]]
        # Rename the columns
        all_flights_arrivals = all_flights_arrivals.rename(columns={"number": "flight_number", "departure.airport.icao": "departure_airport_icao", "arrival.scheduledTime.local": "scheduled_arrival_time","arrival.terminal":"arrival_terminal"})
        # Data cleaning for the scheduled_arrival_time
        all_flights_arrivals["scheduled_arrival_time"] = all_flights_arrivals["scheduled_arrival_time"].str[:-6]
        # Change the type of scheduled_arrival_time and data_collected_time
        all_flights_arrivals["scheduled_arrival_time"] = pd.to_datetime(all_flights_arrivals["scheduled_arrival_time"])
        all_flights_arrivals["data_collected_time"] = pd.to_datetime(all_flights_arrivals["data_collected_time"])
        
    return all_flights_arrivals

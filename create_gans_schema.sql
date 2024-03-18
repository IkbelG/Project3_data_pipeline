-- Drop the database if it already exists
DROP DATABASE IF EXISTS gans;

-- Create test_tablethe database
CREATE DATABASE gans;

-- Use the database
USE gans;

-- DROP TABLE IF EXISTS flights;
-- DROP TABLE IF EXISTS cities_airports;
-- DROP TABLE IF EXISTS airports;
-- DROP TABLE IF EXISTS weather_infos;
-- DROP TABLE IF EXISTS city_infos;
-- DROP TABLE IF EXISTS cities;

CREATE TABLE cities (
    city_id INT AUTO_INCREMENT,
    city VARCHAR(255) NOT NULL,
    country_code VARCHAR(255) NOT NULL,
    PRIMARY KEY (city_id)
);

-- Create the 'city_infos' table
CREATE TABLE city_infos (
    city_id INT,
    latitude VARCHAR(255) NOT NULL,
    longitude VARCHAR(255) NOT NULL,
    population INT NOT NULL,
    year_data_retrieved INT,
    PRIMARY KEY (city_id),
    FOREIGN KEY (city_id)
        REFERENCES cities (city_id)
);

-- Create the 'weather_infos' table
CREATE TABLE weather_infos (
    weather_id INT AUTO_INCREMENT,
    city_id INT NOT NULL,
    weather_time DATETIME NOT NULL,
    data_collected_time DATETIME NOT NULL,
    temperature DECIMAL NOT NULL,
    weather_outlook VARCHAR(255) NOT NULL,
    weather_description VARCHAR(255) NOT NULL,
    wind_speed DECIMAL NOT NULL,
    visibility INT NOT NULL,
    chance_rain DECIMAL NOT NULL,
    rain DECIMAL NOT NULL,
    snow DECIMAL NOT NULL,
    PRIMARY KEY (weather_id),
    FOREIGN KEY (city_id)
        REFERENCES cities (city_id)
);

-- Create the 'airports' table
CREATE TABLE airports (
    airport_icao VARCHAR(255),
    airport_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (airport_icao)
);

-- Create the 'cities_airports' table
CREATE TABLE cities_airports (
    city_id INT,
    airport_icao VARCHAR(255),
    PRIMARY KEY (city_id , airport_icao),
    FOREIGN KEY (city_id)
        REFERENCES cities (city_id),
    FOREIGN KEY (airport_icao)
        REFERENCES airports (airport_icao)
);

-- Create the 'flights' table
CREATE TABLE flights (
    flight_id INT AUTO_INCREMENT,
    flight_number VARCHAR(255) NOT NULL,
    departure_airport_icao VARCHAR(255),
    scheduled_arrival_time DATETIME NOT NULL,
    arrival_terminal VARCHAR(255),
    arrival_airport_icao VARCHAR(255) NOT NULL,
    PRIMARY KEY (flight_id),
    FOREIGN KEY (arrival_airport_icao)
		REFERENCES airports (airport_icao)
);

SELECT 
    *
FROM
    cities;
SELECT 
    *
FROM
    city_infos;
SELECT 
    *
FROM
    weather_infos;
SELECT 
    *
FROM
    airports;
SELECT 
    *
FROM
    cities_airports;
SELECT 
    *
FROM
    flights;


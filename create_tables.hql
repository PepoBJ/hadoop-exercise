CREATE DATABASE indra_exercise;
USE indra_exercise;

CREATE EXTERNAL TABLE IF NOT EXISTS t_country (
    country_code CHAR(3),
    country_name VARCHAR(50)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
LOCATION '/user/training/upload/data/country/paises.ada';

CREATE EXTERNAL TABLE IF NOT EXISTS t_february_log (
    airplane_code CHAR(4),
    day_number INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '_'
LOCATION '/user/training/upload/data/day/fecha.dat';

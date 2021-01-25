---------------------------------------------- DATABASE --------------------------------------------
CREATE DATABASE IF NOT EXISTS indra_exercise;
USE indra_exercise;
-----------------------------------------------------------------------------------------------------

------------------------------------------- LOAD TABLES -------------------------------------------
CREATE TABLE IF NOT EXISTS t_country (
    country_code CHAR(3),
    country_name VARCHAR(50)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
LOCATION '/user/maria_dev/upload/data/country';

CREATE TABLE IF NOT EXISTS t_february_log (
    airplane_code CHAR(4),
    day_number INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\137'
LOCATION '/user/maria_dev/upload/data/date';

CREATE TABLE IF NOT EXISTS t_flight (
    airplane_code CHAR(4),
    country_origin_code CHAR(3),
    country_target_code CHAR(3)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\174'
LOCATION '/user/maria_dev/upload/data/flight';

CREATE TABLE IF NOT EXISTS t_flight_delay (
    airplane_code CHAR(4),
    delay_time INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES('input.regex'='([0-9]{4})([0-9]{2})')
LOCATION '/user/maria_dev/upload/data/delay';
-------------------------------------------------------------------------------------------------------

--------------------------------------- CREATE VIEWS --------------------------------------------------
CREATE VIEW v_flight_delay AS
SELECT airplane_code, delay_time, row_number() over() as delay_row_number
FROM t_flight_delay;

CREATE VIEW v_february_log AS
SELECT airplane_code, day_number, '2020-02' AS date_log, row_number() over() as february_row_number
FROM t_february_log;

CREATE VIEW v_flight AS
SELECT airplane_code, country_origin_code, country_target_code, row_number() over() as flight_row_number
FROM t_flight;

CREATE VIEW v_flight_origin_target_country AS
SELECT DISTINCT f.airplane_code, f.country_origin_code, c.country_name AS country_origin_name,
       f.country_target_code, c2.country_name AS country_target_name, f.flight_row_number
FROM v_flight f
LEFT JOIN t_country c
ON f.country_origin_code = c.country_code
LEFT JOIN t_country c2
ON f.country_target_code = c2.country_code;
-------------------------------------------------------------------------------------------------------

--------------------------------------- CREATE HISTORY ------------------------------------------------

CREATE TABLE IF NOT EXISTS t_flight_history (
    airplane_code CHAR(4),
    country_origin_code CHAR(3),
    country_origin_name VARCHAR(50),
    country_target_code CHAR(3),
    country_target_name VARCHAR(50),
    delay_time INT,
    day_number INT,
    date_log VARCHAR(7)
);

INSERT OVERRIDE TABLE t_flight_history
SELECT fot.airplane_code, fot.country_origin_code, fot.country_origin_name,
       fot.country_target_code, fot.country_target_name, d.delay_time,
       fl.day_number,  fl.date_log
FROM v_flight_origin_target_country fot
LEFT JOIN v_flight_delay d
ON fot.airplane_code = d.airplane_code AND fot.flight_row_number = d.delay_row_number
LEFT JOIN v_february_log fl
ON fot.airplane_code = fl.airplane_code AND fot.flight_row_number = fl.february_row_number;
--------------------------------------------------------------------------------------------------------
--- flight issue: 5306


--------------------------------------------- QUERY TEST------------------------------------------------
SELECT * FROM t_flight_history LIMIT 10;
--------------------------------------------------------------------------------------------------------
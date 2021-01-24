CREATE DATABASE indra_exercise;
USE indra_exercise;

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

CREATE TABLE IF NOT EXISTS t_flight_log (
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


CREATE TABLE IF NOT EXIST t_flight_history (
    airplane_code CHAR(4),
    country_origin_code CHAR(3),
    country_origin_name VARCHAR(50),
    country_target_code CHAR(3),
    country_target_name VARCHAR(50),
    delay_time INT,
    day_number INT,
    date_log VARCHAR(7)
);

CREATE VIEW IF NOT EXIST v_flight_origin_country AS
SELECT f.airplane_code, f.country_origin_code, c.country_name AS country_origin_name
FROM t_flight_log f
LEFT JOIN t_country c
ON f.country_origin_code = c.country_code;

CREATE VIEW IF NOT EXIST v_flight_target_country AS
SELECT f.airplane_code, f.country_target_code, c.country_name AS country_origin_name
FROM t_flight_log f
LEFT JOIN t_country c
ON f.country_target_code = c.country_code;

CREATE VIEW IF NOT EXIST v_flight_delay AS
SELECT f.airplane_code, d.delay_time
FROM t_flight_log f
LEFT JOIN t_flight_delay d
ON f.airplane_code = d.airplane_code;

CREATE VIEW IF NOT EXIST v_flight_february_log AS
SELECT f.airplane_code, fl.day_number,  '2020-02' AS date_log
FROM  t_flight_log f
LEFT JOIN t_february_log fl
ON f.airplane_code = fl.airplane_code;

INSERT INTO TABLE t_flight_history PARTITION (date_log)

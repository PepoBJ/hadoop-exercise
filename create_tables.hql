CREATE DATABASE indra_exercise;
USE indra_exercise;


CREATE TABLE t_country (
    code CHAR(3),
    name VARCHAR(50)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/user/training/upload/data/country/paises.ada';
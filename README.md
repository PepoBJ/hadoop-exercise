# AIR TRIBU

## 1. Problema
---------------
Se han recolectado los datos de la Compañía aérea air-tribu, en concreto los vuelos realizados por esta compañía en 25 países durante el mes de Febrero de 2019. 
Se dispone de 4 archivos:

> - ***países***: relaciona código de país con el País correspondiente 
> - ***vuelos***: recopila información sobre el número de vuelo, origen y destino
> - ***retrasos***: información sobre el retraso que ha tenido el vuelo (valores entre 10 y 99 minutos)
> - ***fecha***: día de Febrero que tuvo lugar el vuelo

- El archivo airTribu.xlsx , es un excel con el resumen de todos los archivos.

- Subir los archivos a HDFS, se creará una ruta para este ejercicio, y en esta ruta habrá una carpeta para cada archivo. 
Todos los archivos deben ser subidos ejecutando un único fichero.sh
- Analizar los archivos y crear 4 tablas en Hive, con la información de dichos archivos.
- Crear una nueva tabla en Hive, que contenga la siguiente información: Vuelo, Origen, Destino, Retraso.
- Hive se generarán las tablas necesarias para responder a las siguientes preguntas 
    - ¿De qué país salieron más aviones?
    - ¿A qué país llegaron más aviones?

- Spark usando DF o RDD:
    - ¿qué día hubo más vuelos? ¿y menos?
    - ¿qué día hubo más retrasos? ¿y menos?

- Ejecutar las sentencias en Spark, y guardar la info en unas nuevas tablas Hive.
- Agregar un resumen con toda la información de valor que consideréis. 
- proponer que se podría hacer con estos datos

## 2. Ejecución
---------------
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

## 2. Ejecución (paso a paso)
---------------
1. Subir archivos a HDFS:
    - Los archivos se suben a la ruta: ***/user/maria_dev/upload/data/***
    - Ejecutar en la terminal:

    ```shell
    chmod +x upload_files.sh
    ./upload_files.sh
    ```

2. Creación de las tablas en HIVE
    - Creará la BD HIVE ***indra_exercise*** y las tablas correspondientes
    - Ejecutar en la terminal:

    ```shell
    hive -f create_tables.hql
    ```

3. Ejecución del script PYSPARK
    - Creará las tablas con la información relevante.
    - Ejecutar en la terminal:

    ```shell
    export PYTHONIOENCODING=utf8  #Support Utf-8
    spark-submit queries_data.py
    ```

## 3. Ejecución (lotes)
---------------
1. Para ejecutar todos los pasos anteriores en un solo fichero ***sh***:

    ```shell
    chmod +x upload_files.sh
    chmod +x setup.sh
    ./setup.sh
    ```

## 4. Resultados
---------------
- Base de datos ***indra_exercise***

    | Database      |
    | ------------- |
    | indra_exercise|
- Tablas y vistas creadas
    | Name          | Tipo           | Descripción  |
    | ------------- |:-------------:| -----:|
    | t_country  | TABLE | Tabla de paises  |
    |t_february_log | TABLE | Tabla de las fechas de los vuelos (Febrero)  |
    |t_flight | TABLE   | Tabla de vuelos - origin / destino  |
    |t_flight_delay | TABLE | Tabla de retrasos de los vuelos  |
    |v_february_log | VIEW | Vista con los días de los vuelos (febrero)  |
    |v_flight | VIEW   | Vista con los vuelos, origen/destino (rank) |
    |v_flight_delay | VIEW | Vista con los retrasos por vuelo (rank) |
    |v_flight_origin_target_country | VIEW | Vista con los nombre de los paises origen/destino de los vuelos (rank) |
    |t_flight_history | TABLE   | Tabla consolidada de vuelos, país origin/destino, retraso y fecha  |
    |t_delay_country_flight | TABLE | Tabla de promedio de retrasos de vuelos por paises  |
    |t_number_time_flight | TABLE   | Tabla consolidada de el número de vuelos y promedio de retraso por fecha  |
    |t_take_off_arrive_flight | TABLE   | Tabla consolidada de despegues y llegadas por país  |
    

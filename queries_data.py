from pyspark.sql import SparkSession
from pyspark.sql.functions import *

print('Initializing job...')
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sql('use indra_exercise')

print('> The countries with the most take-offs:')
take_off_df = spark.sql('SELECT country_origin_code as country_code, count(*) as take_off FROM t_flight_history GROUP BY country_origin_code ORDER BY take_off DESC')
take_off_df.show(5, False)

print('> The countries with the most arrivals:')
arrive_df =  spark.sql('SELECT country_target_code as country_code, count(*) as arrive FROM t_flight_history GROUP BY country_target_code ORDER BY arrive DESC')
arrive_df.show(5, False)

print('> Consolidated table with take-offs and arrivals:')
country_df = spark.sql('SELECT country_code, country_name FROM t_country')

take_arrive_country_df = country_df.join(take_off_df, on='country_code', how='left').join(arrive_df, on='country_code', how='left').orderBy('country_name')
take_arrive_country_df.show(50, False)

print('> The days with more and less flights:')
number_flight_df = spark.sql('SELECT date_log, day_number, count(*) as number_flight FROM t_flight_history GROUP BY day_number, date_log ORDER BY number_flight DESC')
number_flight_df.show(5, False)

print('> The days with the most and least delays:')
flight_time_df = spark.sql('SELECT date_log, day_number, avg(delay_time) as delay_time FROM t_flight_history GROUP BY day_number, date_log ORDER BY delay_time DESC')
flight_time_df.show(5, False)

print('> Consolidated table with flights and delays:')
number_flight_time_df = number_flight_df.join(flight_time_df, on=['date_log', 'day_number'], how='full')
number_flight_time_df.show(50, False)

print('> [+] Consolidated table with countries with delayed flights:')
country_delay_flight_df = spark.sql('SELECT country_origin_code, country_origin_name, avg(delay_time) as delay_time FROM t_flight_history GROUP BY country_origin_code, country_origin_name ORDER BY delay_time DESC')
country_delay_flight_df.show(50, False)

print('\n\nWriting the consolidated table <indra_exercise.t_take_off_arrive_flight> in HIVE...')
take_arrive_country_df.write.mode("overwrite").saveAsTable("indra_exercise.t_take_off_arrive_flight")
print('Writing the consolidated table <indra_exercise.t_number_time_flight> in HIVE...')
number_flight_time_df.write.mode("overwrite").saveAsTable("indra_exercise.t_number_time_flight")
print('Writing the consolidated table <indra_exercise.t_delay_country_flight> in HIVE...')
country_delay_flight_df.write.mode("overwrite").saveAsTable("indra_exercise.t_delay_country_flight")
print('Done.')



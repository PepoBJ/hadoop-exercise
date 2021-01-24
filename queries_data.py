from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sql('use indra_exercise')

take_off_df = spark.sql('SELECT country_origin_code as country_code, count(*) as take_off FROM t_flight_history GROUP BY country_origin_code ORDER BY take_off DESC')
take_off_df.show(5, False)

arrive_df =  spark.sql('SELECT country_target_code as country_code, count(*) as arrive FROM t_flight_history GROUP BY country_target_code ORDER BY arrive DESC')
arrive_df.show(5, False)

country_df = spark.sql('SELECT country_code, country_name FROM t_country')

take_arrive_country_df = country_df.join(take_off_df, on='country_code', how='left').join(arrive_df, on='country_code', how='left').orderBy('country_name')
take_arrive_country_df.show(50, Fasle)

number_flight_df = spark.sql('SELECT date_log, day_number, count(*) as number_flight FROM t_flight_history GROUP BY day_number, date_log ORDER BY number_flight DESC')
number_flight_df.show(5, False)

flight_time_df = spark.sql('SELECT date_log, day_number, avg(delay_time) as delay_time FROM t_flight_history GROUP BY day_number, date_log ORDER BY delay_time DESC')
flight_time_df.show(5, False)

number_flight_time_df = number_flight_df.join(flight_time_df, on=['date_log', 'day_number'], how='full')
number_flight_time_df.show(50, Fasle)

take_arrive_country_df.write.mode("overwrite").saveAsTable("indra_exercise.t_take_off_arrive_flight")
number_flight_time_df.write.mode("overwrite").saveAsTable("indra_exercise.t_number_time_flight")


